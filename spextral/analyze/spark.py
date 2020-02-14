from functools import lru_cache
import logging
import os
import string
import sys

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession, Row, DataFrame, SQLContext
from pyspark.sql.types import StringType, StructType, ArrayType
from pyspark.sql.functions import *

from spextral import globals
from spextral.core.decorators import timeout_after
from spextral.core.exceptions import SpextralTimeoutWarning
from spextral.core.metaclasses import SpextralAnalyzer
from spextral.core.utils import boolish,\
    error,\
    getconfig,\
    getenviron,\
    isreadable,\
    setenviron


class Spark(SpextralAnalyzer):

    def __init__(self, engine):
        self.engine = engine
        super().__init__(self.__class__.__name__)
        self.target = self.config("master", required=True)
        self.bucket = self.getbucket()
        self.sc = None
        self.sqlc = None
        self.session = None
        self.limit_reached = False
        self.results_returned = True
        self.timeoutmsg = "%s operation failed: connection refused by %s at %s" \
                          % (self.engine.options.operation.capitalize(), self.integration.capitalize(), self.target)
        self.required_jars = []
        self.spark_logging_level = self.config("logging.level", required=True, defaultvalue="ERROR",
                                               choices=["CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO", "WARN", "WARNING", "OFF"]).upper()

    def getbucket(self):
        configbucket = self.config("topic", required=False, defaultvalue=None)
        if configbucket and configbucket not in ["none", "default"]:
            bucket = configbucket.strip().translate(str.maketrans(string.punctuation, '_' * len(string.punctuation)))
        else:
            bucket = 'spextral'
        return bucket[:255]

    @property
    def schema(self):
        """
        Example message JSON:

            {
              "spxtrl": {
                "meta": {
                  "sent": "2020-02-01T17:05:44.099229",
                  "spxv": "0.0.1",
                  "spxh": {
                    "name": "MacBook-Pro.lan",
                    "fqdn": "macbook-pro.lan",
                    "ips": "192.168.9.161"
                  }
                },
                "data": [
                    {
                        "spxtrlbkt": "flan",
                        "spxtrldata": "91.224.160.4 - - [15/Oct/2019:17:54:08 -0400] \"POST /wp-login.php HTTP/1.0\" 200 2967 \"http://nationalphilosophicalcounselingassociation.org/\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.162 Safari/535.19 Flan/0.0.37 (https://bret.guru/flan)\"",
                        "spxtrlephn": "MacBook-Pro.lan",
                        "spxtrlid": "e52d1ce64560514646aa",
                        "spxtrlsrc": "/var/bret/out/access.log",
                        "spxtrlstyp": "bret_access_combined",
                        "spxtrlts1": "15/Oct/2019:17:54:08 -0400",
                        "spxtrlts2": "20191015175408",
                        "spxtrlts3": "1571176448",
                        "spxtrlx": "1571198413"
                    } (,...)
                ]
              }
            }

        """
        spxh = StructType()\
            .add("name", StringType(), nullable=False)\
            .add("fqdn", StringType(), nullable=False)\
            .add("ips", StringType(), nullable=False)
        meta = StructType()\
            .add("sent", StringType(), nullable=False)\
            .add("spxv", StringType(), nullable=False)\
            .add("spxh", spxh)
        event = StructType()\
            .add("spxtrlbkt", StringType(), nullable=False)\
            .add("spxtrldata", StringType(), nullable=False)\
            .add("spxtrlephn", StringType(), nullable=False)\
            .add("spxtrlid", StringType(), nullable=False)\
            .add("spxtrlsrc", StringType(), nullable=False)\
            .add("spxtrlstyp", StringType(), nullable=False)\
            .add("spxtrlts1", StringType(), nullable=False)\
            .add("spxtrlts2", StringType(), nullable=False)\
            .add("spxtrlts3", StringType(), nullable=False)\
            .add("spxtrlx", StringType(), nullable=False)
        spxtrl = StructType().\
            add("meta", meta).\
            add("data", ArrayType(event))
        schema = StructType().\
            add("spxtrl", spxtrl)
        return schema

    def _check4requiredextraclasses(self, spark_home):
        if not globals.SPARK_REQUIRED_EXTRACLASS:
            return True
        spark_conf_file = os.path.join(spark_home, "conf/spark-defaults.conf")
        if not isreadable(spark_conf_file):
            return True
        xc = globals.SPARK_REQUIRED_EXTRACLASS.lower()
        f1 = False
        f2 = False
        with open(spark_conf_file, "r") as f:
            for row in f:
                row = row.lstrip().rstrip().lower()
                if row[:1] == "s":
                    if "spark.driver.extraclasspath" in row and xc in row:
                        f1 = True
                    elif "spark.executor.extraclasspath" in row and xc in row:
                        f2 = True
        if not f1 or not f2:
            self.error("%s is not correctly configured for Kafka streaming on Spextral. Please download this file to your Spextral analyze server:\r\n"
                       "   %s    (to /your/server/path, for example)\r\n"
                       "then add these lines to %s:\r\n"
                       "   spark.driver.extraClassPath   %s\r\n"
                       "   spark.executor.extraClassPath %s\r\n" %
                       (spark_conf_file, xc, spark_conf_file, "/your/server/path/%s" % xc, "/your/server/path/%s" % xc))
        return True

    def _prepare(self):
        spark_prepared = boolish(getenviron("SPEXTRAL_SPARK_PREPARED", "False"))
        if not spark_prepared:
            if sys.platform == "darwin":
                try:
                    os.mkdir("/tmp/spark-events")
                except OSError:
                    pass
            spark_home = setenviron("SPARK_HOME", self.config("home", required=False, defaultvalue=getenviron("SPARK_HOME")))
            setenviron("PYSPARK_PYTHON", self.config("pyspark.python", required=False, defaultvalue=getenviron("PYSPARK_PYTHON")))
            setenviron("PYSPARK_DRIVER_PYTHON", self.config("pyspark.driver.python", required=False, defaultvalue=getenviron("PYSPARK_DRIVER_PYTHON")))
            setenviron("SPARK_LOCAL_IP", self.config("pyspark.local.ip", required=False, defaultvalue=getenviron("SPARK_LOCAL_IP")))
            self.info("SPARK_HOME=%s" % getenviron("SPARK_HOME"))
            self.info("SPARK_LOCAL_IP=%s" % getenviron("SPARK_LOCAL_IP"))
            self.info("PYSPARK_PYTHON=%s" % getenviron("PYSPARK_PYTHON"))
            self.info("PYSPARK_DRIVER_PYTHON=%s" % getenviron("PYSPARK_DRIVER_PYTHON"))
            self._check4requiredextraclasses(spark_home)
            os_pyspark_submit_list = getenviron("PYSPARK_SUBMIT_ARGS", "").replace("--packages", "").strip().split(" ")
            pyspark_submit_list = globals.PYSPARK_REQUIRED_PACKAGES.strip().split(" ")
            for pkg in os_pyspark_submit_list:
                if pkg not in pyspark_submit_list:
                    pyspark_submit_list.append(pkg)
            pyspark_submit_args = "--packages"
            for pkg in pyspark_submit_list:
                pyspark_submit_args = "%s %s" % (pyspark_submit_args, pkg)
            spextral_log4j_properties = os.path.join(globals.ROOT_DIR, 'config/log4j.properties')
            pyspark_submit_args = "%s %s %s %s" % (
                pyspark_submit_args,
                '--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=%s"' % spextral_log4j_properties,
                '--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=%s"' % spextral_log4j_properties,
                '--files %s' % spextral_log4j_properties
            )
            setenviron("PYSPARK_SUBMIT_ARGS", pyspark_submit_args)
            self.info("PYSPARK_SUBMIT_ARGS=%s" % getenviron("PYSPARK_SUBMIT_ARGS"))
            s_logger = logging.getLogger('py4j')
            s_logger.setLevel(logging.CRITICAL if self.spark_logging_level == "CRITICAL"
                              else logging.DEBUG if self.spark_logging_level == "DEBUG"
                              else logging.ERROR if self.spark_logging_level == "ERROR"
                              else logging.FATAL if self.spark_logging_level == "FATAL"
                              else logging.INFO if self.spark_logging_level == "INFO"
                              else logging.WARN if self.spark_logging_level in ["WARN", "WARNING"]
                              else logging.OFF)
            setenviron("SPEXTRAL_SPARK_PREPARED", "True")

    @timeout_after(60)
    def _getsparkconfiguration(self):
        try:
            sc_conf = SparkConf()
            sc_conf.setAppName(self.name)
            sc_conf.setMaster(self.target)
            sparkoptions = self.config("sparkcontext.options", required=False, defaultvalue=None)
            if sparkoptions:
                for k in sparkoptions.keys():
                    sc_conf.set(k, sparkoptions[k])
            self.sc = SparkContext(appName=self.name, conf=sc_conf)
            self.sc.setLogLevel(self.spark_logging_level)
            log4j_logging_level = self.sc._jvm.org.apache.log4j.Level.CRITICAL if self.spark_logging_level == "CRITICAL" \
                else self.sc._jvm.org.apache.log4j.Level.DEBUG if self.spark_logging_level == "DEBUG" \
                else self.sc._jvm.org.apache.log4j.Level.ERROR if self.spark_logging_level == "ERROR" \
                else self.sc._jvm.org.apache.log4j.Level.FATAL if self.spark_logging_level == "FATAL" \
                else self.sc._jvm.org.apache.log4j.Level.INFO if self.spark_logging_level == "INFO" \
                else self.sc._jvm.org.apache.log4j.Level.WARNING if self.spark_logging_level in ["WARN", "WARNING"] \
                else self.sc._jvm.org.apache.log4j.Level.ERROR
            self.sc._jvm.org.apache.log4j.LogManager.getLogger("org").setLevel(log4j_logging_level)
            self.sc._jvm.org.apache.log4j.LogManager.getLogger("akka").setLevel(log4j_logging_level)
            return sc_conf
        except SpextralTimeoutWarning as w:
            pass

    def connect(self, **kwargs):
        self.info("Connecting to %s analyze cluster at %s" % (self.integration.capitalize(), self.target))
        self._prepare()
        try:
            self.session = SparkSession \
                .builder \
                .appName(self.name) \
                .config(conf=self._getsparkconfiguration()) \
                .getOrCreate()
            self.results = '__QUERY_PENDING__'
        except Exception as e:
            error("establishing %s session at %s: %s" % (self.integration.capitalize(), self.target, str(e)))
        self.info("Connected to %s %s" % (self.integration.capitalize(), self.sc.version))

    @property
    def connected(self):
        if self.session:
            return True
        return False

    @property
    @lru_cache()
    def _stream(self):
        stream = None
        try:
            if self.engine.transport.integration == "kafka":
                consumer_options = getconfig("analyze", self.engine.transport.integration, "consumer.options", required=True)
                s = self.session.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.engine.transport.target) \
                    .option("subscribe", self.bucket) \
                    .option("startingOffsets", "earliest") \
                    .option("maxOffsetsPerTrigger", 10000)
                for k, v in consumer_options.items():
                    s.option("kafka.%s" % k, v)
                stream = s.load()
        except Exception as e:
            self.error("reading from %s transport stream at %s: %s" % (self.integration.capitalize(), self.target, str(e)))
        return stream

    def dump(self):
        self.results = self._stream \
            .selectExpr("CAST(value AS STRING) as spxmsgraw") \
            .select(from_json("spxmsgraw", self.schema).alias("spxmsg")) \
            .select("spxmsg.spxtrl.data.spxtrldata") \
            .writeStream \
            .format("console") \
            .option('truncate', 'false') \
            .start() \
            .awaitTermination()

    def close(self, **kwargs):
        pass

    def analyze(self):
        self.results = self._stream \
            .selectExpr("CAST(value AS STRING) as spxmsgraw") \
            .select(from_json("spxmsgraw", self.schema).alias("spxmsg")) \
            .select("spxmsg.spxtrl.data.spxtrldata") \
            .writeStream \
            .format("console") \
            .option('truncate', 'false') \
            .start() \
            .awaitTermination()

