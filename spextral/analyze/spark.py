import ast
import os
import stat
import string
import sys

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from py4j.protocol import Py4JJavaError
from pyspark.sql import Row, DataFrame, SQLContext, SparkSession
from pyspark.sql.types import TimestampType, StringType, StructType, StructField, ArrayType, IntegerType
from pyspark.sql.functions import *

from spextral import globals
from spextral.core.decorators import timeout_after
from spextral.core.exceptions import SpextralTimeoutWarning
from spextral.core.metaclasses import SpextralAnalyzer
from spextral.core.utils import getenviron, setenviron, tmpfile, getconfig


def _createlog4jpropfile():
    globals.LOG4JPROPFILE = tmpfile() if globals.LOG4JPROPFILE is None else globals.LOG4JPROPFILE
    with open(globals.LOG4JPROPFILE, "a") as log4jpropfile:
        log4jpropfile.write("log4j.appender.console.layout = org.apache.log4j.PatternLayout")
        log4jpropfile.write("log4j.appender.console.layout.ConversionPattern = Spextral/" + globals.__VERSION__ + " %d{yyyy-MM-dd}T%d{HH:mm:ss}: %p %c {1}: %m%n")
        log4jpropfile.write("log4j.logger.org.eclipse.jetty = WARN")
        log4jpropfile.write("log4j.logger.org.eclipse.jetty.util.component.AbstractLifeCycle = WARN")
        log4jpropfile.write("log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper = WARN")
        log4jpropfile.write("log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter = WARN")
    os.chmod(globals.LOG4JPROPFILE, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)  # make it read only to everyone


class Spark(SpextralAnalyzer):

    def getbucket(self):
        configbucket = self.config("topic", required=False, defaultvalue=None)
        if configbucket and configbucket not in ["none", "default"]:
            bucket = configbucket.strip().translate(str.maketrans(string.punctuation, '_' * len(string.punctuation)))
        else:
            bucket = 'spextral'
        return bucket[:255]

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
        self.timeoutmsg = "%s operation failed: connection refused by %s at %s" % (self.engine.options.operation.capitalize(), self.integration.capitalize(), self.target)

    @property
    def message_schema(self):
        spextralhost = StructType([
            StructField("name", StringType(), True),
            StructField("fqdn", StringType(), True),
            StructField("ips", StringType(), True),
        ])
        spextralmeta = StructType([
            StructField("sent", TimestampType(), True),
            StructField("spxv", StringType(), True),
            StructField("host", spextralhost, True),
        ])
        spextraldata = StructType([
            StructField("spxtrlbkt", StringType(), True),
            StructField("spxtrldata", StringType(), True),
            StructField("spxtrlevt1", TimestampType(), True),
            StructField("spxtrlevt2", IntegerType(), True),
            StructField("spxtrlevt3", IntegerType(), True),
            StructField("spxtrlhost", StringType(), True),
            StructField("spxtrlid", StringType(), True),
            StructField("spxtrlsrc", StringType(), True),
            StructField("spxtrltyp", StringType(), True),
            StructField("spxtrlx", IntegerType(), True),
        ])
        spextralpacket = StructType(
            StructField("meta", spextralmeta, True),
            StructField("data", spextraldata, True),
        )
        spextralmessage = StructType(
            StructField("spxtrl", spextralpacket, True),
        )
        return spextralmessage

    @timeout_after(60)
    def _setcontext(self):
        try:
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
            required_jars = self.config("required.jars", required=True, converttolist=True, defaultvalue=[])
            if required_jars:
                if isinstance(required_jars, list):
                    for jar in required_jars:
                        jar = os.path.join(spark_home, "jars/%s" % jar)
                        if not os.path.exists(jar):
                            self.error("required jar '%s' is not installed on this host.")
            _createlog4jpropfile()
            sc_conf = SparkConf()
            sc_conf.setAppName(self.name)
            sc_conf.setMaster(self.target)
            sc_conf.set("spark.executor.extraJavaOptions", "\"-Dlog4j.configuration=file://%s\"" % globals.LOG4JPROPFILE)
            sc_conf.set("spark.driver.extraJavaOptions", "\"-Dlog4j.configuration=file://%s\"" % globals.LOG4JPROPFILE)
            sparkoptions = self.config("sparkcontext.options", required=False, defaultvalue=None)
            if sparkoptions:
                for k in sparkoptions.keys():
                    sc_conf.set(k, sparkoptions[k])
            self.sc = SparkContext(appName=self.name, conf=sc_conf)
            self.sc._jvm.org.apache.log4j.LogManager.getLogger("org").setLevel(self.sc._jvm.org.apache.log4j.Level.WARN)
            self.sc._jvm.org.apache.log4j.LogManager.getLogger("akka").setLevel(self.sc._jvm.org.apache.log4j.Level.WARN)
            return sc_conf
        except SpextralTimeoutWarning as w:
            pass

    def connect(self, **kwargs):
        self.info("Connecting to %s analyze cluster at %s" % (self.integration.capitalize(), self.target))
        try:
            self.session = SparkSession. \
                builder. \
                appName(self.name). \
                config(conf=self._setcontext()). \
                getOrCreate()
            self.results = 'READY2RECIEVE'
        except Exception as e:
            self.error("establishing %s session at %s: %s" % (self.integration.capitalize(), self.target, str(e)))

    @property
    def connected(self):
        if self.session:
            return True
        return False

    def analyze(self):
        stream = None
        try:
            if self.engine.transport.integration == "kafka":
                consumer_options = getconfig("analyze", self.engine.transport.integration, "consumer.options", required=True)
                s = self.session.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", self.engine.transport.target) \
                    .option("subscribe", self.bucket) \
                    .option("startingOffsets", "earliest") \
                    .option("max.poll.records", 100) \
                    .option("maxOffsetsPerTrigger", 10000)
                for k, v in consumer_options.items():
                    s.option("kafka.%s" % k, v)
                stream = s.load()
        except Exception as e:
            self.error("reading from %s transport stream at %s: %s" % (self.integration.capitalize(), self.target, str(e)))
        results = stream \
            .selectExpr("CAST(value AS STRING) as spx") \
            .select(from_json("spx", self.message_schema))
        results.printSchema()
        # meta = results.select(
        # meta.printSchema()
        # meta.start()
        # meta.awaitTerminationOrTimeout(90)
        # meta.stop(stopGraceFully=True)


         ##   .select("siteId", "siteData.dataseries", explode("siteData.values").alias("values")) \
         ##   .select("siteId", "dataseries", "values.*")

    def close(self, **kwargs):
        pass