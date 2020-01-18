import os
import stat
import string
import sys

import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from py4j.protocol import Py4JJavaError
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, DataFrame, SQLContext

from spextral import globals
from spextral.core.metaclasses import SpextralAnalyzer
from spextral.core.utils import error, tmpfile
from spextral.core.decorators import timeout_after


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

    def __init__(self, engine):
        self.engine = engine
        super().__init__(self.__class__.__name__)
        self.target = self.config("master", required=True)
        self.bucket = self.getbucket()
        self.batchduration = self.config("batch.duration", required=False, defaultvalue=60)
        self.sc = None
        self.sqlc = None
        self.ssc = None
        self.stream = None
        self.limit_reached = False
        self.results_returned = True

    @timeout_after(60)
    def _setcontext(self):
        if sys.platform == "darwin":
            try:
                os.mkdir("/tmp/spark-events")
            except OSError:
                pass
        os.environ["SPARK_HOME"] = self.config("home", required=False, defaultvalue=os.environ["SPARK_HOME"])
        os.environ["PYSPARK_PYTHON"] = self.config("pyspark.python", required=False, defaultvalue=os.environ["PYSPARK_PYTHON"])
        os.environ["PYSPARK_DRIVER_PYTHON"] = self.config("pyspark.driver.python", required=False, defaultvalue=os.environ["PYSPARK_DRIVER_PYTHON"])
        _createlog4jpropfile()
        sc_conf = SparkConf()
        sc_conf.setAppName(self.name)
        sc_conf.setMaster(self.target)
        sc_conf.set("spark.executor.extraJavaOptions", "\"-Dlog4j.configuration=file://%s\"" % globals.LOG4JPROPFILE)
        sc_conf.set("spark.driver.extraJavaOptions", "\"-Dlog4j.configuration=file://%s\"" % globals.LOG4JPROPFILE)
        sparkoptions = self.config("sparkcontext.options", required=False, defaultvalue={})
        if sparkoptions:
            for k in sparkoptions.keys():
                sc_conf.set(k, sparkoptions[k])
        self.sc = SparkContext(appName=self.name, conf=sc_conf)
        self.sc._jvm.org.apache.log4j.LogManager.getLogger("org").setLevel(self.sc._jvm.org.apache.log4j.Level.WARN)
        self.sc._jvm.org.apache.log4j.LogManager.getLogger("akka").setLevel(self.sc._jvm.org.apache.log4j.Level.WARN)

    def connect(self, **kwargs):
        self.info("Connecting to %s analyze cluster at %s" % (self.integration.capitalize(), self.target))
        try:
            self._setcontext()
            self.sqlc = SQLContext(self.sc)
            self.ssc = StreamingContext(self.sc, self.batchduration)
            self.stream = self.engine.transport.receive(self.ssc, self.bucket)
        except Exception as e:
            error("ERROR during connection to %s at %s: %s" % (self.integration.capitalize(), self.target, str(e)))

    @property
    def data(self):
        return self.stream.map(lambda x: x[1])

    def getbucket(self):
        configbucket = self.config("topic", required=False, defaultvalue=None)
        if configbucket and configbucket not in ["none", "default"]:
            bucket = configbucket.strip().translate(str.maketrans(string.punctuation, '_' * len(string.punctuation)))
        else:
            bucket = 'spextral'
        return bucket[:255]

    @property
    def connected(self):
        if self.stream:
            return True
        else:
            return False

    def analyze(self):
        lines = self.data
        lines.pprint()
        self.ssc.start()
        self.ssc.awaitTermination()
