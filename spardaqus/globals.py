from datetime import datetime
import os
import resource
import sys

# CONSTANTS
__VERSION__ = "0.0.1"
__NAME__ = "Spardaqus"
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
EXITONERROR = True
RSS_MEMORY_BASE = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
RSS_MEMORY_DIVISOR = 1024.0 if sys.platform == "darwin" else 1.0 if sys.platform == "linux" else 1.0
PYSPARK_REQUIRED_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 " \
                            "pyspark-shell "
SPARK_REQUIRED_EXTRACLASS = "kafka-clients-0.10.0.1.jar"

# Dynamically changed by the system; don't mess with these
LOGGER = None
MAX_RSS_MEMORY_USED = 0.0
LAST_RSS_MEMORY_USED = 0.0
SETTINGS_CACHE = {}
CACHED_SETTINGS = {}
KILLSIG = False

# Adjustable if you know what you are doing
MAX_SPLUNK_BATCH_SIZE = 100000000
SPLUNK_BATCH_SIZE = 1
