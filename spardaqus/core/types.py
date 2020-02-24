import socket
import json

from pandas import DataFrame
from pandas.io.json import json_normalize
from pyspark.sql.types import StringType, StructType, ArrayType

from spardaqus import globals
from spardaqus.core.utils import mergedicts
from spardaqus.core.exceptions import SpardaqusMissingSparkSQLStructMetadata, SpardaqusUnknownSparkSQLStruct


class SpardaqusMessage:

    def __parse(self, r, x, i):
        if type(x) is list:
            for y in x:
                r = self.__parse(r, y, i)
        else:
            n = x["name"] if "name" in x.keys() else None
            f = x["fields"] if "fields" in x.keys() else None
            t = x["type"] if "type" in x.keys() else None
            if t:
                if type(t) is str:
                    if t == "struct":
                        r = self.__parse(r, f[0], i)
                    elif t == "string":
                        m = x["metadata"]
                        if i in m.keys():
                            if i == "default":
                                r = mergedicts(r, {n: m[i]})
                            elif m[i]:
                                r = "%s %s = %s" % (r, n, m[i])
                        elif i == "default":
                            raise SpardaqusMissingSparkSQLStructMetadata
                elif type(t) is dict:
                    if "elementType" in t.keys():
                        t = t["elementType"]
                    if i == "default":
                        z = {n: [self.__parse({}, t["fields"], i)]}
                        r = mergedicts(r, z)
                    else:
                        r = self.__parse(r, t["fields"], i)
        return r

    def _struct2json(self, sparksqlstruct):
        return self.__parse({}, sparksqlstruct.jsonValue(), "default")

    def _struct2queryfragment(self, sparksqlstruct, integration):
        return self.__parse("", sparksqlstruct.jsonValue(), integration)

    def __init__(self, integration):

        event = StructType() \
            .add("spdqid", StringType(), nullable=False, metadata={"splunk": "substr(sha512(host + \"::\" + _raw), 1, %d),", "default": ""}) \
            .add("spdqbkt", StringType(), nullable=False, metadata={"splunk": "\"%s\",", "default": ""}) \
            .add("spdqdata", StringType(), nullable=False, metadata={"splunk": "_raw,", "default": ""}) \
            .add("spdqidxn", StringType(), nullable=False, metadata={"splunk": "_index,", "default": ""}) \
            .add("spdqephn", StringType(), nullable=False, metadata={"splunk": "host,", "default": ""}) \
            .add("spdqsrc", StringType(), nullable=False, metadata={"splunk": "source,", "default": ""}) \
            .add("spdqstyp", StringType(), nullable=False, metadata={"splunk": "sourcetype,", "default": ""}) \
            .add("spdqtskey", StringType(), nullable=False, metadata={"splunk": "strftime(_time, \"%%Y%%m%%d%%H%%M%%S\"),", "default": ""}) \
            .add("spdqtstxt", StringType(), nullable=False, metadata={"splunk": "strftime(%s, \"%s\"),", "default": ""}) \
            .add("spdqtssrc", StringType(), nullable=False, metadata={"splunk": "_time", "default": ""})

        spdqh = StructType() \
            .add("name", StringType(), nullable=False, metadata={"splunk": "", "default": socket.gethostname()}) \
            .add("fqdn", StringType(), nullable=False, metadata={"splunk": "", "default": socket.getfqdn()}) \
            .add("ips", StringType(), nullable=False, metadata={"splunk": "", "default": ",".join(socket.gethostbyname_ex(socket.gethostname())[-1])})

        meta = StructType() \
            .add("sent", StringType(), nullable=False, metadata={"splunk": "", "default": "%s"}) \
            .add("spdqv", StringType(), nullable=False, metadata={"splunk": "", "default": globals.__VERSION__}) \
            .add("spdqh", spdqh)

        spdq = StructType(). \
            add("meta", meta). \
            add("data", ArrayType(event))

        self.spark_sql_struct = StructType().add("spdq", spdq)

        self.json_envelope = self._struct2json(self.spark_sql_struct)

        if integration == "splunk":
            self.query_fragment = self._struct2queryfragment(self.spark_sql_struct, integration)


class SpardaqusDataFrame(DataFrame):

    def __init__(self):
        super().__init__()

    @property
    def _constructor_expanddim(self):
        return DataFrame._constructor_expanddim

    @staticmethod
    def _spdq_loadjson(jsonmsg):
        return json.loads(jsonmsg)

    @staticmethod
    def getspardaqusmessagekey(unpackedmsg):
        return "%s:%s" % (unpackedmsg.get("spdqts2"), unpackedmsg.get("spdqid"))

    @staticmethod
    def getspardaqusmessagevalue(unpackedmsg):
        return unpackedmsg.get("spdqdata")

    def add(self, rawmsg):
        msg = self.unpackspardaqusmessage(rawmsg)
        k = self.getspardaqusmessagekey(msg)
        v = self.getspardaqusmessagevalue(msg)
        self.concat(json_normalize({k: v}))
