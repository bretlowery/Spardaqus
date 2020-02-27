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
                            if i == "json":
                                r = mergedicts(r, {n: m[i]})
                            elif m[i]:
                                r = "%s %s = %s" % (r, n, m[i])
                        elif i == "json":
                            raise SpardaqusMissingSparkSQLStructMetadata
                        else:
                            raise SpardaqusUnknownSparkSQLStruct
                    else:
                        raise SpardaqusUnknownSparkSQLStruct
                elif type(t) is dict:
                    a = False
                    if "elementType" in t.keys():
                        t = t["elementType"]
                        a = True
                    if i == "json":
                        z = {n: [self.__parse({}, t["fields"], i)]} if a else {n: self.__parse({}, t["fields"], i)}
                        r = mergedicts(r, z)
                    else:
                        r = self.__parse(r, t["fields"], i)
                else:
                    raise SpardaqusUnknownSparkSQLStruct
        return r

    def _struct2json(self, sparksqlstruct):
        return self.__parse({}, sparksqlstruct.jsonValue(), "json")

    def _struct2queryfragment(self, sparksqlstruct, integration):
        return self.__parse("", sparksqlstruct.jsonValue(), integration)

    def __init__(self, integration):

        event = StructType() \
            .add("spdqid", StringType(), nullable=False, metadata={"splunk": "substr(sha512(host + \"::\" + _raw), 1, %d),", "json": ""}) \
            .add("spdqbkt", StringType(), nullable=False, metadata={"splunk": "\"%s\",", "json": ""}) \
            .add("spdqdata", StringType(), nullable=False, metadata={"splunk": "_raw,", "json": ""}) \
            .add("spdqidxn", StringType(), nullable=False, metadata={"splunk": "_index,", "json": ""}) \
            .add("spdqephn", StringType(), nullable=False, metadata={"splunk": "host,", "json": ""}) \
            .add("spdqsrc", StringType(), nullable=False, metadata={"splunk": "source,", "json": ""}) \
            .add("spdqstyp", StringType(), nullable=False, metadata={"splunk": "sourcetype,", "json": ""}) \
            .add("spdqtskey", StringType(), nullable=False, metadata={"splunk": "strftime(_time, \"%%Y%%m%%d%%H%%M%%S\"),", "json": ""}) \
            .add("spdqtstxt", StringType(), nullable=False, metadata={"splunk": "strftime(%s, \"%s\"),", "json": ""}) \
            .add("spdqtssrc", StringType(), nullable=False, metadata={"splunk": "_time", "json": ""})

        spdqh = StructType() \
            .add("name", StringType(), nullable=False, metadata={"splunk": "", "json": socket.gethostname()}) \
            .add("fqdn", StringType(), nullable=False, metadata={"splunk": "", "json": socket.getfqdn()}) \
            .add("ips", StringType(), nullable=False, metadata={"splunk": "", "json": ",".join(socket.gethostbyname_ex(socket.gethostname())[-1])})

        meta = StructType() \
            .add("sent", StringType(), nullable=False, metadata={"splunk": "", "json": "%s"}) \
            .add("spdqv", StringType(), nullable=False, metadata={"splunk": "", "json": globals.__VERSION__}) \
            .add("spdqh", spdqh)

        spdq = StructType(). \
            add("meta", meta). \
            add("data", ArrayType(event))

        self.spark_sql_struct = StructType().add("spdq", spdq)

        self.json_envelope = self._struct2json(self.spark_sql_struct)

        if integration == "splunk":
            self.query_fragment = self._struct2queryfragment(self.spark_sql_struct, integration)
        else:
            self.query_fragment = None


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
