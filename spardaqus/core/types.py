import socket

from pyspark.sql.types import StringType, StructType, ArrayType

from spardaqus import globals
from spardaqus.core.utils import mergedicts
from spardaqus.core.exceptions import \
    SpardaqusMissingSparkSQLStructMetadata, \
    SpardaqusSparkSQLStructParseError, \
    SpardaqusSparkSQLStructMissingRequiredElementsError


class SpardaqusMessage:

    def _parse(self, r, x, i):
        if type(x) is list:
            for y in x:
                r = self._parse(r, y, i)
        else:
            n = x["name"] if "name" in x.keys() else None
            f = x["fields"] if "fields" in x.keys() else None
            t = x["type"] if "type" in x.keys() else None
            if t:
                if type(t) is str:
                    if t == "struct":
                        r = self._parse(r, f[0], i)
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
                            raise SpardaqusSparkSQLStructParseError
                    else:
                        raise SpardaqusSparkSQLStructParseError
                elif type(t) is dict:
                    a = False
                    if "elementType" in t.keys():
                        t = t["elementType"]
                        a = True
                    if i == "json":
                        z = {n: [self._parse({}, t["fields"], i)]} if a else {n: self._parse({}, t["fields"], i)}
                        r = mergedicts(r, z)
                    else:
                        r = self._parse(r, t["fields"], i)
                else:
                    raise SpardaqusSparkSQLStructParseError
        return r

    def _struct2json(self, sparksqlstruct):
        return self._parse({}, sparksqlstruct.jsonValue(), "json")

    def _struct2queryfragment(self, sparksqlstruct, integration):
        return self._parse("", sparksqlstruct.jsonValue(), integration)

    def __init__(self, integration):

        # .add("spdqdata", StringType(), nullable=False, metadata={"splunk": "_raw,", "json": ""}) \

        event = StructType() \
            .add("spdqid", StringType(), nullable=False, metadata={"splunk": "substr(sha512(host + \"::\" + _raw), 1, %d),", "json": ""}) \
            .add("spdqbkt", StringType(), nullable=False, metadata={"splunk": "\"%s\",", "json": ""}) \
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

        # required fields check
        if "spdq" not in self.json_envelope.keys():
            raise SpardaqusSparkSQLStructMissingRequiredElementsError
        if "data" not in self.json_envelope["spdq"].keys():
            raise SpardaqusSparkSQLStructMissingRequiredElementsError
        if type(self.json_envelope["spdq"]["data"]) is not list:
            raise SpardaqusSparkSQLStructMissingRequiredElementsError
        for check in self.json_envelope["spdq"]["data"]:
            if "spdqtskey" not in check.keys() or "spdqid" not in check.keys():  # or "spdqdata" not in check.keys():
                raise SpardaqusSparkSQLStructMissingRequiredElementsError

        if integration == "splunk":
            self.query_fragment = self._struct2queryfragment(self.spark_sql_struct, integration)
        else:
            self.query_fragment = None


