import socket
import json

from pandas import DataFrame
from pandas.io.json import json_normalize
from pyspark.sql.types import StringType, StructType, ArrayType

from spardaqus import globals
from spardaqus.core.utils import mergedicts


class SpardaqusMessage:

    @staticmethod
    def _flatten(json):

        def __f(r, x):
            if "fields" in x.keys():
                if type(x["fields"]) is list:
                    for fields in x["fields"]:
                        field = {}
                        if type(fields["type"]) is str:
                            m = fields["metadata"]
                            if "literal" in m.keys():
                                field = {fields["name"]: m["literal"]}
                        else:
                            inner = __f(r, fields["type"])
                            field = {fields["name"]: inner}
                        r = mergedicts(r, field)
                    return r
                elif type(x["fields"]) is dict:
                    r = mergedicts(r, {x["name"]: {}})
                    return __f(r, x["type"])
            elif x["type"] == "string":
                m = x["metadata"]
                if "literal" in m:
                    field = {x["name"]: m["literal"]}
                    r = mergedicts(r, field)
                    return r

        return __f({}, json)

    def __init__(self, variant):

        event = StructType() \
            .add("spdqid", StringType(), nullable=False, metadata={"splunk": "substr(sha512(host + \"::\" + _raw), 1, %d),"}) \
            .add("spdqbkt", StringType(), nullable=False, metadata={"splunk": "\"%s\","}) \
            .add("spdqdata", StringType(), nullable=False, metadata={"splunk": "_raw,"}) \
            .add("spdqidxn", StringType(), nullable=False, metadata={"splunk": "_index,"}) \
            .add("spdqephn", StringType(), nullable=False, metadata={"splunk": "host,"}) \
            .add("spdqsrc", StringType(), nullable=False, metadata={"splunk": "source,"}) \
            .add("spdqstyp", StringType(), nullable=False, metadata={"splunk": "sourcetype,"}) \
            .add("spdqtskey", StringType(), nullable=False, metadata={"splunk": "strftime(_time, \"%%Y%%m%%d%%H%%M%%S\"),"}) \
            .add("spdqtstxt", StringType(), nullable=False, metadata={"splunk": "strftime(%s, \"%s\"),"}) \
            .add("spdqtssrc", StringType(), nullable=False, metadata={"splunk": "_time"})


        spdqh = StructType() \
            .add("name", StringType(), nullable=False, metadata={"literal": socket.gethostname()}) \
            .add("fqdn", StringType(), nullable=False, metadata={"literal": socket.getfqdn()}) \
            .add("ips", StringType(), nullable=False, metadata={"literal": ",".join(socket.gethostbyname_ex(socket.gethostname())[-1])})

        meta = StructType() \
            .add("sent", StringType(), nullable=False, metadata={"literal": "%s"}) \
            .add("spdqv", StringType(), nullable=False, metadata={"literal": globals.__VERSION__}) \
            .add("spdqh", spdqh)

        spdq = StructType(). \
            add("meta", meta). \
            add("data", ArrayType(event))
        #
        self.sparksqlstruct = StructType(). \
            add("spdq", spdq)

        jv = self.sparksqlstruct.jsonValue()
        x = self._flatten(jv)

        template_message = {
              "spdq": {
                "meta": {
                  "sent": "%timestamp%",
                  "spdqv": "%globals.__VERSION__%",
                  "spdqh": {
                    "name": "%socket.gethostname()%",
                    "fqdn": "%socket.getfqdn()%",
                    "ips": "%\",\".join(socket.gethostbyname_ex(socket.gethostname())[-1])%"
                  }
                },
                "data": [
                    {
                        "spdqbkt": "",
                        "spdqdata": "",
                        "spdqephn": "",
                        "spdqid": "",
                        "spdqsrc": "",
                        "spdqstyp": "",
                        "spdqtskey": "",
                        "spdqtstxt": "",
                        "spdqtssrc": "",
                        "spdqx": ""
                    }
                ]
              }
        }

        #
        self.json = {
            "spdq": {
                    "meta": {
                            "sent": "%s",
                            "spdqv": globals.__VERSION__,
                            "spdqh": {
                                "name": socket.gethostname(),
                                "fqdn": socket.getfqdn(),
                                "ips": ",".join(socket.gethostbyname_ex(socket.gethostname())[-1]),
                            },
                        },
                    "data": []
                }
            }


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

