import socket
from pyspark.sql.types import StringType, StructType, ArrayType
from spextral import globals


class SpextralMessage:

    def __init__(self):
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
            spxh = StructType() \
                .add("name", StringType(), nullable=False, metadata={"val": socket.gethostname()}) \
                .add("fqdn", StringType(), nullable=False, metadata={"val": socket.getfqdn()}) \
                .add("ips", StringType(), nullable=False, metadata={"val": ",".join(socket.gethostbyname_ex(socket.gethostname())[-1])})
            meta = StructType() \
                .add("sent", StringType(), nullable=False, metadata={"val": "%s"}) \
                .add("spxv", StringType(), nullable=False, metadata={"val": globals.__VERSION__}) \
                .add("spxh", spxh)
            event = StructType() \
                .add("spxtrlbkt", StringType(), nullable=False) \
                .add("spxtrldata", StringType(), nullable=False) \
                .add("spxtrlephn", StringType(), nullable=False) \
                .add("spxtrlid", StringType(), nullable=False) \
                .add("spxtrlsrc", StringType(), nullable=False) \
                .add("spxtrlstyp", StringType(), nullable=False) \
                .add("spxtrlts1", StringType(), nullable=False) \
                .add("spxtrlts2", StringType(), nullable=False) \
                .add("spxtrlts3", StringType(), nullable=False) \
                .add("spxtrlx", StringType(), nullable=False)
            spxtrl = StructType(). \
                add("meta", meta). \
                add("data", ArrayType(event))
            #
            self.sparksql = StructType(). \
                add("spxtrl", spxtrl)
            #
            self.json = {
                "spxtrl": {
                        "meta": {
                                "sent": "%s",
                                "spxv": globals.__VERSION__,
                                "spxh": {
                                    "name": socket.gethostname(),
                                    "fqdn": socket.getfqdn(),
                                    "ips": ",".join(socket.gethostbyname_ex(socket.gethostname())[-1]),
                                },
                            },
                        "data": []
                    }
                }
