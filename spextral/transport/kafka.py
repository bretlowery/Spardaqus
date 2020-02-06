import os
import datetime
import json
from queue import Empty as QueueEmpty
import socket
import string
import subprocess
from time import sleep

from confluent_kafka import KafkaException, KafkaError, Producer, Consumer

from spextral import globals
from spextral.core.decorators import timeout_after
from spextral.core.exceptions import SpextralTimeoutWarning
from spextral.core.metaclasses import SpextralTransport
from spextral.core.utils import istruthy, mergedicts, getenviron


class Kafka(SpextralTransport):

    def __init__(self, engine):
        self.engine = engine
        super().__init__(self.__class__.__name__)
        self.target = self.config("bootstrap.servers", required=True)
        self.groupid = self.config("group.id", required=True, intrange=[0, 999], defaultvalue=0)
        self.multithread = istruthy(self.config("multithread", required=False, defaultvalue=True))
        self.threads = self.config("threads", required=False, defaultvalue=2, intrange=[1, 64])
        self.idempotence = istruthy(self.config("idempotence", required=True, defaultvalue=True))
        common_kwargs = {
            "group.id": self.groupid,
            "bootstrap.servers": self.target,
            "enable.idempotence": self.idempotence
        }
        if self.engine.options.operation in ["extract"]:
            transporter_kwargs = common_kwargs
            self.loss_tolerance = self.config("loss_tolerance", required=False, defaultvalue='zero', choices=['zero', 'high', 'low'])
            if self.loss_tolerance == "zero":
                transporter_kwargs = mergedicts(transporter_kwargs, {"request.required.acks": "all"})
            elif self.loss_tolerance == "high":
                transporter_kwargs = mergedicts(transporter_kwargs, {"request.required.acks": "1"})
            else:
                transporter_kwargs = mergedicts(transporter_kwargs, {"request.required.acks": "0"})
            transporter_options = self.config("producer.options", required=False, defaultvalue=None)
            if transporter_options:
                for k in transporter_options.keys():
                    if k not in transporter_kwargs.keys():
                        transporter_kwargs[k] = transporter_options[k]
            self.transporter_options = transporter_kwargs
        elif self.engine.options.operation in ["analyze", "dump"]:
            transporter_kwargs = mergedicts(common_kwargs, {'auto.offset.reset': 'earliest'})
            transporter_options = self.config("consumer.options", required=False, defaultvalue=None)
            if transporter_options:
                for k in transporter_options.keys():
                    if k not in transporter_kwargs.keys():
                        transporter_kwargs[k] = transporter_options[k]
            self.transporter_options = transporter_kwargs
        self.bucket = self.getbucket()
        self.envelope = {
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
        self.timeoutmsg = "%s operation failed: connection refused by %s at %s" % (self.engine.options.operation.capitalize(), self.integration.capitalize(), self.target)
        self.maxwait = self.config("maxwait", required=False, defaultvalue=0)

    def getbucket(self):
        config_bucket = self.config("topic", required=False, defaultvalue=None)
        if config_bucket and config_bucket not in ["none", "default"]:
            bucket = config_bucket.strip().translate(str.maketrans(string.punctuation, '_' * len(string.punctuation)))
        else:
            bucket = 'spextral'
        return bucket[:255]

    @property
    def version(self):
        kafka_libs = os.path.join(getenviron("KAFKA_HOME"), "libs")
        version = subprocess.check_output("find %s -name \\*kafka_\\* | head -1 | grep -o '\\kafka[^\\\\n]*' | awk -F\"-\" '{print $2}'" % kafka_libs, shell=True)\
            .decode('ascii')\
            .rstrip()
        return version

    def connect(self):
        """Connect to the Kafka instance specified in the extract.yaml's (when extracting) or analyze.yaml's (when analyzing) Kafka connection settings."""
        if self.engine.options.operation == "extract":
            self.info("Connecting to %s transport server at %s as a publisher" % (self.integration.capitalize(), self.target))
            self.transporter = Producer(**self.transporter_options)
        else:
            self.info("Connecting to %s transport server at %s as a subscriber" % (self.integration.capitalize(), self.target))
            self.transporter = Consumer(**self.transporter_options)
        if self.connected:
            self.info("Connected to %s %s" % (self.integration.capitalize(), self.version))
        return

    @timeout_after(20)
    def _chkconnection(self):
        dummy = self.transporter.list_topics(self.bucket)  # this will timeout if Kafka is down
        return True                                        # if the prev statement didn't timeout we are connected

    @property
    def connected(self):
        """Returns True if connected to Kafka, False otherwise."""
        try:
            is_connected = self._chkconnection()
        except:
            is_connected = False
            pass
        return is_connected

    def _threadend(self):
        """
        THREADSAFETY REQUIRED
        Thread teardown tasks
        :return:
        """
        self.close()

    def send(self, argstuple):
        """
        THREADSAFETY REQUIRED
        Send data on the service queue to a Kafka topic.
        :param argstuple:
        :return:
        """
        def __kafkacallback(err, msg):
            if err is not None:
                raise KafkaException(err)

        def _packit(data, encoding):
            packit = self.envelope
            packit["spxtrl"]["data"] = [dict(data)]   # convert OrderedDict to dict, then listify
            packit["spxtrl"]["meta"]["sent"] = datetime.datetime.now().isoformat()
            return json.dumps(packit)

        que = argstuple[0]
        n = argstuple[1]
        thread_name = "%s transport thread %d" % (self.integration.capitalize(), n)
        self.engine.service.instrumenter.register(groupname=self.integration)
        self.info("Starting %s" % thread_name)
        instrumentation = self.engine.service.instrumenter.get(thread_name)
        wait_ticks = 0
        queue_data = None
        exit_thread = False
        with instrumentation:
            while not exit_thread:
                while not queue_data:
                    try:
                        queue_data = que.get_nowait()
                    except QueueEmpty:
                        if globals.KILLSIG or \
                                (instrumentation.counter > 1 and (
                                    0 < self.engine.options.limit <= instrumentation.counter
                                    or (0 < self.maxwait < wait_ticks)
                                    or self.engine.options.command == 'stop'
                                    or self.engine.endpoint.queue_complete
                                )
                                ):
                            exit_thread = True
                            break
                        queue_data = None
                        sleep(1)
                        wait_ticks += 1
                        pass
                if exit_thread or globals.KILLSIG:
                    break
                wait_ticks = 0
                emptyticks = 0
                try:
                    # send data
                    encoded_packet = _packit(queue_data, self.engine.encoding)
                    self.transporter.produce(self.bucket, encoded_packet, on_delivery=__kafkacallback)
                    self.transporter.poll(0)
                    instrumentation.increment()
                    if instrumentation.counter % 1000 == 0:
                        if self.engine.endpoint.queue_complete:
                            self.info("Queueing complete; sent %d via %s" % (instrumentation.counter, thread_name))
                        self.transporter.flush()
                except KafkaException as kx:
                    self._threadend()
                    self.error("KafkaException transporting via %s: %s" % (thread_name, str(kx)))
                except KafkaError as ke:
                    self._threadend()
                    self.error("KafkaError transporting via %s: %s" % (thread_name, str(ke)))
                except Exception as e:
                    self._threadend()
                    self.error("exception transporting via %s: %s" % (thread_name, str(e)))
                    raise e
                queue_data = None
        self._threadend()
        return instrumentation

    @timeout_after(10, "Transport queue is empty")
    def _poll(self):
        return self.transporter.poll()

    def dump(self):
        """Unloads all Spextral messages from Kafka and prints them out to the console instead of sending them to Spark."""
        self.transporter.subscribe([self.bucket])
        msg = None
        try:
            while True:
                msg = None
                msg = self._poll()
                if not msg:
                    break
                print(msg.value().decode(self.engine.encoding))
        except SpextralTimeoutWarning:
            pass
        except KafkaException as kx:
            self.error("KafkaException: %s" % str(kx))
        except KafkaError as ke:
            self.error("KafkaError: %s" % str(ke))
        except Exception as e:
            self.error("Exception: %s" % str(e))
        finally:
            globals.KILLSIG = True

    @property
    def closed(self):
        """Returns True if the connection to Kafka is closed/terminated/inactive, False otherwise."""
        if self.transporter:
            return False
        else:
            return True

    @timeout_after(10)
    def close(self):
        """Closes an open connection to Kafka."""
        try:
            if self.engine.options.operation == "extract":
                self.transporter.flush()
            elif self.engine.options.operation == "analyze":
                self.transporter.close()
        except:
            pass
        return True

