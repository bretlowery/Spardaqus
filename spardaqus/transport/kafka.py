import os
import datetime
import json
from queue import Empty as QueueEmpty
import string
import subprocess
from time import sleep

from confluent_kafka import Producer
from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from confluent_kafka import KafkaError

from spardaqus import globals
from spardaqus.core.decorators import timeout_after
from spardaqus.core.exceptions import SpardaqusTimeout, SpardaqusWaitExpired, SpardaqusMessageCRCMismatchError
from spardaqus.core.metaclasses import SpardaqusTransport
from spardaqus.core.types import SpardaqusTransportStatus
from spardaqus.core.utils import istruthy, mergedicts, getenviron, info, error, exception, crc


class Kafka(SpardaqusTransport):

    def __init__(self, engine):
        self.engine = engine
        super().__init__(self.__class__.__name__)
        self.status = SpardaqusTransportStatus.EMPTY
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
        self.timeoutmsg = "%s operation failed: connection refused by %s at %s" % (self.engine.options.operation.capitalize(), self.integration_capitalized, self.target)
        self.maxwait = self.config("maxwait", required=False, defaultvalue=0)
        self.state = SpardaqusTransportStatus.STARTING

    @property
    def version(self):
        kafka_libs = os.path.join(getenviron("KAFKA_HOME"), "libs")
        version = subprocess.check_output("find %s -name \\*kafka_\\* | head -1 | grep -o '\\kafka[^\\\\n]*' | awk -F\"-\" '{print $2}'" % kafka_libs, shell=True)\
            .decode('ascii')\
            .rstrip()
        return version

    def connect(self):
        """Connect to the Kafka instance specified in the extract.yaml's (when extracting) or analyze.yaml's (when analyzing) Kafka connection settings."""
        self.transporter = None
        if self.engine.worker == "extract":
            info("Connecting to %s transport server at %s as a publisher" % (self.integration_capitalized, self.target))
            self.transporter = Producer(**self.transporter_options)
        else:
            info("Connecting to %s transport server at %s as a subscriber" % (self.integration_capitalized, self.target))
            self.transporter = Consumer(**self.transporter_options)
        if self.connected:
            info("Connected to %s %s" % (self.integration_capitalized, self.version))
        return

    @timeout_after(20)
    def _chkconnection(self):
        dummy = self.transporter.list_topics(self.bucket)  # this will timeout if Kafka is down
        return True                                        # if the prev statement didn't timeout we are connected


    @property
    def connected(self):
        """Returns True if connected to Kafka, False otherwise."""
        is_connected = False
        try:
            is_connected = self._chkconnection()
        except:
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
        envelope = self.engine.service.message_schema.json_envelope

        def __kafkacallback(err, msg):
            if err is not None:
                raise KafkaException(err)

        def _packit(data):
            msg = envelope
            msg["spdq"]["data"] = [dict(data)]   # convert OrderedDict to dict, then listify
            msg["spdq"]["meta"]["sent"] = datetime.datetime.now().isoformat()[:19]
            # msg["spdq"]["crc"] = crc(msg["spdq"]["meta"], msg["spdq"]["data"])
            return json.dumps(msg)

        que = argstuple[0]
        n = argstuple[1]
        thread_name = "%s transport thread %d" % (self.integration_capitalized, n)
        self.engine.service.instrumenter.register(groupname=self.integration)
        info("Starting %s" % thread_name)
        instrumentation = self.engine.service.instrumenter.get(thread_name)
        wait_ticks = 0
        rawmsg = None
        exit_thread = False
        with instrumentation:
            while not exit_thread:
                while not rawmsg:
                    try:
                        rawmsg = que.get_nowait()
                    except QueueEmpty:
                        if globals.KILLSIG or \
                                (instrumentation.counter >= 1 and (
                                    0 < self.engine.options.limit <= instrumentation.counter
                                    or self.engine.options.command == 'stop'
                                    or self.engine.endpoint.queue_complete
                                )
                                ):
                            exit_thread = True
                            break
                        self.status = SpardaqusTransportStatus.EMPTY
                        rawmsg = None
                        sleep(1)
                        wait_ticks += 1
                        if 0 < self.maxwait < wait_ticks:
                            raise SpardaqusWaitExpired
                        pass
                    except SpardaqusWaitExpired:
                        self.status = SpardaqusTransportStatus.WAITEXPIRED
                        info("Max %ds wait time for new messages exceeded; exiting" % self.maxwait)
                        exit_thread = True
                        pass
                if exit_thread or globals.KILLSIG:
                    break
                self.status = SpardaqusTransportStatus.PROCESSING
                wait_ticks = 0
                try:
                    # send data
                    encoded_packet = _packit(rawmsg)
                    self.transporter.produce(self.bucket, encoded_packet, on_delivery=__kafkacallback)
                    self.transporter.poll(0)
                    instrumentation.increment()
                    if instrumentation.counter % 1000 == 0:
                        if self.engine.endpoint.queue_complete:
                            info("Queueing complete; sent %d via %s" % (instrumentation.counter, thread_name))
                        self.transporter.flush()
                except KafkaException as kx:
                    self._threadend()
                    exception("KafkaException transporting via %s: %s" % (thread_name, str(kx)))
                except KafkaError as ke:
                    self._threadend()
                    error("KafkaError transporting via %s: %s" % (thread_name, str(ke)))
                except Exception as e:
                    self._threadend()
                    exception("exception transporting via %s: %s" % (thread_name, str(e)))
                    raise e
                rawmsg = None
        self._threadend()
        return instrumentation

    @timeout_after(10, "Transport queue is empty")
    def _poll(self):
        return self.transporter.poll()

    def receive(self, argstuple=None):
        """
       THREADSAFETY REQUIRED
       Unloads all Spardaqus messages from Kafka and sends them to the analyze endpoint.
       :param argstuple:
       :return:
        """
        if argstuple:
            que = argstuple[0]
            n = argstuple[1]
            thread_name = "%s transport thread %d" % (self.integration_capitalized, n)
            info("Starting %s" % thread_name)
        else:
            que = None
            thread_name = "%s transport thread 1"
        self.engine.service.instrumenter.register(groupname=self.integration)
        instrumentation = self.engine.service.instrumenter.get(thread_name)
        wait_ticks = 0
        exit_thread = False
        with instrumentation:
            while not exit_thread:
                self.transporter.subscribe([self.bucket])
                try:
                    while True:
                        rawmsg = self._poll()
                        if rawmsg:
                            self.status = SpardaqusTransportStatus.PROCESSING
                            msg = rawmsg.value().decode(self.engine.encoding)
                            #if msg["spdq"]["crc"] != crc(msg["spdq"]["meta"], msg["spdq"]["data"]):
                            #    raise SpardaqusMessageCRCMismatchError
                            que.put(msg) if que else print(msg)
                            instrumentation.increment()
                            wait_ticks = 0
                        else:
                            sleep(1)
                            wait_ticks += 1
                        if globals.KILLSIG or \
                                (instrumentation.counter >= 1 and (
                                        0 < self.engine.options.limit <= instrumentation.counter
                                        or self.engine.options.command == 'stop'
                                )
                                ):
                            exit_thread = True
                            break
                        if 0 < self.maxwait < wait_ticks:
                            raise SpardaqusWaitExpired
                except SpardaqusMessageCRCMismatchError:
                    pass
                except SpardaqusWaitExpired:
                    self.status = SpardaqusTransportStatus.WAITEXPIRED
                    info("Max %ds wait time for new messages exceeded; exiting" % self.maxwait)
                    exit_thread = True
                    pass
                except SpardaqusTimeout:
                    self.status = SpardaqusTransportStatus.EMPTY
                    exit_thread = True
                    pass
                except KafkaException as kx:
                    self._threadend()
                    exception("KafkaException transporting via %s: %s" % (thread_name, str(kx)))
                except KafkaError as ke:
                    self._threadend()
                    error("KafkaError transporting via %s: %s" % (thread_name, str(ke)))
                except Exception as e:
                    self._threadend()
                    exception("exception transporting via %s: %s" % (thread_name, str(e)))
                    raise e

    def dump(self):
        """Unloads all Spardaqus messages from Kafka and prints them out to the console instead of sending them to analyze endpoint."""
        return self.receive()

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

