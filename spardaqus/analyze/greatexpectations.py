import concurrent.futures as futures
from contextlib import nullcontext
from functools import lru_cache
import json
import os
from queue import Queue
from queue import Empty as QueueEmpty
import string
import sys
from tabulate import tabulate
from time import sleep

import great_expectations as ge
import pandas as pd

from spardaqus import globals
from spardaqus.core.decorators import timeout_after
from spardaqus.core.exceptions import SpardaqusTimeout, SpardaqusWaitExpired, SpardaqusMessageParseError, SpardaqusMessageCRCMismatchError
from spardaqus.core.metaclasses import SpardaqusAnalyzer
from spardaqus.core.types import SpardaqusTransportStatus
from spardaqus.core.utils import boolish,\
    crc, \
    error,\
    exception,\
    getconfig,\
    getenviron,\
    info,\
    isreadable,\
    istruthy,\
    numcpus,\
    setenviron


class Greatexpectations(SpardaqusAnalyzer):

    def __init__(self, engine):
        self.engine = engine
        super().__init__(self.__class__.__name__)
        self.bucket = self.getbucket()
        self.session = None
        self.limit_reached = False
        self.results_returned = True
        self.timeoutmsg = "%s operation failed: connection refused by %s" \
                          % (self.engine.options.operation.capitalize(), self.integration_capitalized)
        self.thread_count = 1
        self.maxwait = self.config("maxwait", required=False, defaultvalue=0)
        self.timeout = self.maxwait

    def _getthreadcontext(self):
        multithread = istruthy(self.config("multithread", required=False, defaultvalue=True))
        if multithread:
            threads = self.config("threads", required=False, defaultvalue=1, intrange=[1, 64])
            self.thread_count = threads if threads > 0 else numcpus()
            self.thread_count = 1 if 0 < self.engine.options.limit < 10000 else self.thread_count
            thread_context = futures.ThreadPoolExecutor(self.thread_count)
        else:
            thread_context = nullcontext()
        info("Number of analysis threads set to %d" % self.thread_count)
        return thread_context

    def getbucket(self):
        configbucket = self.config("topic", required=False, defaultvalue=None)
        if configbucket and configbucket not in ["none", "default"]:
            bucket = configbucket.strip().translate(str.maketrans(string.punctuation, '_' * len(string.punctuation)))
        else:
            bucket = 'spardaqus'
        return bucket[:255]

    def connect(self, **kwargs):
        info("Starting %s analysis" % self.integration_capitalized)
        thread_context = self._getthreadcontext()
        for n in range(1, self.thread_count + 1):
            thread_context.submit(self.engine.transport.receive, (self.engine.service.que, n,))
        self.engine.service.instrumenter.register(groupname=self.integration)
        self.limit_reached = False
        self.results = pd.DataFrame()

    @property
    def connected(self):
        return True

    @staticmethod
    def _getmsgkey(event):
        return "%s:%s" % (event["spdqtskey"], event["spdqid"])

    @staticmethod
    def _getmsgdata(event):
        return event["spdqdata"]

    @staticmethod
    def _getevents(rawmsgstr):
        rawmsg = json.loads(rawmsgstr)
        return rawmsg["spdq"]["data"]

    def _append2results(self, rawmsgstr):
        eventlist = self._getevents(rawmsgstr)
        if self.results.columns.empty:
            self.results = pd.DataFrame(columns=eventlist[0].keys())
        df = pd.json_normalize(eventlist, max_level=0)
        self.results = pd.concat([self.results, df], axis=0)
        return

    @timeout_after(timeout_interval=None, timeout_message="Max wait time for first message exceeded")
    def _get(self, instrumentation_counter):
        return self.engine.service.que.get() if instrumentation_counter == 0 else self.engine.service.que.get_nowait()

    def dump(self):
        instrumentation = self.engine.service.instrumenter.get("GreatExpectationsDump")
        wait_ticks = 0
        rawmsg = None
        exit_thread = False
        with instrumentation:
            while not exit_thread:
                while not rawmsg:
                    try:
                        try:
                            rawmsg = self._get(instrumentation.counter)
                        except SpardaqusTimeout:
                            exit_thread = True
                            break
                        except QueueEmpty:
                            if globals.KILLSIG \
                                or self.engine.transport.status == SpardaqusTransportStatus.EMPTY \
                                or self.engine.transport.status == SpardaqusTransportStatus.WAITEXPIRED \
                                or (instrumentation.counter >= 1
                                    and (
                                            0 < self.engine.options.limit <= instrumentation.counter
                                            or self.engine.options.command == 'stop'
                                        )
                                    ):
                                exit_thread = True
                                break
                            rawmsg = None
                            sleep(1)
                            wait_ticks += 1
                            if 0 < self.maxwait < wait_ticks:
                                raise SpardaqusWaitExpired
                            pass
                    except SpardaqusWaitExpired:
                        info("Max %ds wait time for new messages exceeded; exiting" % self.maxwait)
                        exit_thread = True
                        break
                    except Exception as e:
                        exception("Exception during analysis: %s" % str(e))
                if exit_thread or globals.KILLSIG:
                    break
                if rawmsg:
                    self._append2results(rawmsg)
                    instrumentation.increment()
                    rawmsg = None
                    if 0 < self.engine.options.limit <= instrumentation.counter:
                        self.limit_reached = True
                        info("--limit value (%d) reached" % self.engine.options.limit)
                        break
            if instrumentation.counter > 0:
                print(tabulate(self.results, tablefmt='psql', headers="keys"))
                if self.engine.options.profile:
                    print("Total analysis dataframe memory usage: %d MB" % self.dataframe_memory_used_mb)
            elif self.engine.transport.status == SpardaqusTransportStatus.EMPTY:
                info("Nothing found in queue to dump analyze")
        self.close()
        

    @property
    def dataframe_memory_used_mb(self):
        return self.results.memory_usage(index=True).sum() / 1024.0 / 1024.0

    def analyze(self, **kwargs):
        return

    def limit_reached(self):
        pass

    def results_returned(self):
        pass

    def close(self):
        globals.KILLSIG = True
