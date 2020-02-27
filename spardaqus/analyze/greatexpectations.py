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
from spardaqus.core.exceptions import SpardaqusTimeout, SpardaqusWaitExpired, SpardaqusMessageParseError
from spardaqus.core.metaclasses import SpardaqusAnalyzer
from spardaqus.core.utils import boolish,\
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
        self.target = self.config("master", required=True)
        self.bucket = self.getbucket()
        self.session = None
        self.limit_reached = False
        self.results_returned = True
        self.timeoutmsg = "%s operation failed: connection refused by %s at %s" \
                          % (self.engine.options.operation.capitalize(), self.integration_capitalized, self.target)
        self.thread_count = 1
        self.maxwait = self.config("maxwait", required=False, defaultvalue=0)

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

    def append2results(self, rawmsgstr):
        rawmsg = json.loads(rawmsgstr)
        if "spdq" not in rawmsg.keys():
            raise SpardaqusMessageParseError
        if "data" not in  rawmsg["spdq"].keys():
            raise SpardaqusMessageParseError
        eventlist = rawmsg["spdq"]["data"]
        if type(eventlist) is not list:
            raise SpardaqusMessageParseError
        if type(eventlist[0]) is not dict:
            raise SpardaqusMessageParseError
        if self.results.columns.empty:
            self.results = pd.DataFrame(columns=eventlist[0].keys())
        df = pd.json_normalize(eventlist, max_level=0)
        self.results = pd.concat([self.results, df], axis=0)
        return

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
                            rawmsg = self.engine.service.que.get_nowait()
                        except QueueEmpty:
                            if globals.KILLSIG or \
                                    (instrumentation.counter >= 1 and (
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
                self.append2results(rawmsg)
                instrumentation.increment()
                rawmsg = None
                if self.engine.options.limit > 0:
                    if self.engine.options.limit == instrumentation.counter:
                        self.limit_reached = True
                        info("--limit value (%d) reached" % self.engine.options.limit)
                        break
            if instrumentation.counter > 0:
                print(tabulate(self.results, headers='keys', tablefmt='psql'))
        self.close()

    def analyze(self, **kwargs):
        return

    def limit_reached(self):
        pass

    def results_returned(self):
        pass

    def close(self):
        globals.KILLSIG = True
