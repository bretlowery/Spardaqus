import concurrent.futures as futures
from contextlib import nullcontext
from functools import lru_cache
import logging
import os
from queue import Queue
from queue import Empty as QueueEmpty
import string
import sys
from time import sleep


import great_expectations as ge
import pandas as pd
from pandas.io.json import json_normalize

from spextral import globals
from spextral.core.decorators import timeout_after
from spextral.core.exceptions import SpextralTimeout, SpextralWaitExpired
from spextral.core.metaclasses import SpextralAnalyzer
from spextral.core.utils import boolish,\
    error,\
    exception,\
    getconfig,\
    getenviron,\
    info,\
    isreadable,\
    istruthy,\
    numcpus,\
    setenviron


class Greatexpectations(SpextralAnalyzer):

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
            bucket = 'spextral'
        return bucket[:255]

    def connect(self, **kwargs):
        info("Starting %s analysis" % self.integration_capitalized)
        thread_context = self._getthreadcontext()
        for n in range(1, self.thread_count + 1):
            thread_context.submit(self.engine.transport.receive, (self.engine.service.que, n,))
        self.results = pd.DataFrame()

    @property
    def connected(self):
        return True

    def dump(self):
        instrumentation = self.engine.service.instrumenter.get("GreatExpectationsDump")
        wait_ticks = 0
        rawmsg = None
        exit_thread = False
        with instrumentation:
            while not exit_thread:
                while not rawmsg:
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
                            raise SpextralWaitExpired
                        pass
                    except SpextralWaitExpired:
                        info("Max %ds wait time for new messages exceeded; exiting" % self.maxwait)
                        exit_thread = True
                        pass
                if exit_thread or globals.KILLSIG:
                    break
                self.results.append(json_normalize(rawmsg))
            if instrumentation.counter > 0:
                with pd.option_context('display.max_rows', instrumentation.counter, 'display.max_columns', None):
                    print(self.results)

    def analyze(self, **kwargs):
        return

    def limit_reached(self):
        pass

    def results_returned(self):
        pass

    def close(self):
        pass

