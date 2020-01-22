from collections import deque
import threading
import time

from spextral.core.utils import containsdupevalues, info, nowint, nowstr


class Instrumentation:
    """Used to track per-thread execution statistics, errors, and status, and report back on them."""
    def __init__(self, manager):
        self.manager = manager
        # If you add anything here, remember to also add it to InstrumentationManager.new() below
        self.tag = "main"
        self.status = "OK"
        self.message = None
        self.running = None
        self.start = None
        self.started = None
        self.end = None
        self.ended = None
        self.elapsed = 0.0
        self.counter = 0
        self.max_limit_reached = False
        self._counters = deque(maxlen=60)
        self._persecond = None
        self._now = None
        self._last = None
        self.avgrps = 0.0
        self.minrps = 0.0
        self.maxrps = 0.0

    def __enter__(self):
        self.running = True
        self.start = time.time()
        self.started = nowstr()
        self.elapsed = 0.0
        self.counter = 0
        self._now = nowint()
        self._last = self._now
        self._persecond = 0
        self.avgrps = 0.0
        self.minrps = 0.0
        self.maxrps = 0.0

    def __exit__(self, exception_type=None, exception_value=None, traceback=None):
        if not traceback:
            self.running = False
            self.message = "COMPLETED" if self.message == "RUNNING" else self.message
            self.end = time.time()
            self.ended = nowstr()
            self.elapsed = self.end - self.start
        else:
            self.status = "ERROR"
            self.message = exception_value

    def get(self, tag):
        """Creates a new instance of an Instrumentation object."""
        return self.manager.new(self, tag)

    def increment(self):
        """Increment the current instrumentation object's counter field and related statistics."""
        self.counter += 1
        self._persecond += 1
        self._now = nowint()
        if self._now > self._last:
            self._counters.append(self._persecond)
            self._persecond = 0
            self._last = self._now
        if self._now % 30 == 0:
            self.avgrps = sum(self._counters) / len(self._counters) if len(self._counters) > 0 else 0.0
            j = min(self._counters)
            if j < self.minrps:
                self.minrps = j
            j = max(self._counters)
            if j > self.maxrps:
                self.maxrps = j


class InstrumentationCollection:

    def __init__(self, groupname):
        self.groupname = groupname.strip().lower()
        self.tags = {}
        self.running = {}
        self.statuses = {}
        self.messages = {}
        self.started = {}
        self.ended = {}
        self.elapsed = {}
        self.counters = {}
        self.avgrps = {}
        self.minrps = {}
        self.maxrps = {}
        self.error = False
        self.errors = []
        self.max_limit_reached = False

    def print(self):
        """Print all of the instrumentation stats for all of the threads in the current thread pool."""
        for t in self.tags.keys():
            info("Batch thread statistics for %s:" % self.tags[t])
            info("* Started: %s" % self.started[t])
            info("* Ended:   %s" % self.ended[t])
            info("* Elapsed: %d secs" % round(self.elapsed[t], 2))
            info("* Status:  %s" % self.statuses[t])
            info("* Count:   %s" % self.counters[t])
            info("* AvgRPS:  %s" % self.avgrps[t])
            info("* MinRPS:  %s" % self.minrps[t])
            info("* MaxRPS:  %s" % self.maxrps[t])


class InstrumentationManager:

    class __SingletonSIM:

        def __init__(self, engine):
            self.engine = engine
            self.groups = {}
            self.players = {}
            self.instruments = {}
            pass

        def __str__(self):
            return repr(self)

    instance = None

    def __init__(self, engine):
        if not InstrumentationManager.instance:
            InstrumentationManager.instance = InstrumentationManager.__SingletonSIM(engine)
        else:
            InstrumentationManager.instance.engine = engine

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def _registered(self, tid):
        if tid in self.instance.players.keys():
            if self.instance.players[tid] in self.instance.instruments.keys():
                return True
        return False

    def register(self, groupname):
        """Define a collection of Instrumentation objects for all of the threads in a given named group or 'pool' of threads."""
        tid = threading.current_thread().ident
        if not self._registered(tid):
            groupkey = groupname.strip().lower()
            if groupkey in self.instance.groups.keys():
                self.instance.groups[groupkey].append(tid)
            else:
                self.instance.groups[groupkey] = [tid]
            while tid not in self.instance.players.keys():
                self.instance.players[tid] = len(self.instance.players)+1
                # did another thread grab the same len value just now?
                # should be rare occurrence, but... maybe
                # fast check for dupe dict values
                if containsdupevalues(self.instance.players):
                    # try again
                    del self.instance.players[tid]
            self.instruments[self.instance.players[tid]] = Instrumentation(manager=self)

    def get(self, tag="main"):
        """Returns the Instrumentation object for the current thread."""
        tid = threading.current_thread().ident
        return self.new(self.instance.instruments[self.instance.players[tid]], tag)

    def collect(self, groupname):
        """Finalizes the stats for all of the Instrumentation objects in the named thread group or 'pool'."""
        collected = InstrumentationCollection(groupname)
        collected.totcount = 0
        numplayers = len(self.instance.players)
        groupkey = groupname.strip().lower()
        for tid in self.instance.groups[groupkey]:
            player = self.instance.players[tid]
            instrument = self.instance.instruments[player]
            collected.tags.update({player: instrument.tag})
            collected.running.update({player: instrument.running})
            collected.messages.update({player: instrument.message})
            collected.statuses.update({player: instrument.status})
            collected.started.update({player: instrument.started})
            collected.ended.update({player: instrument.ended})
            collected.elapsed.update({player: instrument.elapsed})
            collected.counters.update({player: instrument.counter})
            collected.avgrps.update({player: instrument.avgrps})
            collected.minrps.update({player: instrument.minrps})
            collected.maxrps.update({player: instrument.maxrps})
            if instrument.status == "ERROR":
                collected.error = True
                collected.errors.append(instrument.message)
            if instrument.max_limit_reached:
                collected.max_limit_reached = True
        return collected

    def collectall(self):
        """Finalizes the stats for all of the Instrumentation objects in all named thread groups or 'pools'."""
        collection = []
        for groupkey in self.instance.groups.keys():
            collection.append(self.collect(groupkey))
        return collection

    def printall(self, collectionlist):
        """Print all of the instrumentation stats for all of the threads in all of the thread pools."""
        for collection in collectionlist:
            collection.print()

    @staticmethod
    def new(instrumentationinstance, tag="main"):
        """Clears/resets an existing Instrumentation object in a threadsafe way."""
        instrumentationinstance.tag = tag
        instrumentationinstance.status = "OK"
        instrumentationinstance.message = None
        instrumentationinstance.running = None
        instrumentationinstance.start = None
        instrumentationinstance.started = None
        instrumentationinstance.end = None
        instrumentationinstance.ended = None
        instrumentationinstance.elapsed = 0.0
        instrumentationinstance.counter = 0
        instrumentationinstance.max_limit_reached = False
        instrumentationinstance.avgrps = 0.0
        instrumentationinstance.minrps = 0.0
        instrumentationinstance.maxrps = 0.0
        instrumentationinstance._counters.clear()
        return instrumentationinstance

    @property
    def status(self):
        """Returns the Status field value in the current thread's Instrumentation object."""
        for gk in self.instance.groups.keys():
            for tid in self.instance.groups[gk]:
                player = self.instance.players[tid]
                instrument = self.instance.instruments[player]
                if instrument.status == "ERROR":
                    return "ERROR"
        return "OK"

    @property
    def errormessages(self):
        """Returns any error messages in the current thread's Instrumentation object."""
        msg = ""
        for gk in self.instance.groups.keys():
            for tid in self.instance.groups[gk]:
                player = self.instance.players[tid]
                instrument = self.instance.instruments[player]
                if instrument.status == "ERROR":
                    msg = "%s[%s]: %s\r\n" % (msg, instrument.tag, instrument.message)
        return msg