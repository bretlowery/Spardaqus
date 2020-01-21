import datetime
from dateutil import parser as dtparser

from spextral.core.utils import \
    error, info


class Window:

    def __init__(self, endpointinstance):
        self.batchstartdt = datetime.datetime.now()
        self.endpoint = endpointinstance
        self.format = "%Y%m%d%H%M%S"
        self.lower_offset = 0
        self.upper_offset = 0
        self.lower_bound = None
        self.upper_bound = None
        self.epochstart = None
        self.epochend = None
        earliest = self.endpoint.config("earliest", defaultvalue=None)
        if isinstance(earliest, int):
            if earliest < 0:
                self.lower_bound = str(datetime.datetime.strftime(self.batchstartdt - datetime.timedelta(seconds=abs(earliest)),  self.format))
            else:
                self.lower_offset = earliest
        latest = self.endpoint.config("latest", defaultvalue=None)
        if isinstance(latest, int):
            if latest < 0:
                self.upper_bound = str(datetime.datetime.strftime(self.batchstartdt - datetime.timedelta(seconds=abs(latest)),  self.format))
            else:
                self.upper_offset = latest
        self.startkey = self.endpoint.key + "::sdt"
        self.endkey = self.endpoint.key + "::edt"
        self.state = self.endpoint.engine.service.state

    def open(self):
        """Creates a query Window object used to extract data for import into Spextral with appropriate datetime endpoints persisted in Redis."""
        if self.endpoint.engine.options.init:
            self._deletekeys()
        e = self._initend()
        s = self._initstart()
        self.state.set(self.endkey, e)
        self.state.set(self.startkey, s)
        self.state.commit()

    def advance(self, current_as_of):
        """Moves the window forward in time to the next next query window in chronological order based on the window's current start/end times."""
        try:
            s = str(int(current_as_of) + 1)
            self.state.set(self.startkey, s)
            self.state.commit()
            self.epochstart = s
            info("Next batch start date advanced to %s (%s)" % (s, self.start))
        except Exception as e:
            error("Trying to advance window start date: %s" % str(e))
        return

    def _deletekeys(self):
        self.state.delete(self.startkey)
        self.state.delete(self.endkey)

    def _initstart(self):
        s = self.state.get(self.startkey)
        if not s:
            s = self.endpoint.earliest
            if not s:
                s = str((self.batchstartdt - datetime.timedelta(seconds=7776000)).fromtimestamp(0))
            else:
                s = str(int(s) + self.lower_offset)
        if self.lower_bound:
            st = str(datetime.datetime.strptime(self.lower_bound, self.format).fromtimestamp(0))
            if s < st:
                s = st
        self.epochstart = s
        return s

    def _initend(self):
        e = self.state.get(self.endkey)
        if not e:
            e = self.endpoint.latest
            if not e:
                e = str(self.batchstartdt.fromtimestamp(0))
            else:
                e = str(int(e) + self.upper_offset)
        if self.upper_bound:
            et = str(datetime.datetime.strptime(self.upper_bound, self.format).fromtimestamp(0))
            if e > et:
                e = et
        self.epochend = e
        return e

    @property
    def start(self):
        return datetime.datetime.utcfromtimestamp(int(self.epochstart)).strftime(self.format)

    @property
    def end(self):
        return datetime.datetime.utcfromtimestamp(int(self.epochend)).strftime(self.format)
