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
        self.start = None
        self.end = None
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

    def init(self):
        """Creates a query Window object used to extract data for import into Spextral with appropriate datetime endpoints persisted in Redis."""
        if self.endpoint.engine.options.init:
            self._deletekeys()
        edt = self._initend()
        sdt = self._initstart()
        self.state.set(self.endkey, edt)
        self.state.set(self.startkey, sdt)
        self.state.commit()

    def advance(self, current_as_of):
        """Moves the window forward in time to the next next query window in chronological order based on the window's current start/end times."""
        try:
            sdt = str(datetime.datetime.strftime(dtparser.parse(current_as_of) + datetime.timedelta(seconds=1), self.format))
            self.state.set(self.startkey, sdt)
            self.state.commit()
            if self.lower_bound:
                if sdt < self.lower_bound:
                    sdt = self.lower_bound
            self.start = str(datetime.datetime.strftime(dtparser.parse(sdt), self.format))
            info("Next batch start date advanced to %s" % sdt)
        except Exception as e:
            error("Trying to advance window start date: %s" % str(e))
        return

    def _deletekeys(self):
        self.state.delete(self.startkey)
        self.state.delete(self.endkey)

    def _initstart(self):
        sdt = self.state.get(self.startkey)
        if not sdt:
            sdt = self.endpoint.earliest
            if not sdt:
                sdt = str(datetime.datetime.strftime(self.batchstartdt - datetime.timedelta(seconds=7776000), self.format))
            else:
                sdt = str(datetime.datetime.strftime(dtparser.parse(sdt) + datetime.timedelta(seconds=self.lower_offset), self.format))
        if self.lower_bound:
            if sdt < self.lower_bound:
                sdt = self.lower_bound
        self.start = str(datetime.datetime.strftime(dtparser.parse(sdt), self.format))
        return sdt

    def _initend(self):
        edt = self.state.get(self.endkey)
        if not edt:
            edt = self.endpoint.latest
            if not edt:
                edt = str(datetime.datetime.strftime(self.batchstartdt, self.format))
            else:
                edt = str(datetime.datetime.strftime(dtparser.parse(edt) + datetime.timedelta(seconds=self.upper_offset), self.format))
        if self.upper_bound:
            if edt > self.upper_bound:
                edt = self.upper_bound
        self.end = str(datetime.datetime.strftime(dtparser.parse(edt), self.format))
        return edt

