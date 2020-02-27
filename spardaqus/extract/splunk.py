import concurrent.futures as futures
from contextlib import nullcontext
import hashlib
import io
import json
from time import sleep

import splunklib.client as client
import splunklib.results as results

from spardaqus import globals
from spardaqus.core.metaclasses import SpardaqusEndpoint
import spardaqus.core.profiling as profile
from spardaqus.core.streamio import SpardaqusStreamBuffer
from spardaqus.core.utils import numcpus, info, error, exception
from spardaqus.core.windowing import Window


class Splunk(SpardaqusEndpoint):

    def __init__(self, engine):
        self.engine = engine
        super().__init__(self.__class__.__name__)
        self.timestamp_field_name = self.config("timestamp_field_name", defaultvalue="_time")
        self.timestamp_field_format = self.config("timestamp_field_format", defaultvalue="%d/%b/%Y:%H:%M:%S %z")
        if self.timestamp_field_name != "_time":
            self.timestamp_field_name = 'strptime(%s, \"%s\")' % (self.timestamp_field_name, self.timestamp_field_format)
        self.sample_percentage = self.config("sample_percentage", defaultvalue=100, intrange=[1, 100])
        self.static_query_filter = self.config("query_filter", defaultvalue="")
        self.static_source_filter = self.config("source_filter", defaultvalue=None, quotestrings=True)
        self.static_sourcetype_filter = self.config("sourcetype_filter", defaultvalue=None, quotestrings=True)
        self.static_host_filter = self.config("host_filter", defaultvalue=None, quotestrings=True)
        timeout = self.config("timeout", defaultvalue=60)
        if timeout > 0:
            self.timeout = timeout
        k = (self.static_sourcetype_filter if self.static_sourcetype_filter is not None else "?") + "::" + \
            (self.static_source_filter if self.static_source_filter is not None else "?" + "::") + \
            (self.static_host_filter if self.static_host_filter is not None else "?" + "::") + \
            (self.static_query_filter if self.static_query_filter is not None else "?")
        self.key = hashlib.sha3_256(k.encode('utf-8')).hexdigest()
        self.forward = self.config("forward", defaultvalue=True, choices=[True, False])
        self.batch_goal = self.config("batch_goal", intrange=[1, 1000000], defaultvalue=10000)
        self.query_comment = ""
        enable_query_comment = self.config("enable_query_comment", defaultvalue=False, choices=[True, False])
        if enable_query_comment:
            query_comment = self.config("query_comment", defaultvalue="")[:1024]
            self.query_comment = "`comment(\"%s/%s%s\")`" % (globals.__NAME__, globals.__VERSION__, " %s" % query_comment if query_comment else "")
        self.grand_total_sent = 0
        self.thread_count = 0
        self.thread_results = []
        self.error_statuses = self.config("error_statuses", defaultvalue="FATAL,ERROR", converttolist=True)
        self.warning_statuses = self.config("warning_statuses", defaultvalue="WARN,WARNING", converttolist=True)
        self.limit_reached = False
        self.queue_complete = False
        self.results_returned = True
        self.on_no_results = self.config("on_no_results", defaultvalue="exit", choices=["exit", "halt", "pause", "wait", "sleep"])
        if self.on_no_results in ["pause", "sleep"]:
            self.on_no_results = "wait"
        elif self.on_no_results == "halt":
            self.on_no_results = "exit"
        self.on_no_results_wait_interval = self.config("on_no_results_wait_interval", required=True, intrange=[1, 604800]) if self.on_no_results == "wait" else None

    def _createwindow(self):
        self.window = Window(endpointinstance=self)
        self.window.open()

    def _energizetransporters(self, thread_context):
        self.queue_complete = False
        self.results_returned = True
        for x in range(1, self.thread_count + 1):
            self.thread_results.append(thread_context.submit(self.engine.transport.send, (self.engine.service.que, x,)))

    def connect(self):
        """
        Connects to the Splunk endpoint specified in the extract.yaml config file
        :return:
        """
        if not self.window:
            self._createwindow()
        self.engine.service.instrumenter.register(groupname=self.integration)
        s = self.config("scheme", required=True)
        h = self.config("host", required=True)
        p = self.config("port", required=True)
        max_connection_attempts = self.config("max_connection_attempts", defaultvalue=0)
        connection_retry_interval = self.config("connection_retry_interval", defaultvalue=60)
        self.target = "%s://%s:%s" % (s, h, p)
        while self.connection_attempts <= max_connection_attempts:
            info("Connecting to %s at %s" % (self.integration_capitalized, self.target))
            try:
                self.source = client.connect(
                        scheme=s,
                        host=h,
                        port=p,
                        username=self.config("username", required=True),
                        password=self.config("password", required=True)
                )
            except ConnectionRefusedError as e:
                error("connection refused by %s at %s: %s" % (self.integration_capitalized, self.target, str(e)))
                pass
            except Exception as e:
                exception("during connection to %s at %s: %s" % (self.integration_capitalized, self.target, str(e)))
            if self.connected:
                info("Connected")
                break
            self.connection_attempts += 1
            info("Connection unsuccessful; retrying in %d seconds..." % connection_retry_interval)
            sleep(connection_retry_interval)
        if self.connection_attempts > max_connection_attempts:
            error("Max connection attempts exceeded for %s endpointname %s" % (self.integration_capitalized, self.target))
        self.bucketname = self.config("index", required=True)
        self.bucket = self.source.indexes[self.bucketname]
        return

    @property
    def connected(self):
        isconnected = False
        if isinstance(self.source, client.Service):
            try:
                apps = self.source.apps
                for app in apps:
                    if app:
                        isconnected = True
                        break
            except Exception as e:
                error("testing connection to %s at %s: %s" % (self.integration_capitalized, self.target, str(e)))
        return isconnected

    def _buildquery(self, dynamic_query_filter, lookup, lookup_label):
        sample_percentage = 100 if lookup else self.sample_percentage
        main_subquery = 'index=%s' % self.bucketname
        basic_filters = ""
        if self.static_source_filter:
            basic_filters = "source = %s" % self.static_source_filter
        if self.static_sourcetype_filter:
            if basic_filters:
                basic_filters = "%s sourcetype = %s" % (basic_filters, self.static_sourcetype_filter)
            else:
                basic_filters = "sourcetype = %s" % self.static_sourcetype_filter
        if self.static_host_filter:
            if basic_filters:
                basic_filters = "%s host = %s" % (basic_filters, self.static_host_filter)
            else:
                basic_filters = "host = %s" % self.static_host_filter
        if basic_filters:
            main_subquery = "%s %s " % (main_subquery, basic_filters)
        if self.static_query_filter:
            main_subquery = "%s %s " % (main_subquery, self.static_query_filter)
        boxed_subquery = main_subquery
        if self.window:
            if self.window.epochstart and self.window.epochend:
                boxed_subquery = '%s _time >= %s _time <= %s ' % (main_subquery, self.window.epochstart, self.window.epochend)
        if lookup:
            query = "%s search %s" % (self.query_comment, boxed_subquery)
        else:
            query = "%s search %s | eval spdqx=[ tstats count where (%s _time >= %s _time <= %s) by _time span=1s | streamstats sum(count) AS totalcount global=t " \
                    "| eval offset=totalcount - %d | where offset > 0 | sort offset asc | head 1 | rename _time AS xq " \
                    "| appendpipe [ stats count as xq | where xq==0 ] | return $xq ] " \
                    "| where(_time <= spdqx) " \
                    % (self.query_comment,
                       boxed_subquery,
                       main_subquery,
                       self.window.epochstart,
                       self.window.epochend,
                       self.batch_goal)
        if dynamic_query_filter:
            query = "%s | %s" % (query, dynamic_query_filter)
        if sample_percentage < 100:
            query = "%s | where(_serial %% ceiling(100 / %d) = 0)" % (query, sample_percentage)
        return query.strip()

    def _getthreadcontext(self, multithread):
        if multithread:
            self.thread_count = self.engine.transport.threads if self.engine.transport.threads > 0 else numcpus()
            self.thread_count = 1 if 0 < self.engine.options.limit < 10000 else self.thread_count
            info("Number of transport threads set to %d" % self.thread_count)
            thread_context = futures.ThreadPoolExecutor(self.thread_count)
        else:
            thread_context = nullcontext()
        return thread_context

    def _executequery(self, dynamic_query_filter="where(1==2)", lookup=None, lookup_label=None):
        query = None
        if not self.connected:
            self.connect()
        instrumentation = self.engine.service.instrumenter.get(tag=self.integration_capitalized)
        status = "OK"
        rtn = "OK"
        rlist = []
        query = self._buildquery(dynamic_query_filter, lookup, lookup_label)
        kwargs_export = {
            "count": 0,
            "maxEvents": globals.MAX_SPLUNK_BATCH_SIZE,
            "preview": False,
            "search_mode": "normal",
        }
        if lookup and lookup_label:
            info("Metadata lookup (%s)" % lookup_label)
        else:
            info("Querying starting from base window date %s (%s)" % (self.window.epochstart, self.window.start))
        batch_end_et = None
        datareturned = False
        msg = None
        multithread = self.forward and not lookup
        thread_context = self._getthreadcontext(multithread)
        if not globals.KILLSIG:
            with thread_context:
                with instrumentation:
                    try:
                        for r in results.ResultsReader(io.BufferedReader(SpardaqusStreamBuffer(self.source.jobs.export(query, **kwargs_export)))):
                            if globals.KILLSIG:
                                break
                            if isinstance(r, dict):
                                if 'spdqempty' in r.keys():
                                    msg = "spdqempty"
                                    break
                                elif lookup:
                                    datareturned = True
                                    for k in r:
                                        rlist.append(r[k])
                                        break
                                    break
                                else:
                                    datareturned = True
                                    if self.forward:
                                        if "dump" in self.engine.worker:
                                            print(json.dumps(r))
                                        else:
                                            if self.grand_total_sent == 0:
                                                self._energizetransporters(thread_context)
                                            self.engine.service.que.put(r)
                                    self.grand_total_sent += 1
                                    instrumentation.increment()
                                    if instrumentation.counter % 10000 == 0:
                                        info("Queued %d to send" % instrumentation.counter) if self.forward \
                                            else info("DISCARDED %d events (forward = False)" % instrumentation.counter)
                                        if self.engine.options.profile:
                                            profile.memory()
                                    if not batch_end_et:
                                        batch_end_et = r["spdqx"]
                                    if self.engine.options.limit > 0:
                                        if self.engine.options.limit == self.grand_total_sent:
                                            self.limit_reached = True
                                            info("--limit value (%d) reached" % self.engine.options.limit)
                                            break
                            else:
                                status = r.type
                                if status in self.error_statuses:
                                    rtn = "While querying %s: %s" % (self.integration_capitalized, r.message)
                                    if query:
                                        rtn = "%s; query attempted = `%s`" % (rtn, query)
                                elif status in self.warning_statuses:
                                    info("While querying %s: %s" % (self.integration_capitalized, r.message))
                                    status = "OK"
                    except Exception as e:
                        exception("querying %s at %s: %s" % (self.integration_capitalized, self.target, str(e)))
                    self.queue_complete = True
                    if (lookup or instrumentation.counter > 0) and status not in ['FATAL', 'ERROR']:
                        if lookup:
                            if rlist:
                                rtn = rlist[0]
                                info("Metadata answer (%s) = %s" % (lookup_label, rtn))
                            else:
                                error("%s returned message: %s; query attempted = `%s`" % (self.integration_capitalized, r.message, query))
                        else:
                            if self.forward:
                                info("Queued a total of %d events" % instrumentation.counter)
                                totsent = 0
                            else:
                                info("Discarded a total of %d events (extract.yaml::forward = False)" % instrumentation.counter)
        if not datareturned:
            if msg == "spdqempty":
                info("No more %s data in current window. " % self.integration_capitalized)
                self.results_returned = False
            elif msg:
                error("no results returned from %s; response was: \"%s\"; query attempted = `%s`" % (self.integration_capitalized, msg, query))
            else:
                error("no response from %s; query attempted = `%s`" % (self.integration_capitalized, query))
        if lookup:
            return rtn
        else:
            instrumentation_collection = None
            if self.forward:
                for future in futures.as_completed(self.thread_results):
                    try:
                        threadresult = future.result()
                    except futures.CancelledError as ce:
                        info("Transport thread cancelled")
                        pass
                instrumentation_collection = self.engine.service.instrumenter.collectall()
            if batch_end_et:
                self.window.advance(batch_end_et)
            if self.engine.options.profile:
                profile.memory()
            self.engine.service.instrumenter.printall(instrumentation_collection)
            return instrumentation_collection

    def dump(self):
        """
        self.engine.worker is checked in the extract::_executequery call to determine that it's a dump we want
        """
        return self.extract()

    def extract(self):
        """
        Called by the engine's Run function. Reads data from the Splunk endpoint and queues it up locally to send
        via the transport mechanism to downstream consumers.
        :return:
        """
        query_fragment = self.engine.service.message_schema.query_fragment
        query_fragment = 'eval %s | appendpipe [ stats count as spdqempty | where spdqempty==0 ] | table spdq* ' % query_fragment
        query_fragment = query_fragment % (self.keysize, self.bucketname, self.timestamp_field_name, self.timestamp_field_format)
        return self._executequery(query_fragment)

    @property
    def earliest(self):
        """Returns the earliest possible date for extractable data in Splunk, in UNIX epoch time format."""
        query_fragment = 'stats earliest(_time) as spdqval | table spdqval'
        return self._executequery(query_fragment, lookup="spdqval", lookup_label="timestamp of earliest relevant event")

    @property
    def latest(self):
        """Returns the latest possible date for extractable data in Splunk, in UNIX epoch time format."""
        query_fragment = 'stats latest(_time) as spdqval | table spdqval'
        return self._executequery(query_fragment, lookup="spdqval", lookup_label="timestamp of latest relevant event")

    def close(self, **kwargs):
        pass
