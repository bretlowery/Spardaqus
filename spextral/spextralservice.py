import logging
import logging.config
from logging.handlers import SysLogHandler
import os
from queue import Queue
import sys
from service import Service as SystemService, find_syslog
import yappi

from spextral import globals
from spextral.core.instrumentation import InstrumentationManager, InstrumentationCollection
from spextral.core.utils import \
    debugging, \
    error, \
    getconfig, \
    info, \
    nowstr, \
    mergedicts, \
    profile_memory
from spextral.core.state import Redis as StateManager


def extract(serviceinstance):
    """
    Extract wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.endpoint.extract()


def analyze(serviceinstance):
    """
    Analysis wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.endpoint.analyze()


class SpextralService(SystemService):
    """
    Creates an instance of the appropriate service locally (extractor, analyzer, etc)
    based on the passed commandline startup values sent to the calling instance of SpextralEngine.
    """
    def __init__(self, *args, **kwargs):
        self.engine = kwargs.pop("engine")
        super().__init__(*args, **kwargs)
        if self.engine.options.command != "interactive":
            self.logger.addHandler(SysLogHandler(address=find_syslog(),
                                                 facility=SysLogHandler.LOG_DAEMON))
            self.logger.setLevel(logging.INFO)
            globals.LOGGER = self.logger
        self.service_breaker_test = False
        self.que = Queue()
        self.state = StateManager()
        self.instrumenter = InstrumentationManager(engine=self.engine)

    def is_running(self):
        if debugging:
            return False
        else:
            return super().is_running()

    @property
    def do(self):
        return not globals.KILLSIG \
            and self.instrumenter.status == "OK" \
            and self.engine.endpoint.connected \
            and not self.engine.endpoint.limit_reached \
            and not (self.engine.endpoint.no_results_returned and self.engine.endpoint.on_no_results == "exit") \
            and not self.engine.options.command == 'stop'

    def run(self):
        """
        Main service execution loop. Connects to the endpoint, analyzer, and transport mechanisms specified in the local Spextral
        config, then calls the extractor or analyzer master functions as appropriate.
        """
        try:
            info("Starting %s" % self.name)
            # set up defaults when running as a service
            from argparse import Namespace
            options = Namespace(
                    abort=False,
                    enabled=True,
                    haltonerror=True,
                    init=False,
                    limit=0,
                    logerrors=True,
                    loginfo=False,
                    profile=False,
                    test=False,
                    quiet=True
            )
            # update default service settings with the entries in the spextral.yaml
            settings = getconfig("spextral", "config")
            for setting in settings:
                setting = setting.strip().lower()
                if setting in options.__dict__:
                    try:
                        options.__dict__[setting] = settings[setting]
                    except Exception as e:
                        error('Error loading setting "%s" from %s: %s' % (setting,  os.path.join(globals.ROOT_DIR, 'config/%s.yaml'), str(e)))
            self.engine.options.__dict__ = mergedicts(options.__dict__, self.engine.options.__dict__, overwrite=True)
            #
            # main work loop: everything Spextral does happens here
            #
            pstats_file = None
            if self.engine.options.profile:
                pstats_file = '/var/log/yappi.%s.%s.%s' % (globals.__NAME__, globals.__VERSION__, nowstr())
                yappi.clear_stats()
                yappi.start()
                info("** --profile specified; starting performance profiling")
                info("** pstats data will be written to /var/log/yappi.%s.%s" % (globals.__NAME__, globals.__VERSION__))
            self.engine.transport.connect()
            self.engine.endpoint.connect()
            if self.service_breaker_test:
                info("Service breaker test signalled; shutting down")
            elif self.engine.endpoint.connected:
                worker = getattr(sys.modules[__name__], self.engine.options.operation)
                while self.do:
                    results = worker(self)
                    if self.engine.options.profile:
                        profile_memory()
                self.engine.transport.close()
                self.engine.endpoint.close()
            if self.engine.options.profile:
                yappi.stop()
                func_stats = yappi.get_func_stats()
                if func_stats:
                    _rows = []
                    for _stat in func_stats._as_dict:
                        if '/Spextral/' in _stat.full_name and '/venv/' not in _stat.full_name:
                            _gizmo = _stat.full_name.split("/")[-1]
                            _rows.append([ _gizmo.split(" ")[1], _gizmo.split(" ")[0], _stat.ncall, _stat.tavg, _stat.ttot, _stat.tsub])
                    info("*")
                    info("* TOP 50 CALLS BY TOT TIME")
                    info("*")
                    _hdr = ["NAME", "LOCATION", "CALLS", "AvgTIME", "TotTIME", "TotTIMELessSubcalls"]
                    info("{: <40} {: <32} {: >12} {: >24} {: >24} {: >24}".format(*_hdr))
                    _rows.sort(key=lambda x: x[4], reverse=True)
                    i = 0
                    for _row in _rows:
                        info("{: <40} {: <32} {: >12} {: >24} {: >24} {: >24}".format(*_row))
                        i += 1
                        if i == 50:
                            break
                    info("*")
                    info("* TOP 50 CALLS BY NUMBER OF CALLS")
                    info("*")
                    info("{: <40} {: <32} {: >12} {: >24} {: >24} {: >24}".format(*_hdr))
                    _rows.sort(key=lambda x: x[2], reverse=True)
                    i = 0
                    for _row in _rows:
                        info("{: <40} {: <32} {: >12} {: >24} {: >24} {: >24}".format(*_row))
                        i += 1
                        if i == 50:
                            break
                func_stats.save(pstats_file, type='pstat')
                yappi.clear_stats()
            #
            #
            #
        except Exception as e:
            error("Error in %s: %s" % (self.name, str(e)))
        finally:
            self.que.task_done()
            self.stop()

    def stop(self, block=False):
        globals.KILLSIG = True
        try:
            super().stop(block)
        except:
            pass
        sys.exit(0)

    @staticmethod
    def _print_stats(stats, out, limit=None):
        if stats.empty():
            return
        sizes = [36, 5, 8, 8, 8]
        columns = dict(zip(range(len(yappi.COLUMNS_FUNCSTATS)), zip(yappi.COLUMNS_FUNCSTATS, sizes)))
        show_stats = stats
        if limit:
            show_stats = stats[:limit]
        info("** Profiler Results:\r\n%s" % show_stats)
