import logging
import logging.config
from logging.handlers import SysLogHandler
import os
from queue import Queue
import sys
from service import Service as SystemService, find_syslog
from time import sleep

from spardaqus import globals
from spardaqus.core.instrumentation import InstrumentationManager
import spardaqus.core.profiling as profile
from spardaqus.core.utils import \
    debugging, \
    error, \
    exception,\
    getconfig, \
    info, \
    mergedicts
from spardaqus.core.state import Redis as StateManager
from spardaqus.core.types import SpardaqusMessage


def analyze(serviceinstance):
    """
    Analysis wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.endpoint.analyze()


def extract(serviceinstance):
    """
    Extract wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.endpoint.extract()


def dumpanalyze(serviceinstance):
    """
    Dump analyze wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.endpoint.dump()


def dumpextract(serviceinstance):
    """
    Dump extract wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.endpoint.dump()


def dumptransport(serviceinstance):
    """
    Extract wrapper function
    :param serviceinstance:
    :return:
    """
    return serviceinstance.engine.transport.dump()


class SpardaqusService(SystemService):
    """
    Creates an instance of the appropriate service locally (extractor, analyzer, etc)
    based on the passed commandline startup values sent to the calling instance of SpardaqusEngine.
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
        self.message_schema = None

    def is_running(self):
        if debugging:
            return False
        else:
            return super().is_running()

    @property
    def runnable(self):
        try:
            endpoint_has_results = self.engine.endpoint.results.empty
        except AttributeError:
            endpoint_has_results = self.engine.endpoint.results
            pass
        return not globals.KILLSIG \
            and self.instrumenter.status == "OK" \
            and (
                   (self.engine.endpoint.connected or self.engine.options.command == "dump")
                   and not (self.engine.endpoint.on_no_results == "exit" and not endpoint_has_results)
                   and not self.engine.endpoint.limit_reached
               ) \
            and not self.engine.options.command == 'stop'

    def run(self):
        """
        Main service execution loop. Connects to the endpoint, analyzer, and transport mechanisms specified in the local Spardaqus
        config, then calls the extractor or analyzer master functions as appropriate.
        """
        try:
            info("Starting %s" % self.name)
            # set up defaults when running as a service
            from argparse import Namespace
            options = Namespace(
                    abort=False,
                    debug=False,
                    enabled=True,
                    init=False,
                    limit=0,
                    logfile="",
                    loglevel="INFO",
                    profile=False,
                    quiet=False
            )
            # update default service settings with the entries in the spardaqus.yaml
            settings = getconfig("spardaqus", "config")
            for setting in settings:
                setting = setting.strip().lower()
                if setting in options.__dict__:
                    try:
                        options.__dict__[setting] = settings[setting]
                    except Exception as e:
                        error('Error loading setting "%s" from %s: %s' % (setting,  os.path.join(globals.ROOT_DIR, 'config/%s.yaml'), str(e)))
            self.engine.options.__dict__ = mergedicts(options.__dict__, self.engine.options.__dict__, overwrite=True)
            #
            # main work loop: everything Spardaqus does happens here
            #
            pstats_file = None
            if self.engine.options.profile:
                pstats_file = profile.start()
                info("** --profile specified; starting performance profiling")
                info("** pstats data will be written to /var/log/yappi.%s.%s" % (globals.__NAME__, globals.__VERSION__))
            self.engine.transport.connect()
            if self.engine.transport.connected:
                if not self.engine.worker == "dumptransport":
                    self.engine.endpoint.connect()
                if self.service_breaker_test:
                    info("Service breaker test signalled; shutting down")
                elif self.engine.endpoint.connected or self.engine.worker == "dumptransport":
                    self.message_schema = SpardaqusMessage(self.engine.endpoint.integration)
                    worker = getattr(sys.modules[__name__], self.engine.worker)
                    while self.runnable:
                        results = worker(self)
                        if self.engine.options.profile:
                            profile.memory()
                        if "dump" in self.engine.options.command:
                            break
                        if not self.engine.endpoint.results_returned and self.engine.endpoint.on_no_results == "wait":
                            info("Waiting %d seconds before trying again..." % self.engine.endpoint.on_no_results_wait_interval)
                            sleep(int(self.engine.endpoint.on_no_results_wait_interval))
                    self.engine.transport.close()
                    self.engine.endpoint.close()
            if self.engine.options.profile:
                profile.end(pstats_file)
            #
            #
            #
        except Exception as e:
            exception("Exception in %s: %s" % (self.name, str(e)))
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
