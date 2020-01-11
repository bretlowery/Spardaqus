import logging
import logging.config
from logging.handlers import SysLogHandler
import os
from queue import Queue
import sys
from service import Service as SystemService, find_syslog
# import yappi

from spextral import globals
from spextral.core.instrumentation import InstrumentationManager, InstrumentationCollection
from spextral.core.utils import \
    debugging, \
    error, \
    getconfig, \
    info, \
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
                    singleprocess=False,
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
            self.engine.transport.connect()
            self.engine.endpoint.connect()
            if self.service_breaker_test:
                info("Service breaker test signalled; shutting down")
                self.stop()
            if self.engine.endpoint.connected:
                worker = getattr(sys.modules[__name__], self.engine.options.operation)
                results = worker(self)
                while not globals.KILLSIG \
                        and self.instrumenter.status == "OK" \
                        and self.engine.endpoint.connected \
                        and not self.engine.endpoint.limit_reached \
                        and not self.engine.options.command == 'stop':
                    results = worker(self)
                self.engine.transport.close()
                self.engine.endpoint.close()
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

