import argparse
import importlib
import os
import sys
from time import sleep

from loguru import logger

from spextral import globals
from spextral.spextralservice import SpextralService
from spextral.core.metaclasses import SpextralNullEndpoint
import spextral.core.profiling as profile
from spextral.core.utils import \
    debugging, \
    getconfig, \
    info, \
    error, \
    istruthy, \
    printmsg, \
    xlatearg


class SpextralEngine:
    """
    Main starting point and controlling class for Spextral. Creates an instance of the appropriate service locally (extractor, analyzer, etc)
    based on the passed commandline startup values. Handles start, stop logic.
    """

    def config(self,
               settingname,
               required=True,
               defaultvalue=0,
               choices=None,
               intrange=None,
               quotestrings=False,
               noneisnone=True):
        return getconfig(self.options.operation,
                         self.options.operation,
                         settingname=settingname,
                         required=required,
                         defaultvalue=defaultvalue,
                         choices=choices,
                         intrange=intrange,
                         quotestrings=quotestrings,
                         noneisnone=noneisnone)

    def __init__(self):
        argz = argparse.ArgumentParser(usage="spextralcmd "
                                             "[options] "
                                             "["
                                                "[start | stop | interactive] [extract | analyze]"
                                                " | "
                                                "dump [extract | analyze | transport [fromextract | fromanalyze]"
                                             "]")
        argz.add_argument("command",
                          nargs='?',
                          default=None)
        argz.add_argument("operation",
                          nargs='?',
                          default=None)
        argz.add_argument("option",
                          nargs='?',
                          default="none")
        argz.add_argument("--init",
                          action="store_true",
                          dest="init"
                          )
        argz.add_argument("--limit",
                          action="store",
                          type=int,
                          dest="limit",
                          default=0
                          )
        argz.add_argument("--profile",
                          action="store_true",
                          dest="profile",
                          default=False
                          )
        argz.add_argument("--logfile",
                          action="store",
                          dest="logfile",
                          default=""
                          )
        argz.add_argument("--quiet",
                          action="store_true",
                          dest="quiet",
                          default=False
                          )
        argz.add_argument("--debug",
                          action="store_true",
                          dest="debug",
                          default=False
                          )
        self.options = argz.parse_args()
        try:
            self.options.command = xlatearg(self.options.command)
            self.options.operation = xlatearg(self.options.operation)
            self.options.option = xlatearg(self.options.option)
            self.options.logfile = self.options.logfile.strip()
        except IndexError:
            self.options.command = None
            self.name = None
            pass
        self.endpoint = None
        self.transport = None
        self.encoding = None
        self.service = None
        self.worker = None
        self.scrub_inputs()
        self.init_logging()
        info("Halting Spextral") if self.options.command == "stop" else info("Initializing Spextral")
        if self.options.profile:
            profile.memory()
        if debugging():
            info("Debugging mode detected")
        self.create_service_topology()
        self.create_service_instance()
        return

    def init_logging(self):
        logger.remove()
        log_enabled = getconfig("spextral", "config", "log.enabled", required=True)
        if log_enabled and not self.options.quiet:
            log_level = getconfig("spextral", "config", "log.level",
                                  required=True, defaultvalue="error", choices=["info", "error"]).upper()
            log_format = getconfig("spextral", "config", "log.format",
                                   required=False, defaultvalue="{time} {level} {message}")
            log_console = getconfig("spextral", "config", "log.console", required=True)
            if log_console:
                logger.add(sys.stderr, format=log_format, level="ERROR")
                if log_level == "INFO":
                    logger.add(sys.stdout, format=log_format, level="INFO")
            log_file = getconfig("spextral", "config", "log.file", required=False, defaultvalue=self.options.logfile)
            if not log_file:
                if not log_console:
                    error("Logging is enabled in the config, but neither console logging nor a log file are specified.")
            else:
                rotation = getconfig("spextral", "config", "log.rotation", required=False, defaultvalue=None)
                compression = getconfig("spextral", "config", "log.compression", required=False, defaultvalue=None)
                logger.add(log_file,
                           format=log_format,
                           level=log_level,
                           rotation=rotation,
                           compression = compression)
        return

    def scrub_inputs(self):
        if not self.options.command or not self.options.operation:
            error("Insufficient arguments provided")
        if self.options.command not in ['start', 'stop', 'interactive', 'dump']:
            error("Invalid command '%s'; must be one of: 'start', 'stop', 'interactive', 'dump'" % self.options.command)
        elif self.options.command in ['start', 'stop', 'interactive']:
            if self.options.operation not in ['extract', 'analyze']:
                error("Invalid operation '%s' for command '%s' provided; must be one of: 'extract', 'analyze'" % (self.options.operation, self.options.command))
        elif self.options.command == "dump":
            if self.options.operation not in ['extract', 'analyze', 'transport']:
                error("Invalid dump operation '%s' provided; must be one of: 'extract', 'analyze', 'transport'" % self.options.operation)
        if self.options.option:
            if self.options.command in ['dump']:
                if self.options.operation in ['transport']:
                    if self.options.option not in ['fromextract', 'fromanalyze']:
                        if self.options.option == "none":
                            error("Either 'fromextract' or 'fromanalyze' must be specified when using dump transport")
                        else:
                            error("Invalid dump transport option '%s' provided; must be either 'fromextract' or 'fromanalyze'" % self.options.option)
                elif self.options.option not in ['none']:
                    error("Invalid or incomplete dump option '%s' provided" % self.options.option)
            elif self.options.option not in ['none']:
                error("Invalid or incomplete option '%s' provided" % self.options.option)
        elif self.options.command in ['dump'] and self.options.operation in ['transport']:
            error("Either 'fromextract' or 'fromanalyze' must be specified when using dump transport")
        if self.options.command in ["dump"]:
            if self.options.operation in ['transport']:
                self.worker = "dumptransport"
                if self.options.option == 'fromanalyze':
                    self.options.operation = "analyze"
                elif self.options.option == 'fromextract':
                    self.options.operation = "extract"
            else:
                self.worker = "dump%s" % self.options.operation
        else:
            self.worker = self.options.operation

    def create_service_topology(self):
        tn = self.config("transport")
        epn = self.config("endpoint")
        if self.worker == "dumptransport":
            self.endpoint = SpextralNullEndpoint(self, epn)
            self.endpoint.name = "SpextralDump%s/%s" % (tn.capitalize(), globals.__VERSION__)
        else:
            epclass = getattr(importlib.import_module("spextral.%s.%s" % (self.options.operation, epn)), epn.capitalize())
            self.endpoint = epclass(self)
            if self.options.command in ["dump"]:
                self.endpoint.name = "SpextralDump%s/%s" % (epn.capitalize(), globals.__VERSION__)
            else:
                self.endpoint.name = "Spextral%s%s/%s" % (self.endpoint.integration.capitalize(), self.options.operation.capitalize(), globals.__VERSION__)
        transportclass = getattr(importlib.import_module("spextral.transport.%s" % tn), tn.capitalize())
        self.transport = transportclass(self)
        self.transport.name = self.endpoint.name

    def create_service_instance(self):
        self.encoding = getconfig("spextral", "config", "encoding", defaultvalue="utf-8")
        self.service = SpextralService(engine=self, pid_dir="/tmp", name=self.endpoint.name)

    def exit(self):
        if self.options.profile:
            x = 1024.0 if sys.platform == "darwin" else 1.0 if sys.platform == "linux" else 1.0
            info("\n\r%s MB maximum (peak) memory used" % str(round(globals.MAX_RSS_MEMORY_USED / 1024.0 / x, 3)))


def main():
    engine = SpextralEngine()
    try:
        if engine.options.command in ["dump", "interactive"]:
            engine.service.run()
        elif engine.options.command == "start":
            engine.service.run() if debugging else engine.service.start()
            sleep(2)
            if engine.service.is_running():
                printmsg("%s is running." % engine.service.name)
            else:
                printmsg("WARNING, %s is NOT running." % engine.service.name)
        elif engine.options.command == "stop":
            if not debugging:
                engine.service.stop()
            sleep(2)
            if engine.service.is_running():
                printmsg("WARNING, %s is STILL running." % engine.service.name)
            else:
                printmsg("%s is not running." % engine.service.name)
        elif engine.options.command == "test":
            engine.service.service_breaker_test = True
            engine.service.run() if debugging else engine.service.start()
            printmsg("Waiting for %s to start..." % engine.service.name)
            sleep(3)
            if engine.service.is_running():
                printmsg("%s is running." % engine.service.name)
            else:
                printmsg("WARNING, %s is NOT running." % engine.service.name)
            printmsg("Waiting for automatic breaker throw...")
            sleep(30)
            if engine.service.is_running():
                printmsg("WARNING, %s is STILL running." % engine.service.name)
            else:
                printmsg("%s stopped successfully." % engine.service.name)
                printmsg("%s service configuration looks GOOD." % engine.service.name)
        elif engine.options.command == "status":
            if engine.service.is_running():
                printmsg("%s is running." % engine.service.name)
            else:
                printmsg("%s is not running." % engine.service.name)
        else:
            error("Invalid command")
    except SystemError:
        halt(engine, 1)
    finally:
        halt(engine, 0)
    return


def halt(engine, rtncode=0):
    globals.KILLSIG = True
    if engine:
        if rtncode == 0:
            engine.exit()
        if engine.service and not debugging:
            try:
                engine.service.stop()
            except:
                pass
    info("Finished (code %d)" % rtncode)
    os._exit(rtncode)


if __name__ == "__main__":
    main()
