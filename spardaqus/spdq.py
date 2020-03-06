import argparse
import importlib
import os
import sys
from time import sleep

from loguru import logger

from spardaqus import globals
from spardaqus.spdqservice import SpardaqusService
from spardaqus.core.metaclasses import SpardaqusNullEndpoint
import spardaqus.core.profiling as profile
from spardaqus.core.utils import \
    debugging, \
    getconfig, \
    info, \
    error, \
    istruthy, \
    printmsg, \
    writable_path, \
    xlatearg


class SpardaqusEngine:
    """
    Main starting point and controlling class for Spardaqus. Creates an instance of the appropriate service locally (extractor, analyzer, etc)
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
        self.argz = argparse.ArgumentParser(usage="spardaquscmd "
                                             "[options] "
                                             "["
                                                "[start | stop | interactive] [extract | analyze]"
                                                " | "
                                                "dump [extract | analyze | transport ] [stdout | 'outputfile']"
                                             "]")
        self.argz.add_argument("command",
                          nargs='?',
                          default=None)
        self.argz.add_argument("operation",
                          nargs='?',
                          default=None)
        self.argz.add_argument("output",
                          nargs='?',
                          default="stdout")
        self.argz.add_argument("--init",
                          action="store_true",
                          dest="init")
        self.argz.add_argument("--limit",
                          action="store",
                          type=int,
                          dest="limit",
                          default=0)
        self.argz.add_argument("--profile",
                          action="store_true",
                          dest="profile",
                          default=False)
        self.argz.add_argument("--logfile",
                          action="store",
                          dest="logfile",
                          default="")
        self.argz.add_argument("--quiet",
                          action="store_true",
                          dest="quiet",
                          default=False)
        self.argz.add_argument("--debug",
                          action="store_true",
                          dest="debug",
                          default=False)
        self.argz.add_argument("--dumpdelimiter",
                          action="store",
                          dest="delimiter",
                          default="\t")
        self.argz.add_argument("--dumpvalueifmissing",
                          action="store",
                          dest="ifmissing",
                          default="?")
        self.argz.add_argument("--dumpfloatformat",
                          action="store",
                          dest="floatformat",
                          default=None)
        self.argz.add_argument("--dumpindex",
                          action="store_true",
                          dest="index",
                          default=False)
        self.argz.add_argument("--dumpcompression",
                          action="store",
                          dest="compression",
                          default="infer")
        self.argz.add_argument("--dumpquotechar",
                          action="store",
                          dest="quotechar",
                          default="\"")
        self.argz.add_argument("--dumpterminator",
                          action="store",
                          dest="line_terminator",
                          default="\n")
        self.options = self.argz.parse_args()
        try:
            self.options.command = xlatearg(self.options.command)
            self.options.operation = xlatearg(self.options.operation)
            self.options.logfile = self.options.logfile.strip()
            self.options.output = self.options.output.strip()
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
        info("Halting Spardaqus") if self.options.command == "stop" else info("Initializing Spardaqus")
        if debugging():
            info("Debugging mode detected")
        self.create_service_topology()
        self.create_service_instance()
        return

    def init_logging(self):
        logger.remove()
        log_enabled = getconfig("spardaqus", "config", "log.enabled", required=True)
        if log_enabled and not self.options.quiet:
            log_level = getconfig("spardaqus", "config", "log.level", required=True, defaultvalue="error", choices=["info", "error"]).upper()
            log_format = getconfig("spardaqus", "config", "log.format", required=False, defaultvalue="{time} {level} {message}").lower()
            log_time_format = getconfig("spardaqus", "config", "log.time.format", required=False, defaultvalue=None, noneisnone=True)
            if log_time_format:
                log_format = log_format.replace("{time}", "{time:%s}" % log_time_format)
            log_console = getconfig("spardaqus", "config", "log.console", required=True)
            if log_console:
                logger.add(sys.stderr, format=log_format, level="ERROR")
                if log_level == "INFO":
                    logger.add(sys.stdout, format=log_format, level="INFO")
            log_file = getconfig("spardaqus", "config", "log.file", required=False, defaultvalue=self.options.logfile)
            if not log_file:
                if not log_console:
                    logger.add(sys.stderr, format=log_format, level="ERROR")
                    error("Logging is enabled in the config, but neither console logging nor a log file are specified.")
            else:
                rotation = getconfig("spardaqus", "config", "log.rotation", required=False, defaultvalue=None)
                compression = getconfig("spardaqus", "config", "log.compression", required=False, defaultvalue=None)
                logger.add(log_file,
                           format=log_format,
                           level=log_level,
                           rotation=rotation,
                           compression = compression)
        return

    def scrub_inputs(self):
        if not self.options.command or not self.options.operation:
            error("Insufficient arguments provided", call=self.argz.print_help())
        if self.options.command not in ['start', 'stop', 'interactive', 'dump']:
            error("Invalid command '%s'; must be one of: 'start', 'stop', 'interactive', 'dump'" % self.options.command, call=self.argz.print_help())
        elif self.options.command in ['start', 'stop', 'interactive']:
            if self.options.operation not in ['extract', 'analyze']:
                error("Invalid operation '%s' for command '%s' provided; must be one of: 'extract', 'analyze'" % (self.options.operation, self.options.command), call=self.argz.print_help())
        elif self.options.command == "dump":
            if self.options.operation not in ['extract', 'analyze', 'transport']:
                error("Invalid dump operation '%s' provided; must be one of: 'extract', 'analyze', 'transport'" % self.options.operation, call=self.argz.print_help())
        if self.options.command in ["dump"]:
            if self.options.operation in ['transport']:
                self.worker = "dumptransport"
                self.options.operation = "analyze"
            else:
                self.worker = "dump%s" % self.options.operation
            if self.options.output != "stdout":
                if not writable_path(self.options.output):
                    error("'%s' is not a valid dump output file path and/or name." % self.options.output)
        elif self.options.output != "stdout":
            error("Invalid commandline options specified", call=self.argz.print_help())
        else:
            self.worker = self.options.operation

    def create_service_topology(self):
        tn = self.config("transport")
        epn = self.config("endpoint")
        if self.worker == "dumptransport":
            self.endpoint = SpardaqusNullEndpoint(self, epn)
            svcname = "SpardaqusDump%s/%s" % (tn.capitalize(), globals.__VERSION__)
        else:
            epclass = getattr(importlib.import_module("spardaqus.%s.%s" % (self.options.operation, epn)), epn.capitalize())
            self.endpoint = epclass(self)
            if self.options.command in ["dump"]:
                svcname = "SpardaqusDump%s/%s" % (epn.capitalize(), globals.__VERSION__)
            else:
                svcname = "Spardaqus%s%s/%s" % (self.endpoint.integration_capitalized, self.options.operation.capitalize(), globals.__VERSION__)
        self.endpoint.name = svcname.replace(" ", "")
        transportclass = getattr(importlib.import_module("spardaqus.transport.%s" % tn), tn.capitalize())
        self.transport = transportclass(self)
        self.transport.name = self.endpoint.name

    def create_service_instance(self):
        self.encoding = getconfig("spardaqus", "config", "encoding", defaultvalue="utf-8")
        self.service = SpardaqusService(engine=self, pid_dir="/tmp", name=self.endpoint.name)

    def exit(self):
        if self.options.profile:
            x = 1024.0 if sys.platform == "darwin" else 1.0 if sys.platform == "linux" else 1.0
            info("\n\r%s MB maximum (peak) memory used" % str(round(globals.MAX_RSS_MEMORY_USED / 1024.0 / x, 3)))


def main():
    engine = SpardaqusEngine()
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
            error("Invalid command '%s'" % engine.options.command)
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
