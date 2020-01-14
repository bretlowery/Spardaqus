import argparse
import importlib
import os
import sys
from time import sleep

from spextral import globals
from spextral.spextralservice import SpextralService
from spextral.core.utils import \
    debugging, \
    getconfig, \
    error, \
    info, \
    printmsg, \
    profile_memory, \
    wipe, \
    xlatearg


class SpextralEngine:
    """
    Main starting point and controlling class for Spextral. Creates an instance of the appropriate service locally (extractor, analyzer, etc)
    based on the passed commandline startup values. Handles start, stop logic.
    """
    def config(self, setting, required=True, defaultvalue=0, choices=None, intrange=None, quotestrings=False):
        return getconfig(self.options.operation, self.options.operation, setting, required=required, defaultvalue=defaultvalue, choices=choices, intrange=intrange, quotestrings=quotestrings)

    def __init__(self):
        argz = argparse.ArgumentParser(usage="spextralcmd [options] [command] [operation]")
        argz.add_argument("command",
                          nargs='?',
                          default=None)
        argz.add_argument("operation",
                          nargs='?',
                          default=None)
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
        argz.add_argument("--singleprocess",
                          action="store_true",
                          dest="singleprocess",
                          default=False
                          )
        self.options = argz.parse_args()
        try:
            self.options.command = xlatearg(self.options.command)
            self.options.operation = xlatearg(self.options.operation)
        except IndexError:
            self.options.command = None
            self.name = None
            pass
        if not self.options.command or not self.options.operation:
            error("No command and/or service name provided")
        if self.options.command not in ['start', 'stop', 'status', 'test', 'interactive']:
            error("Invalid command '%s' provided" % self.options.command)
        if self.options.operation not in ['extract', 'analyze']:
            error("invalid operation name '%s' provided" % self.options.operation)
        info("Initializing Spextral")
        if self.options.profile:
            profile_memory()
        if debugging():
            info("Debugging mode detected")
        self.endpointname = self.config("endpoint")
        endpointclass = getattr(importlib.import_module("spextral.%s.%s" % (self.options.operation, self.endpointname)), self.endpointname.capitalize())
        self.endpoint = endpointclass(self)
        self.endpoint.name = "Spextral%s%s/%s" % (self.endpoint.integration.capitalize(), self.options.operation.capitalize(), globals.__VERSION__)
        self.transportname = self.config("transport")
        transportclass = getattr(importlib.import_module("spextral.transport.%s" % self.transportname), self.transportname.capitalize())
        self.transport = transportclass(self)
        self.transport.name = self.endpoint.name
        self.encoding = getconfig("spextral", "config", "encoding", defaultvalue="utf-8")
        self.service = SpextralService(engine=self, pid_dir="/tmp", name=self.endpoint.name)

    def exit_actions(self):
        return
        #if self.options.profile:
            #x = 1024.0 if sys.platform == "darwin" else 1.0 if sys.platform == "linux" else 1.0
            #info("\n\r%s MB maximum (peak) memory used" % str(round(globals.MAX_RSS_MEMORY_USED / 1024.0 / x, 3)))


def halt(engine, rtncode=0):
    globals.KILLSIG = True
    if engine:
        if rtncode == 0:
            engine.exit_actions()
        if engine.service and not debugging:
            try:
                engine.service.stop()
            except:
                pass
    info("Finished (code %d)" % rtncode)
    os._exit(rtncode)


def main():
    engine = SpextralEngine()
    try:
        if engine.options.command == "interactive":
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
        if globals.LOG4JPROPFILE:
            wipe(globals.LOG4JPROPFILE)
        halt(engine, 1)
    finally:
        if globals.LOG4JPROPFILE:
            wipe(globals.LOG4JPROPFILE)
        halt(engine, 0)
    return


if __name__ == "__main__":
    main()
