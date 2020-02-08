import datetime
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
import yaml

from spextral import globals


def _getsetyaml(fname,
                section,
                settingname=None,
                newvalue=None,
                defaultvalue="",
                required=False,
                noneisnone=False,
                defaultisdefault=False,
                choices=None,
                intrange=None,
                quotestrings=False,
                converttolist=False):
    """ Gets or sets Spextral yaml-resident config values. Do not call directly. Overridden by utils.getconfig and in SpextralIntegration and its subclasses."""

    def _load(_icf, _c=None):
        if _c is not None:
            with open(_icf, "w") as _f:
                yaml.safe_dump(_c, _f, default_flow_style=False)
                globals.SETTINGS_CACHE = {}
        try:
            with open(_icf, "r") as _f:
                if len(globals.SETTINGS_CACHE) == 0:
                    _c = yaml.safe_load(_f)
                elif fname not in globals.SETTINGS_CACHE.keys():
                    _c = mergedicts(globals.SETTINGS_CACHE, yaml.safe_load(_f))
                else:
                    _c = globals.SETTINGS_CACHE
        except Exception as e:
            error("Error reading config file '%s': %s" % (_icf, str(e)))
        return _c

    fname = fname.strip().lower()
    cf = os.path.join(globals.ROOT_DIR, 'config/%s.yaml' % fname)
    c = _load(cf)
    root = "spextral"
    if not settingname:
        val = c[root][section]
    else:
        val = None
        settingname = settingname.strip().lower()
        if section:
            if newvalue is not None:
                c[root][section][settingname] = sstrip(newvalue)
                c = _load(cf, c)
            try:
                val = c[root][section][settingname]
            except KeyError:
                pass
        else:
            if newvalue is not None:
                c[root][settingname] = sstrip(newvalue)
                c = _load(cf, c)
            try:
                val = c[root][settingname]
            except KeyError:
                pass
    if not val and (isinstance(defaultvalue, dict) or isinstance(defaultvalue, list)):
        val = defaultvalue
    if not isinstance(val, dict) and not isinstance(val, list):
        if isinstance(val, str):
            val = defaultvalue if val.lower().strip() == "default" and defaultisdefault else val
            if val is not None:
                val = None if val.lower().strip() == "none" and noneisnone else val
            if val is not None:
                val = boolish(val)
            if quotestrings:
                if val:
                    if "," in val:
                        arry = val.split(",")
                        val = ""
                        for item in arry:
                            item = ('"' if item[:1] != '"' else '') + item.strip() + ('"' if item[-1:] != '"' else '')
                            val = "%s,%s" % (val, item) if val else item
                    else:
                        val = ('"' if val[:1] != '"' else '') + val.strip() + ('"' if val[-1:] != '"' else '')
        if sequalsci(val, "none") or (not isinstance(val, bool) and not val):
            if required and defaultvalue is None:
                error("No value for required config setting '%s.%s' in %s.yaml was found" % (section, settingname, fname))
            val = boolish(defaultvalue)
        if choices and (val or isinstance(val, bool)):
            if val not in choices:
                error("'%s' is not a valid setting choice for '%s' in %s.yaml; valid choices are: %s" % (str(val), settingname, fname, str(choices)))
        if intrange and val:
            if not isinstance(val, int):
                error("an integer range cannot be required for non-integer setting '%s' in %s.yaml" % (settingname, fname))
            if val < intrange[0] or val > intrange[1]:
                error("value for '%s' in %s.yaml (= %d) is outside the allowable range of between %d and %d" % (settingname, fname, val, intrange[0], intrange[1]))
        if converttolist:
            if isinstance(val, str):
                val = val.strip().split(",")
            else:
                error("_getsetyaml.ConvertToList cannot be True for non-string setting '%s' in %s.yaml" % (settingname, fname))
    return val


def boolish(val):
    """ Returns True if the passed val is T, True, Y, Yes, or a boolean True.
    Returns False if the passed val is F, False, N, No. Otherwise, returns val. Ignores case."""
    if isinstance(val, str):
        return {
            't': True,
            'true': True,
            'y': True,
            'yes': True,
            'f': False,
            'false': False,
            'n': False,
            'no': False
        }.get(str(val).strip().lower(), val)
    else:
        return val


def containsdupevalues(structure):
    """Returns True if the passed dict has duplicate items/values, False otherwise. If the passed structure is not a dict, returns None."""
    if isinstance(structure, dict):
        # fast check for dupe keys
        rev_dict = {}
        for key, value in structure.items():
            rev_dict.setdefault(value, set()).add(key)
        dupes = list(filter(lambda x: len(x) > 1, rev_dict.values()))
        if dupes:
            return True
        else:
            return False
    return None


def debugging():
    """ Returns True if running in a IDE's debugging environment/mode, False otherwise."""
    return sys.gettrace() is not None


def getenviron(key, defaultvalue=None):
    k = key.upper()
    try:
        v = os.environ[k]
    except KeyError:
        v = defaultvalue
    return v


def error(msg, onerrorexit=True):
    """ Standard error logging. """
    if msg is None:
        msg = "%s/%s %s: unknown error" % (globals.__NAME__, globals.__VERSION__, datetime.datetime.now().isoformat()[:22])
    else:
        msg = "%s/%s %s: %s" % (globals.__NAME__, globals.__VERSION__, datetime.datetime.now().isoformat()[:22], msg)
    if globals.LOGGER and not debugging():
        globals.LOGGER.error(msg)
    else:
        printmsg(msg)
    if onerrorexit:
        globals.KILLSIG = True
        msg = "%s/%s %s: Exiting due to error" % (globals.__NAME__, globals.__VERSION__, datetime.datetime.now().isoformat()[:22])
        if globals.LOGGER and not debugging():
            globals.LOGGER.error(msg)
        else:
            printmsg(msg)
        sys.exit(1)


def getconfig(
        filename,
        sectionname,
        settingname=None,
        defaultvalue="",
        required=False,
        noneisnone=True,
        defaultisdefault=True,
        choices=None,
        intrange=None,
        quotestrings=False,
        converttolist=False
):
    """ Gets Spextral yaml-resident config values. May be overridden in SpextralIntegration and its subclasses."""
    return _getsetyaml(filename,
                       sectionname,
                       settingname,
                       defaultvalue=defaultvalue,
                       newvalue=None,
                       required=required,
                       noneisnone=noneisnone,
                       defaultisdefault=defaultisdefault,
                       choices=choices,
                       intrange=intrange,
                       quotestrings=quotestrings,
                       converttolist=converttolist)


def info(msg):
    """ Standard info logging. """
    if globals.LOGGER and not debugging():
        msg = " %s: %s" % (datetime.datetime.now().isoformat()[:22], msg)
        globals.LOGGER.info(msg)
    else:
        msg = "%s/%s %s: %s" % (globals.__NAME__, globals.__VERSION__, datetime.datetime.now().isoformat()[:22], msg)
        printmsg(msg)


def isreadable(path):
    """ Returns True if the current user context has permission to read data from the passed path value."""
    if os.path.exists(path):
        try:
            with open(path, "r") as tmp:
                dummy = tmp.read()
                tmp.close()
            return True
        except:
            pass
    return False


def istruthy(val):
    """ Returns True if the passed val is T, True, Y, Yes, 1, or boolean True.
    Returns False if the passed val is boolean False or a string that is not T, True, Y, Yes, or 1, or an integer that is not 1.
    Returns the passed val otherwise. Ignores case."""
    if isinstance(val, bool):
        return val
    elif isinstance(val, str):
        return {
            't': True,
            'true': True,
            'y': True,
            'yes': True,
            '1': True
        }.get(str(val).strip().lower(), False)
    elif isinstance(val, int):
        return {
            1: True
        }.get(val, False)
    else:
        return val


def iswritable(path):
    """ Returns True if the current user context has permission to write data to the passed path value."""
    if os.path.exists(path):
        try:
            tmpfile = os.path.join(path, "%s.tmp" % str(uuid.uuid4()))
            with open(tmpfile, "w+") as tmp:
                tmp.write("")
                tmp.close()
            wipe(tmpfile)
            return True
        except:
            pass
    return False


def mergedicts(a, b, path=None, overwrite=False):
    """ Merges dict a and dict b, returning the merged results into dict a.
    e.g. a={'x':1, 'y':2} and b={'z':3, 'q':4} --> a={'x':1, 'y':2, 'z':3, 'q':4}.
    Merge keys example: e.g. a={'x':1, 'y':2} and b={'y':3, 'q':4} --> a={'x':1, 'y':3, 'q':4} [if overwrite=True], OR an exception [if overwrite=False].
    """
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                mergedicts(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            elif overwrite:
                a[key] = b[key]
            else:
                raise KeyError('Duplicate dictionary keys at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a


def nowstr():
    """Returns the current datetime in YYYYMMDDHHMMSS format as a string."""
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S")


def nowint():
    """Returns the current datetime in YYYYMMDDHHMMSS format as an integer."""
    return int(nowstr())


def numcpus():
    """ Number of available virtual or physical CPUs on this system, i.e.
    user/real as output by time(1) when called with an optimally scaling
    userspace-only program"""

    # cpuset
    # cpuset may restrict the number of *available* processors
    try:
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$',
                      open('/proc/self/status').read())
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                return res
    except IOError:
        pass

    # Python 2.6+
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        pass

    # https://github.com/giampaolo/psutil
    try:
        import psutil
        return psutil.cpu_count()  # psutil.NUM_CPUS on old versions
    except (ImportError, AttributeError):
        pass

    # POSIX
    try:
        res = int(os.sysconf('SC_NPROCESSORS_ONLN'))

        if res > 0:
            return res
    except (AttributeError, ValueError):
        pass

    # Windows
    try:
        res = int(os.environ['NUMBER_OF_PROCESSORS'])

        if res > 0:
            return res
    except (KeyError, ValueError):
        pass

    # jython
    try:
        from java.lang import Runtime
        runtime = Runtime.getRuntime()
        res = runtime.availableProcessors()
        if res > 0:
            return res
    except ImportError:
        pass

    # BSD
    try:
        sysctl = subprocess.Popen(['sysctl', '-n', 'hw.ncpu'],
                                  stdout=subprocess.PIPE)
        scStdout = sysctl.communicate()[0]
        res = int(scStdout)

        if res > 0:
            return res
    except (OSError, ValueError):
        pass

    # Linux
    try:
        res = open('/proc/cpuinfo').read().count('processor\t:')

        if res > 0:
            return res
    except IOError:
        pass

    # Solaris
    try:
        pseudoDevices = os.listdir('/devices/pseudo/')
        res = 0
        for pd in pseudoDevices:
            if re.match(r'^cpuid@[0-9]+$', pd):
                res += 1

        if res > 0:
            return res
    except OSError:
        pass

    # Other UNIXes (heuristic)
    try:
        try:
            dmesg = open('/var/run/dmesg.boot').read()
        except IOError:
            dmesgProcess = subprocess.Popen(['dmesg'], stdout=subprocess.PIPE)
            dmesg = dmesgProcess.communicate()[0]

        res = 0
        while '\ncpu' + str(res) + ':' in dmesg:
            res += 1

        if res > 0:
            return res
    except OSError:
        pass

    return 1


def printmsg(msg):
    """Print text to stdout."""
    print(msg, file=sys.stdout)
    sys.stdout.flush()


def setconfig(filename, sectionname, settingname, newvalue):
    """ Sets Spextral yaml-resident config values at runtime. May be overridden in SpextralIntegration and its subclasses."""
    return _getsetyaml(filename, sectionname, settingname, newvalue)


def setenviron(key, value):
    k = key.upper()
    if value:
        value = None if value.lower().strip() == "none" else value
    if value:
        os.environ[k] = value
    else:
        try:
            del os.environ[k]
        except KeyError:
            pass
        value = None
    return value


def sequalsci(val, compareto):
    """Takes two strings, lowercases them, and returns True if they are equal, False otherwise."""
    if isinstance(val, str):
        return val.lower() == compareto.lower()
    else:
        return False


def slower(val):
    """If the passed val is a string, returns its lowercased representation. Returns the passed val unchanged otherwise."""
    if isinstance(val, str):
        return val.lower()
    else:
        return val


def sstrip(val):
    """If the passed val is a string, returns its whitespace-stripped representation. Returns the passed val unchanged otherwise."""
    if isinstance(val, str):
        return val.strip()
    else:
        return val


def supper(val):
    """If the passed val is a string, returns its uppercased representation. Returns the passed val unchanged otherwise."""
    if isinstance(val, str):
        return val.upper()
    else:
        return val


def tmpfile():
    """Generates a random file name, creates the file in /tmp, and returns the filespec to the caller."""
    file = '/tmp/%s.spx' % str(uuid.uuid4())
    # wipe(file)
    Path(file).touch()
    return file


def wipe(file):
    """Deletes the passed filename."""
    if os.path.isfile(file):
        os.unlink(file)


def xlatearg(argval):
    """ Normalize multiple argument values to a single canonical value.
    Allows acceptible variants that mean the same thing: e.g., analyze or analysis --> analyze."""
    arg = argval.strip().lower()
    if arg in ["analyze", "analysis"]:
        arg = "analyze"
    return arg
