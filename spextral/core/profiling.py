import resource
import yappi

from spextral import globals
from spextral.core.utils import nowstr, info


def _m(x):
    return str(round(x / 1024.0 / globals.RSS_MEMORY_DIVISOR, 3))


def memory():
    """Take an available memory measurement. If larger than the current value of globals.MAX_RSS_MEMORY_USED,
    replace globals.MAX_RSS_MEMORY_USED with the new value. At EOP, globals.MAX_RSS_MEMORY_USED will contain
    the largest point in time amount of memory used by Spextral."""
    curr_mem_used = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - globals.RSS_MEMORY_BASE
    if curr_mem_used > globals.MAX_RSS_MEMORY_USED:
        globals.MAX_RSS_MEMORY_USED = curr_mem_used
    if globals.LAST_RSS_MEMORY_USED == 0:
        info("--profile: memory in use: %s MB so far" % _m(curr_mem_used))
    else:
        pct_change = str(round((curr_mem_used - globals.LAST_RSS_MEMORY_USED) / globals.LAST_RSS_MEMORY_USED * 100.0, 2))
        if curr_mem_used > globals.LAST_RSS_MEMORY_USED:
            pct_change = "+%s%%; " % pct_change
            info("** --profile: memory in use: %s MB (%s%s max MB so far)" %
                 (_m(curr_mem_used),
                  pct_change,
                  _m(globals.MAX_RSS_MEMORY_USED)))
        elif curr_mem_used < globals.LAST_RSS_MEMORY_USED:
            pct_change = "%s%%; " % pct_change
            info("** --profile: memory in use: %s MB (%s%s max MB so far)" %
                 (_m(curr_mem_used),
                  pct_change,
                  _m(globals.MAX_RSS_MEMORY_USED)))
    globals.LAST_RSS_MEMORY_USED = curr_mem_used


def start(tag=None):
    """Starts a yappi profiling session."""
    pstats_file = '/var/log/yappi.%s.%s.%s%s' % (globals.__NAME__, globals.__VERSION__, "%s." % tag if tag else "", nowstr())
    yappi.clear_stats()
    yappi.start()
    return pstats_file


def end(pstats_file):
    """Ends yappi profiling session, saves the profilinf info to a CProfile pstats file, and pretty-prints it to the console."""
    yappi.stop()
    func_stats = yappi.get_func_stats()
    if func_stats:
        _rows = []
        for _stat in func_stats._as_dict:
            if '/Spextral/' in _stat.full_name and '/venv/' not in _stat.full_name:
                _gizmo = _stat.full_name.split("/")[-1]
                _rows.append([_gizmo.split(" ")[1], _gizmo.split(" ")[0], _stat.ncall, _stat.tavg, _stat.ttot, _stat.tsub])
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