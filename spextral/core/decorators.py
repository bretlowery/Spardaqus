from queue import Queue, Empty as QueueEmpty
import threading
try:
    import thread
except ImportError:
    import _thread as thread

from spextral.core.exceptions import SpextralTimeoutWarning
from spextral.core.utils import info


def _call(timer, q, fn, args, kwargs):
    timer.start()
    result = fn(*args, **kwargs)
    q.put(result)
    timer.cancel()


def _timeout(q):
    q.put("TIMEDOUT")


def timeout_after(timeout_interval=None, timeout_message=None):
    """
    Use as decorator to exit process if function takes longer than s seconds
    """
    def outer(fn):
        def inner(*args, **kwargs):
            try:
                # if timeout_interval is not passed, use the encapsulating class's self.timeout property, if specified
                maxtime = timeout_interval if timeout_interval else args[0].timeout if args[0].timeout else 30
            except:
                maxtime = timeout_interval if timeout_interval else 30
                pass
            try:
                # if timeout_message is not passed, use the encapsulating class's self.timeoutmsg property, if specified
                timeoutmsg = timeout_message if timeout_message else args[0].timeoutmsg if args[0].timeoutmsg else "Operation timed out"
            except:
                timeoutmsg = timeout_message if timeout_message else "Operation timed out"
                pass
            q = Queue()
            results = None
            timer = threading.Timer(maxtime, _timeout, args=[q])
            timerthread = threading.Thread(target=_call, args=[timer, q, fn, args, kwargs])
            timerthread.start()
            while timer and not results:
                try:
                    results = q.get_nowait()
                except QueueEmpty:
                    pass
            if timer:
                timer.cancel()
            q.task_done()
            if results:
                if isinstance(results, str):
                    if results == "TIMEDOUT":
                        info(timeoutmsg)
                        raise SpextralTimeoutWarning
            return results
        return inner
    return outer
