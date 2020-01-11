import threading
import _thread as thread
import sys

from spextral.core.utils import error


def _timeout(timeoutmsg=None):
    if timeoutmsg:
        error(timeoutmsg, onerrorexit=True)
    thread.interrupt_main()  # raises KeyboardInterrupt
    sys.exit(1)


def timeout_after(s):
    """
    Use as decorator to exit process if function takes longer than s seconds
    """
    def outer(fn):
        def inner(*args, **kwargs):
            try:
                maxtime = s if s < 30 else args[0].timeout   # this is an encapsulating class's self.timeout property
            except:
                maxtime = s
                pass
            try:
                timeoutmsg = args[0].timeoutmsg  # this is an encapsulating class's self.timeoutmsg property
            except:
                timeoutmsg = "Operation timed out"
                pass
            timer = threading.Timer(maxtime, _timeout, args=[timeoutmsg])
            timer.start()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer.cancel()
            return result
        return inner
    return outer

