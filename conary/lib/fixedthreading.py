#
# This file is a modified copy of the threading.py module distributed
# as part of python 2.3. As such, it is licensed under the Python
# Software Foundation license agreement, which can be found at
# www.python.org.

# there is a bug in Python 2.4.1.  Here's the fix:
# http://svn.python.org/view?rev=41525&view=rev

from threading import Thread, currentThread
import sys
from time import time as _time

if sys.version_info[:3] < (2, 4, 3):
    BrokenThread = Thread
    class Thread(BrokenThread):
        def join(self, timeout=None):
            assert self._Thread__initialized, "Thread.__init__() not called"
            assert self._Thread__started, "cannot join thread before it is started"
            assert self is not currentThread(), "cannot join current thread"
            if __debug__:
                if not self._Thread__stopped:
                    self._note("%s.join(): waiting until thread stops", self)
            self._Thread__block.acquire()
            try:
                if timeout is None:
                    while not self.__stopped:
                        self._Thread__block.wait()
                    if __debug__:
                        self._note("%s.join(): thread stopped", self)
                else:
                    deadline = _time() + timeout
                    while not self._Thread__stopped:
                        delay = deadline - _time()
                        if delay <= 0:
                            if __debug__:
                                self._note("%s.join(): timed out", self)
                            break
                        self.__block.wait(delay)
                    else:
                        if __debug__:
                            self._note("%s.join(): thread stopped", self)
            finally:
                self.__block.release()

