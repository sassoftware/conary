#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


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
