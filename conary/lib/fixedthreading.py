#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
