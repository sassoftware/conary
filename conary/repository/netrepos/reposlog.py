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


import cPickle, mmap, os, struct, time

from conary.repository import calllog
from conary.lib import log

class RepositoryCallLogEntry:

    def __init__(self, info):
        self.revision = info[0]

        if (self.revision == 1):
            self.entKey = 'unknown'
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entClass),
             self.methodName, self.args, self.exceptionStr) = info[1:]
        elif (self.revision == 2):
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entClass, self.entKey),
             self.methodName, self.args, self.exceptionStr) = info[1:]
        elif (self.revision == 3):
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entitlements),
             self.methodName, self.args, self.exceptionStr) = info[1:]
        elif (self.revision == 4):
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entitlements),
             self.methodName, self.args, self.kwArgs,
             self.exceptionStr) = info[1:]
        elif (self.revision == 5):
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entitlements),
             self.methodName, self.args, self.kwArgs,
             self.exceptionStr, self.latency) = info[1:]
        elif (self.revision == 6):
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entitlements),
             self.methodName, self.args, self.kwArgs,
             self.exceptionStr, self.latency, self.systemId) = info[1:]
        else:
            assert(0)

class RepositoryCallLogger(calllog.AbstractCallLogger):

    EntryClass = RepositoryCallLogEntry
    logFormatRevision = 6

    def __init__(self, logPath, serverNameList, readOnly = False):
        self.serverNameList = serverNameList
        calllog.AbstractCallLogger.__init__(self, logPath, readOnly = readOnly)

    def log(self, remoteIp, authToken, methodName, args, kwArgs = {},
            exception = None, latency = None, systemId = None):
        # lazy re-open the log file in case it was rotated from underneath us
        self.reopen()
        if exception:
            exception = str(exception)

        (user, entitlements) = authToken[0], authToken[2]
        logStr = cPickle.dumps((self.logFormatRevision, self.serverNameList,
                                time.time(), remoteIp, (user, entitlements),
                                methodName, args, kwArgs, exception,
                                latency, systemId))
        try:
            self.fobj.write(struct.pack("!I", len(logStr)) + logStr)
            self.fobj.flush()
        except IOError, e:
            log.warning("'%s' while logging call from (%s,%s) to %s\n",
                        str(e), remoteIp, user, methodName)

    def __iter__(self):
        fd = os.open(self.path, os.O_RDONLY)
        size = os.fstat(fd).st_size
        if size == 0:
            raise StopIteration
        map = mmap.mmap(fd, size, access = mmap.ACCESS_READ)
        i = 0
        while i < size:
            length = struct.unpack("!I", map[i: i + 4])[0]
            i += 4
            yield self.EntryClass(cPickle.loads(map[i:i + length]))
            i += length

        os.close(fd)
