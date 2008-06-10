#
# Copyright (c) 2004-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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
        else:
            assert(0)

class RepositoryCallLogger(calllog.AbstractCallLogger):

    EntryClass = RepositoryCallLogEntry
    logFormatRevision = 5

    def __init__(self, logPath, serverNameList, readOnly = False):
        self.serverNameList = serverNameList
        calllog.AbstractCallLogger.__init__(self, logPath, readOnly = readOnly)

    def log(self, remoteIp, authToken, methodName, args, kwArgs = {},
            exception = None, latency = None):
        # lazy re-open the log file in case it was rotated from underneath us
        self.reopen()
        if exception:
            exception = str(exception)

        (user, entitlements) = authToken[0], authToken[2]
        logStr = cPickle.dumps((self.logFormatRevision, self.serverNameList,
                                time.time(), remoteIp, (user, entitlements),
                                methodName, args, kwArgs, exception,
                                latency))
        try:
            os.write(self.logFd, struct.pack("!I", len(logStr)) + logStr)
        except OSError, e:
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

    def getEntry(self):
        size = struct.unpack("!I", os.read(self.logFd, 4))[0]
        return self.EntryClass(cPickle.loads(os.read(self.logFd, size)))

    def follow(self):
        where = os.lseek(self.logFd, 0, 2)
        while True:
            size = os.fstat(self.logFd).st_size
            while where < size:
                yield self.getEntry()
                where = os.lseek(self.logFd, 0, 1)

            time.sleep(1)

