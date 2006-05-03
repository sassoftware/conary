#
# Copyright (c) 2004-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import cPickle, mmap, os, struct, time

class CallLogEntry:

    def __init__(self, info):
        revision = info[0]
        assert(revision == 1)

        (self.serverName, self.timeStamp, self.remoteIp,
         (self.user, self.entitlement), 
         self.methodName, self.args, self.exceptionStr) = info[1:]

class CallLogger:

    logFormatRevision = 1

    def log(self, remoteIp, authToken, methodName, args, exception = None):
        if exception:
            exception = str(exception)

        (user, entitlement) = authToken[0], authToken[2]
        logStr = cPickle.dumps((self.logFormatRevision, self.serverName,
                                time.time(), remoteIp, (user, entitlement),
                                methodName, args, exception))
        os.write(self.logFd, struct.pack("!I", len(logStr)) + logStr)

    def __iter__(self):
        fd = os.open(self.path, os.O_RDONLY)
        size = os.fstat(fd).st_size
        map = mmap.mmap(fd, size, access = mmap.ACCESS_READ)
        i = 0
        while i < size:
            length = struct.unpack("!I", map[i: i + 4])[0]
            i += 4
            yield CallLogEntry(cPickle.loads(map[i:i + length]))
            i += length

        os.close(fd)

    def getEntry(self):
        size = struct.unpack("!I", os.read(self.logFd, 4))[0]
        return CallLogEntry(cPickle.loads(os.read(self.logFd, size)))

    def follow(self):
        where = os.lseek(self.logFd, 0, 2)
        while True:
            size = os.fstat(self.logFd).st_size
            while where < size:
                yield self.getEntry()
                where = os.lseek(self.logFd, 0, 1)

            time.sleep(1)

    def __init__(self, logPath, serverName, readOnly = False):
        self.serverName = serverName
        self.path = logPath
        if readOnly:
            self.logFd = os.open(logPath, os.O_RDONLY)
        else:
            self.logFd = os.open(logPath, os.O_CREAT | os.O_APPEND | os.O_RDWR)

