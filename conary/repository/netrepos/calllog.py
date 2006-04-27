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

class CallLogger:

    logFormatRevision = 1

    def log(self, remoteIp, authToken, methodName, args, exception = None):
        (user, entitlement) = authToken[0], authToken[2]
        logStr = cPickle.dumps((self.logFormatRevision, self.serverName,
                                time.time(), remoteIp, (user, entitlement),
                                methodName, args, str(exception)))
        os.write(self.logFd, struct.pack("!I", len(logStr)) + logStr)

    def __iter__(self):
        fd = os.open(self.path, os.O_RDONLY)
        size = os.fstat(fd).st_size
        map = mmap.mmap(fd, size, access = mmap.ACCESS_READ)
        i = 0
        while i < size:
            length = struct.unpack("!I", map[i: i + 4])[0]
            i += 4
            item = cPickle.loads(map[i:i + length])
            i += length
            yield item

    def __init__(self, logPath, serverName):
        self.serverName = serverName
        self.path = logPath
        self.logFd = os.open(logPath, os.O_CREAT | os.O_APPEND | os.O_WRONLY)

