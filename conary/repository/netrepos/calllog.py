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

        if (revision == 1):
            self.entKey = 'unknown'
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entClass),
             self.methodName, self.args, self.exceptionStr) = info[1:]
        elif (revision == 2):
            (self.serverName, self.timeStamp, self.remoteIp,
             (self.user, self.entClass, self.entKey),
             self.methodName, self.args, self.exceptionStr) = info[1:]
        else:
            assert(0)

class CallLogger:
    logFormatRevision = 2

    def __init__(self, logPath, serverNameList, readOnly = False):
        self.serverNameList = serverNameList
        self.path = logPath
        self.readOnly = readOnly
        self.logFd = None
        self.inode = None
        self.reopen()

    def reopen(self):
        reopen = False
        # if we've never had an inode, we can simply open
        if not self.inode:
            reopen = True
        else:
            try:
                sb = os.stat(self.path)
                inode = (sb.st_dev, sb.st_ino)
                if inode != self.inode:
                    reopen = True
            except OSError:
                reopen = True
        # if we don't need to re-open the log file, return now
        if not reopen:
            return
        # otherwise, re-open the log file
        if self.readOnly:
            self.logFd = os.open(self.path, os.O_RDONLY)
        else:
            self.logFd = os.open(self.path, os.O_CREAT | os.O_APPEND | os.O_RDWR)
        # record the inode of the log file
        sb = os.stat(self.path)
        self.inode = (sb.st_dev, sb.st_ino)

    def log(self, remoteIp, authToken, methodName, args, exception = None):
        # lazy re-open the log file in case it was rotated from underneath us
        self.reopen()
        if exception:
            exception = str(exception)

        (user, entClass, entKey) = authToken[0], authToken[2], authToken[3]
        logStr = cPickle.dumps((self.logFormatRevision, self.serverNameList,
                                time.time(), remoteIp, (user, entClass, entKey),
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

