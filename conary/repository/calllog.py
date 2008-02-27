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

CLIENT_LOG = 1000000

class ClientCallLogEntry:

    def __init__(self, info):
        self.revision = info[0]

        if (self.revision == CLIENT_LOG + 1):
            (self.url, self.entitlement,
             self.methodName, self.args, self.result,
             self.latency) = info[1:]

class AbstractCallLogger:

    def __init__(self, logPath, readOnly = False):
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

    def __iter__(self):
        fd = os.open(self.path, os.O_RDONLY)
        size = os.fstat(fd).st_size
        if not size:
            return

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

class ClientCallLogger(AbstractCallLogger):

    EntryClass = ClientCallLogEntry
    logFormatRevision = CLIENT_LOG + 1

    def log(self, url, entitlement, methodName, args, result, latency = None):
        # lazy re-open the log file in case it was rotated from underneath us
        self.reopen()

        logStr = cPickle.dumps((self.logFormatRevision, url, entitlement,
                                methodName, args, result, latency))
        os.write(self.logFd, struct.pack("!I", len(logStr)) + logStr)
