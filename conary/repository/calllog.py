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
        self.fobj = None
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
            self.fobj = open(self.path, 'rb')
        else:
            self.fobj = open(self.path, 'a+')
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
        size = struct.unpack("!I", self.fobj.read(4))[0]
        data = self.fobj.read(size)
        return self.EntryClass(cPickle.loads(data))

    def follow(self):
        self.fobj.seek(0, 2)
        where = self.fobj.tell()
        while True:
            size = os.fstat(self.fobj.fileno()).st_size
            while where < size:
                yield self.getEntry()
                where = self.fobj.tell()

            time.sleep(1)

    def close(self):
        if self.fobj is not None:
            self.fobj.close()
        self.fobj = None


class ClientCallLogger(AbstractCallLogger):

    EntryClass = ClientCallLogEntry
    logFormatRevision = CLIENT_LOG + 1

    def log(self, url, entitlement, methodName, args, result, latency = None):
        # lazy re-open the log file in case it was rotated from underneath us
        self.reopen()

        logStr = cPickle.dumps((self.logFormatRevision, url, entitlement,
                                methodName, args, result, latency))
        self.fobj.write(struct.pack("!I", len(logStr)) + logStr)
        self.fobj.flush()
