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


import os
import stat
import struct
import tempfile

from conary import callbacks
from conary.files import InodeStream
from conary.streams import *

JOURNAL_VERSION = 1

_JOURNAL_ENTRY_OLD_NAME  = 1
_JOURNAL_ENTRY_NEW_NAME  = 2
_JOURNAL_ENTRY_NEW_INODE = 3

_INFO_STREAM_PERMS = 1
_INFO_STREAM_MTIME = 2
_INFO_STREAM_OWNER = 3
_INFO_STREAM_GROUP = 4

JOURNAL_ENTRY_RENAME        = 0
JOURNAL_ENTRY_BACKUP        = 1
JOURNAL_ENTRY_CREATE        = 2
JOURNAL_ENTRY_REMOVE        = 3
JOURNAL_ENTRY_MKDIR         = 4
JOURNAL_ENTRY_BACKDIR       = 5
JOURNAL_ENTRY_TRYCLEANUPDIR = 6

class InodeInfo(StreamSet):

    streamDict = {
          _INFO_STREAM_PERMS : (SMALL, ShortStream,  "perms"),
          _INFO_STREAM_MTIME : (SMALL, MtimeStream,  "mtime"),
          _INFO_STREAM_OWNER : (SMALL, IntStream,    "uid"  ),
          _INFO_STREAM_GROUP : (SMALL, IntStream,    "gid"  )
        }
    __slots__ = [ "perms", "mtime", "uid", "gid" ]

class JournalEntry(StreamSet):

    streamDict = {
          _JOURNAL_ENTRY_OLD_NAME  : (DYNAMIC, StringStream, "old" ),
          _JOURNAL_ENTRY_NEW_NAME  : (DYNAMIC, StringStream, "new" ),
          _JOURNAL_ENTRY_NEW_INODE : (SMALL,   InodeInfo,    "inode" )
        }
    __slots__ = [ "old", "new", "inode" ]

class NoopJobJournal:

    def __init__(self):
        pass

    def rename(self, origName, newName):
        pass

    def create(self, name):
        pass

    def mkdir(self, name):
        pass

    def remove(self, name):
        pass

    def backup(self, target, skipDirs = False):
        pass

    def commit(self):
        pass

    def removeJournal(self):
        pass

    def revert(self):
        pass

class JobJournal(NoopJobJournal):

    # this is designed to be readable back to front, not front to back

    @staticmethod
    def _normpath(path):
        return os.path.normpath(path).replace('//', '/')

    def __init__(self, path, root = '/', create = False, callback = None):
        NoopJobJournal.__init__(self)
        # normpath leaves a leading // (probably for windows?)
        self.path = path

        self.root = self._normpath(root)
        if root:
            self.rootLen = len(self.root)
        else:
            self.rootLen = 0

        self.hSize = struct.calcsize("!H")
        self.hdrSize = struct.calcsize("!BH")

        if callback is None:
            self.callback = callbacks.UpdateCallback()
        else:
            self.callback = callback

        if create:
            self.immutable = False
            self.fd = os.open(path,
                              os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0600)
            os.write(self.fd, struct.pack("!H", JOURNAL_VERSION))
        else:
            self.immutable = True
            self.fd = os.open(path, os.O_RDONLY)
            ver = struct.unpack("!H", os.read(self.fd, self.hSize))[0]
            assert(ver == JOURNAL_VERSION)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None

    def _record(self, kind, origName, newName):
        assert(not self.immutable)
        s = JournalEntry()
        s.old.set(origName[self.rootLen:])
        s.new.set(newName[self.rootLen:])
        frz = s.freeze()
        os.write(self.fd, frz + struct.pack("!BH", kind, len(frz)))

    def _backup(self, origName, newName, statBuf, kind = JOURNAL_ENTRY_BACKUP):
        assert(not self.immutable)
        s = JournalEntry()
        s.old.set(origName[self.rootLen:])
        s.new.set(newName[self.rootLen:])

        s.inode.mtime.set(statBuf.st_mtime)
        s.inode.uid.set(statBuf.st_uid)
        s.inode.gid.set(statBuf.st_gid)
        s.inode.perms.set(statBuf.st_mode & 07777)

        frz = s.freeze()
        os.write(self.fd, frz + struct.pack("!BH", kind, len(frz)))

    def _backdir(self, name, statBuf):
        self._backup("", name, statBuf, kind = JOURNAL_ENTRY_BACKDIR)

    def rename(self, origName, newName):
        origName = self._normpath(origName)
        newName = self._normpath(newName)
        self._record(JOURNAL_ENTRY_RENAME, origName, newName)

    def create(self, name):
        name = self._normpath(name)
        self._record(JOURNAL_ENTRY_CREATE, '', name)

    def mkdir(self, name):
        name = self._normpath(name)
        self._record(JOURNAL_ENTRY_MKDIR, '', name)

    def remove(self, name):
        name = self._normpath(name)
        self._record(JOURNAL_ENTRY_REMOVE, '', name)

    def tryCleanupDir(self, name):
        # on *commit* try to remove this directory
        name = self._normpath(name)
        self._record(JOURNAL_ENTRY_TRYCLEANUPDIR, '', name)

    def backup(self, target, skipDirs = False):
        target = self._normpath(target)
        try:
            sb = os.lstat(target)
        except OSError:
            sb = None

        if sb:
            path = os.path.dirname(target)
            name = os.path.basename(target)

            tmpfd, tmpname = tempfile.mkstemp(name, '.ct', path)
            os.close(tmpfd)
            os.unlink(tmpname)
            if not stat.S_ISDIR(sb.st_mode):
                self._backup(target, tmpname, sb)
                os.link(target, tmpname)
            elif not skipDirs:
                self._backdir(target, sb)

    def commit(self):
        for kind, entry in self:
            if kind == JOURNAL_ENTRY_BACKUP:
                os.unlink(self.root + entry.new())
            elif kind == JOURNAL_ENTRY_TRYCLEANUPDIR:
                # XXX would be nice to avoid the try/except here with some C
                try:
                    os.rmdir(self.root + entry.new())
                except OSError:
                    pass
        self.close()

    def removeJournal(self):
        os.unlink(self.path)

    def revert(self):
        for kind, entry in self:
            try:
                if kind == JOURNAL_ENTRY_BACKUP:
                    what = "restore"
                    path = self.root + entry.old()
                    os.rename(self.root + entry.new(), path)
                    os.chown(path, entry.inode.uid(), entry.inode.gid())
                    os.chmod(path, entry.inode.perms())
                    os.utime(path, (entry.inode.mtime(), entry.inode.mtime()))
                elif kind == JOURNAL_ENTRY_RENAME:
                    what = "restore"
                    os.rename(self.root + entry.new(), self.root + entry.old())
                elif kind == JOURNAL_ENTRY_CREATE:
                    what = "remove"
                    os.unlink(self.root + entry.new())
                elif kind == JOURNAL_ENTRY_MKDIR:
                    what = "remove"
                    os.rmdir(self.root + entry.new())
                elif kind == JOURNAL_ENTRY_REMOVE:
                    pass
                elif kind == JOURNAL_ENTRY_BACKDIR:
                    pass
                elif kind == JOURNAL_ENTRY_TRYCLEANUPDIR:
                    pass
                else:
                    self.callback.warning('unknown journal entry %d', kind)
            except OSError, e:
                self.callback.warning('could not %s file %s: %s',
                                      what, self.root + entry.new(), e.strerror)
        self.close()

    def __iter__(self):
        self.immutable = False
        next = os.fstat(self.fd).st_size - self.hdrSize
        while next > 0:
            os.lseek(self.fd, next, 0)
            kind, size = struct.unpack("!BH", os.read(self.fd, self.hdrSize))

            os.lseek(self.fd, next - size, 0)
            frz = os.read(self.fd, size)
            s = JournalEntry(frz)
            yield kind, s
            next = next - self.hdrSize - size
