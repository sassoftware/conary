#
# Copyright (c) 2006 rPath, Inc.
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

import os
import stat
import struct
import tempfile

from conary.lib import log
from conary.streams import *

JOURNAL_VERSION = 1

_JOURNAL_ENTRY_OLD_NAME = 1
_JOURNAL_ENTRY_NEW_NAME = 2

JOURNAL_ENTRY_RENAME = 0
JOURNAL_ENTRY_BACKUP = 1
JOURNAL_ENTRY_CREATE = 2
JOURNAL_ENTRY_REMOVE = 3
JOURNAL_ENTRY_MKDIR  = 4

class JournalEntry(StreamSet):

    streamDict = { _JOURNAL_ENTRY_OLD_NAME : (DYNAMIC, StringStream, "old" ),
                   _JOURNAL_ENTRY_NEW_NAME : (DYNAMIC, StringStream, "new" ) }
    __slots__ = [ "old", "new" ]

class JobJournal:

    # this is designed to be readable back to front, not front to back

    @staticmethod
    def _normpath(path):
        return os.path.normpath(path).replace('//', '/')

    def __init__(self, path, root = '/', create = False):
        # normpath leaves a leading // (probably for windows?)
        self.root = self._normpath(root)
        if root:
            self.rootLen = len(self.root)
        else:
            self.rootLen = 0

        self.hSize = struct.calcsize("!H")
        self.hdrSize = struct.calcsize("!BH")

        if create:
            self.immutable = False
            self.fd = os.open("/tmp/journal",
                              os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0600)
            os.write(self.fd, struct.pack("!H", JOURNAL_VERSION))
        else:
            self.immutable = True
            self.fd = os.open(path, os.O_RDONLY)
            ver = struct.unpack("!H", os.read(self.fd, self.hSize))[0]
            assert(ver == JOURNAL_VERSION)

    def _record(self, kind, origName, newName):
        assert(not self.immutable)
        s = JournalEntry()
        s.old.set(origName[self.rootLen:])
        s.new.set(newName[self.rootLen:])
        frz = s.freeze()
        os.write(self.fd, frz)
        os.write(self.fd, struct.pack("!BH", kind, len(frz)))

    def _backup(self, origName, newName):
        name = self._normpath(origName)
        name = self._normpath(newName)
        self._record(JOURNAL_ENTRY_BACKUP, origName, newName)

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

    def backup(self, target, skipDirs = False):
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
                self._backup(target, tmpname)
                os.link(target, tmpname)
            elif not skipDirs:
                self._rename(target, tmpname)
                os.rename(target, tmpname)

    def commit(self):
        for kind, entry in self:
            if kind == JOURNAL_ENTRY_BACKUP:
                os.unlink(self.root + entry.new())

    def revert(self):
        for kind, entry in self:
            try:
                if kind == JOURNAL_ENTRY_BACKUP:
                    what = "restore"
                    os.rename(self.root + entry.new(), self.root + entry.old())
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
                else:
                    log.warning('unknown journal entry %d', kind)
            except OSError, e:
                log.warning('could not %s file %s: %s',
                            what, self.root + entry.new(), e.strerror)

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
