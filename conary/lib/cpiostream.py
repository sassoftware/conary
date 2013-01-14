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


"""
Streaming implementation of a CPIO archive.
Usage::

    >>> fileObj = file("archive.cpio")
    >>> arch = CpioStream(fileObj)
    >>> for ent in arch:
    >>>    print "Entry name:", ent.filename
    >>>    print "Payload size:", ent.header.filesize
    >>>    # To read the entry contents
    >>>    while True:
    >>>        buf = ent.payload.read(1024)
    >>>        if not buf:
    >>>            break
    >>>        # Do something with the data
"""

import os
import stat
import StringIO
import struct
import sys
import errno

class Error(Exception):
    "Base exception"

class InvalidMagicError(Exception):
    "Invalid magic"

class IncompleteHeaderError(Exception):
    "Incomplete header"

class InvalidFieldValue(Exception):
    "Invalid field value"

class ShortReadError(Exception):
    "Short read"

class OutOfOrderRead(Exception):
    "Read out of order"

class CpioHeader(object):
    __slots__ = [
        'magic',
        'inode', 'mode', 'uid', 'gid', 'nlink', 'mtime', 'filesize',
        'devmajor', 'devminor', 'rdevmajor', 'rdevminor', 'namesize', 'check',
        # These slots are for internal use and not part of the header structure
        # How many bytes to skip to get to the next header, relative to the
        # beginning of this header
        'skip',
    ]
    HeaderLength = 110
    MAGIC = '070701'
    def __init__(self, data):
        assert(len(data) == self.HeaderLength)
        # Conveniently, the new ASCII format defines all fields (except for
        # the magic) to be 8 chars long, each field being a hex encoded
        # length.
        format = '6s' + '8s' * 13
        arr = struct.unpack(format, data)
        for slotName, val in zip(self.__slots__, arr):
            if slotName != 'magic':
                try:
                    val = int(val, 16)
                except ValueError:
                    raise InvalidFieldValue(val)
            setattr(self, slotName, val)
        # Pad to multiple of four, first the file name
        self.skip = self.pad(self.HeaderLength + self.namesize)
        # Then the file size
        self.skip += self.pad(self.filesize)

    def serialize(self):
        out = StringIO.StringIO()
        out.write(self.magic)
        for slotName in self.__slots__[1:14]:
            out.write("%08x" % getattr(self, slotName))
        return out.getvalue()

    @classmethod
    def pad(cls, number):
        return number + 3 - (number + 3) % 4

class OverlayStream(object):
    __slots__ = [ '_stream', '_startPosition', '_currentPosition', '_size' ]

    def __init__(self, stream, size):
        self._stream = stream
        self._startPosition = stream.tell()
        self._currentPosition = 0
        self._size = size

    def read(self, amt = None):
        # We track the position in the parent stream, so if the parent stream
        # advanced outside of this read(), we need to fail
        if self._currentPosition >= self._size:
            return ""
        self._verifyPosition()
        if amt is None:
            amt = self._size - self._currentPosition
        else:
            amt = min(amt, self._size - self._currentPosition)
        buf = self._stream.read(amt)
        self._currentPosition += len(buf)
        return buf

    def tell(self):
        return self._currentPosition

    def _verifyPosition(self):
        if self._startPosition + self._currentPosition != self._stream.tell():
            raise OutOfOrderRead()

class CpioEntry(object):
    __slots__ = [ 'header', 'filename', 'payload' ]
    def __init__(self, header, filename, payload):
        self.header = header
        self.filename = filename
        self.payload = payload

class CpioStream(object):
    def __init__(self, stream):
        self.stream = stream
        self._nextEntry = 0
        self._currentPosition = 0

    def next(self):
        if self._nextEntry != self._currentPosition:
            self._readExact(self._nextEntry - self._currentPosition)
        buf = self._readExact(CpioHeader.HeaderLength, eofOK = True)
        if not buf:
            return None
        header = self.readHeader(buf)
        # Trim out the trailing NUL byte
        filename = self._readExact(header.namesize)[:-1]
        pad = (header.pad(header.HeaderLength + header.namesize) -
            header.HeaderLength - header.namesize)
        if pad:
            self._readExact(pad)
        # self._currentPosition is a multiple of 4 already, but calling pad()
        # is handy
        self._nextEntry = header.pad(self._currentPosition + header.filesize)
        payload = OverlayStream(self, header.filesize)
        entry = CpioEntry(header, filename, payload)
        return entry

    def __iter__(self):
        while True:
            entry = self.next()
            if entry is None or entry.filename == 'TRAILER!!!':
                break
            yield entry

    def read(self, amt):
        return self._readExact(amt)
        out = StringIO.StringIO()
        buf = self.stream.read(size)
        if not buf:
            return
        bufLen = len(buf)
        offset = self._nextHeader
        while offset < bufLen:
            headerRemainder = CpioHeader.HeaderLength - bufLen + offset
            if headerRemainder > 0:
                nbuf = self.stream.read(headerRemainder)
                bufLen += len(nbuf)
                buf += nbuf
            header = self.readHeader(buf[offset:])
            self.transformHeader(header)
            out.write(header.serialize())
            out.write(buf[offset + CpioHeader.HeaderLength:offset + header.skip])
            offset += header.skip
        self._nextHeader = offset - bufLen
        return buf

    @classmethod
    def readHeader(cls, buf):
        if len(buf) < CpioHeader.HeaderLength:
            raise IncompleteHeaderError()
        header = CpioHeader(buf[:CpioHeader.HeaderLength])
        if header.magic != header.MAGIC:
            raise InvalidMagicError(header.magic)
        return header

    def tell(self):
        return self._currentPosition

    def _readExact(self, amt, eofOK = False):
        buf = self.stream.read(amt)
        if not buf and eofOK:
            return None
        if len(buf) != amt:
            raise ShortReadError("Expected %d bytes, got %d" % (
                amt, len(buf)))
        self._currentPosition += amt
        return buf

class CpioExploder(CpioStream):

    def explode(self, destDir):
        linkMap = {}
        for ent in self:
            try:
                target = destDir + '/' + ent.filename

                parent = os.path.dirname(target)
                if not os.path.exists(parent):
                    os.makedirs(parent)

                if stat.S_ISCHR(ent.header.mode):
                    os.mknod(target, stat.S_IFCHR,
                             os.makedev(ent.rdevmajor, ent.rdevminor))
                elif stat.S_ISBLK(ent.header.mode):
                    os.mknod(target, stat.S_IFBLK,
                             os.makedev(ent.rdevmajor, ent.rdevminor))
                elif stat.S_ISDIR(ent.header.mode):
                    os.mkdir(target)
                elif stat.S_ISFIFO(ent.header.mode):
                    os.mkfifo(target)
                elif stat.S_ISLNK(ent.header.mode):
                    os.symlink(ent.payload.read(),target)
                elif stat.S_ISREG(ent.header.mode):
                    # save hardlinks until after the file content is written
                    if ent.header.nlink > 1 and ent.header.filesize == 0:
                        l = linkMap.get(ent.header.inode, [])
                        l.append(target)
                        linkMap[ent.header.inode] = l
                        continue

                    f = open(target, "w")
                    buf = ent.payload.read(64 * 1024)
                    while buf:
                        f.write(buf)
                        buf = ent.payload.read(64 * 1024)
                    f.close()
                else:
                    raise Error("unknown file mode 0%o for %s"
                                % (ent.header.mode, ent.filename))

                # create hardlinks after the file content is written
                if ent.header.nlink > 1 and ent.header.filesize:
                    l = linkMap.get(ent.header.inode, [])
                    # the last entry with the same inode should contain the
                    # contents so this list should always have at least one
                    # entry
                    assert(l)
                    for t in l:
                        os.link(target, t)
                # create hardlinks after the file content is written

            except OSError, e:
                if e.errno == errno.EEXIST:
                    pass
                else:
                    raise
            if not stat.S_ISLNK(ent.header.mode):
                os.chmod(target, ent.header.mode & 0777)

if __name__ == '__main__':
    sys.exit(main())
