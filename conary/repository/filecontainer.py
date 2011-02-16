#
# Copyright (c) 2010 rPath, Inc.
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
"""
Provides a file which can store multiple files inside of it.

A table of contents is utilized, and the caller can attach arbitrary
data to an entry in the table. No compression is used, but that could
be changed.

The data attached to table entries can be modified, but doing so isn't
particularly efficient (the entire table gets rewritten).

The last file added to the container can be erased from it. This is
to allow operations to be reverted.

The file format is::
  - magic
  - file format version
  - file table entry 1
  - file table entry 2
  - file N

The header and table entries are uncompressed.  The contents of the file
table are compressed, and each file is individually compressed. When files
are retrieved from the container, the returned file object automatically
uncompresses the file.

There are two formats for file table entries. The original format is used
for all files less than 4GB in size (after compression)::

 - SUBFILE_MAGIC (2 bytes)
 - length of file name (2 bytes)
 - length of compressed file data(4 bytes)
 - length of arbitrary data (2 bytes)
 - file name
 - arbitrary file table data
 - file data

Files larger than 4GB are stored somewhat differently:

 - LARGE_SUBFILE_MAGIC (2 bytes)
 - length of file name + length of file table data + file length (8 bytes)
 - file name
 - arbitrary file table data
 - file data
 - length of file name (2 bytes)
 - length of arbitrary data (2 bytes)

This somewhat tortuous format was designed to accomodate the addFile API not
knowing the total size being stored as well as the dynamic compression making
that size unknowable in advance. It does limit file storage size a bit, but
leaves us with well over 63 bits of length.

This code is careful not to depend on the file pointer at all for reading
(via pread). The file pointer is used while creating file containers.
"""

import gzip
import struct

import conary.errors
from conary.lib import util
from conary.repository import filecontents

FILE_CONTAINER_MAGIC = "\xEA\x3F\x81\xBB"
SUBFILE_MAGIC = 0x3FBB
# used for files whose contents are > 4gig
LARGE_SUBFILE_MAGIC = 0x40CD

# File container versions. Add references to these in netclient too.
FILE_CONTAINER_VERSION_FILEID_IDX   = 2007022001
FILE_CONTAINER_VERSION_WITH_REMOVES = 2006071301
FILE_CONTAINER_VERSION_NO_REMOVES   = 2005101901

READABLE_VERSIONS = [ FILE_CONTAINER_VERSION_FILEID_IDX,
                      FILE_CONTAINER_VERSION_WITH_REMOVES,
                      FILE_CONTAINER_VERSION_NO_REMOVES ]

FILE_CONTAINER_VERSION_LATEST = max(READABLE_VERSIONS)

SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

class FileContainer:

    bufSize = 128 * 1024

    def readHeader(self):
        magic = self.file.pread(4, 0)
        if len(magic) != 4 or magic != FILE_CONTAINER_MAGIC:
            raise BadContainer, "bad magic"

        version = self.file.pread(4, 4)
        if len(version) != 4:
            raise BadContainer, "invalid container version"
        self.version = struct.unpack("!I", version)[0]
        if self.version not in READABLE_VERSIONS:
            raise BadContainer, "unsupported file container version %d" % \
                        self.version

        self.contentsStart = 8
        self.next = self.contentsStart

    def close(self):
        self.file = None

    def addFile(self, fileName, contents, tableData, precompressed = False):
        assert(isinstance(contents, filecontents.FileContents))
        assert(self.mutable)

        fileObj = contents.get()
        headerOffset = self.file.tell()
        self.file.write(struct.pack("!HH", SUBFILE_MAGIC, len(fileName)))
        self.file.write(struct.pack("!IH", 0, len(tableData)))
        self.file.write(fileName)
        self.file.write(tableData)

        if precompressed:
            size = util.copyfileobj(fileObj, self.file)
        else:
            start = self.file.tell()
            gzFile = gzip.GzipFile('', "wb", 6, self.file)
            util.copyfileobj(fileObj, gzFile)
            gzFile.close()
            size = self.file.tell() - start

        if size < 0x100000000:
            self.file.seek(headerOffset + 4, SEEK_SET)
            self.file.write(struct.pack("!I", size))
            self.file.seek(0, SEEK_END)
        else:
            self.file.seek(headerOffset, SEEK_SET)
            totalSize = size + len(fileName) + len(tableData)
            self.file.write(struct.pack("!HII", LARGE_SUBFILE_MAGIC,
                                        totalSize >> 32,
                                        totalSize & 0xFFFFFFFF))
            self.file.seek(0, SEEK_END)
            self.file.write(struct.pack("!HH", len(fileName), len(tableData)))

    def getNextFile(self):
        assert(not self.mutable)

        name, tag, size, dataOffset, nextOffset = self._nextFile()

        if name is None:
            return None

        fcf = util.SeekableNestedFile(self.file, size, start = dataOffset)

        self.next = nextOffset

        return (name, tag, fcf)

    def _nextFile(self):
        offset = self.next

        nameLen = self.file.pread(10, offset)
        if not len(nameLen):
            return (None, None, None, None, None)

        offset += 10

        subMagic = struct.unpack("!H", nameLen[0:2])[0]
        if subMagic == LARGE_SUBFILE_MAGIC:
            most, least = struct.unpack("!II", nameLen[2:])
            totalSize = (most << 32) + least

            otherLengths = self.file.pread(4, offset + totalSize)
            nameLen, tagLen  = struct.unpack("!HH", otherLengths)
            size = totalSize - nameLen - tagLen
            nextOffset = offset + totalSize + 4
        else:
            assert(subMagic == SUBFILE_MAGIC)
            nameLen, size, tagLen = struct.unpack("!HIH", nameLen[2:])
            nextOffset = offset + nameLen + tagLen + size

        name = self.file.pread(nameLen, offset)
        offset += nameLen
        tag = self.file.pread(tagLen, offset)
        offset += tagLen

        return (name, tag, size, offset, nextOffset)

    @staticmethod
    def _packFileHeader(name, tag, size):
        """Return the header and footer for the given contents."""
        if size < 0x100000000:
            header = struct.pack('!HHIH', SUBFILE_MAGIC,
                    len(name), size, len(tag))
            footer = ''
        else:
            total = size + len(name) + len(tag)
            header = struct.pack('!HQ', LARGE_SUBFILE_MAGIC, total)
            footer = struct.pack('!HH', len(name), len(tag))
        return ''.join((header, name, tag)), footer

    def dumpIter(self, readFileFunc, args=()):
        """Dump the changeset as a byte stream, yielding chunks of bytes.

        C{readFileFunc} allows the caller to replace placeholders with actual
        file contents. It should take as arguments C{name, tag, rawSize,
        subfile}, C{subfile} being a file-like object of size C{rawSize}, and
        return a tuple C{(tag, expandedSize, dataIter)}. C{tag} replaces the
        old tag. C{dataIter} is an iterable that yields bytestrings totalling
        C{expandedSize} bytes.

        @param readFileFunc: File reading callback
        @type  readFileFunc: callable
        @param args: Extra arguments to pass to C{readFileFunc}
        @type  args: C{tuple}
        """
        assert not self.mutable

        fileHeader = self.file.pread(8, 0)
        yield fileHeader

        next = self.getNextFile()
        while next is not None:
            name, tag, subfile = next
            rawSize = subfile.size
            tag, expandedSize, dataIter = readFileFunc(name, tag, rawSize,
                    subfile, *args)
            header, footer = self._packFileHeader(name, tag, expandedSize)

            yield header
            for chunk in dataIter:
                yield chunk

            # > 4 GiB files have length of file name and tag after contents
            # (see format at the top of this file)
            yield footer

            next = self.getNextFile()

    def reset(self):
        """
        Reset the current position in the filecontainer to the beginning.
        """
        assert(not self.mutable)
        self.next = self.contentsStart

    def __del__(self):
        if self.file:
            self.close()

    def __init__(self, file, version = None, append = False):
        """
        Create a FileContainer object.

        @param file: an open python file object referencing the file
        container file on disk. If that file is empty (size 0) the
        file container is immediately initialized. A copy of the file
        is retained, so the caller may optionally close it.
        @param append: if True, creates a new filecontainer at the end
        of the passed flie object
        """

        # make our own copy of this file which nobody can close underneath us
        self.file = file

        if version is None:
            version = FILE_CONTAINER_VERSION_LATEST

        self.file.seek(0, SEEK_END)
        if append or not self.file.tell():
            if not append:
                self.file.seek(SEEK_SET, 0)
                self.file.truncate()

            self.file.write(FILE_CONTAINER_MAGIC)
            self.file.write(struct.pack("!I", version))

            self.mutable = True
        else:
            # we don't need to put this file pointer back; we don't depend
            # on it here at all; everything is through pseek
            try:
                self.readHeader()
            except:
                self.file.close()
                self.file = None
                raise
            self.mutable = False

class BadContainer(conary.errors.ConaryError):

    pass
