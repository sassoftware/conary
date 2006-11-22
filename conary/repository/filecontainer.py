#
# Copyright (c) 2004-2005 rPath, Inc.
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
  - file 1
  - file table entry 2
  - file 2
  .
  .
  .
  - file table entry N
  - file N

The header and table entries are uncompressed.  The contents of the file 
table are compressed, and each file is individually compressed. When files 
are retrieved from the container, the returned file object automatically 
uncompresses the file.

Each file table entry looks like::

  SUBFILE_MAGIC (2 bytes)
  length of entry (4 bytes), not including these 4 bytes
  length of file name (4 bytes)
  length of compressed file data(4 bytes)
  length of arbitrary data (4 bytes)
  file name 
  arbitrary file table data
"""

import gzip
import struct

import conary.errors
from conary.lib import util
from conary.repository import filecontents

FILE_CONTAINER_MAGIC = "\xEA\x3F\x81\xBB"
SUBFILE_MAGIC = 0x3FBB
FILE_CONTAINER_VERSION = 2005101901
READABLE_VERSIONS = [ FILE_CONTAINER_VERSION ]
SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

class FileContainer:

    bufSize = 128 * 1024

    def readHeader(self):
	magic = self.file.read(4)
	if len(magic) != 4 or magic != FILE_CONTAINER_MAGIC:
	    raise BadContainer, "bad magic"

	version = self.file.read(4)
	if len(version) != 4:
	    raise BadContainer, "invalid container version"
	version = struct.unpack("!I", version)[0]
	if version not in READABLE_VERSIONS:
	    raise BadContainer, "unsupported file container version %d" % \
			version

	self.contentsStart = self.file.tell()
	self.next = self.contentsStart

    def close(self):
	self.file = None
    
    def addFile(self, fileName, contents, tableData, precompressed = False):
	assert(isinstance(contents, filecontents.FileContents))
	assert(self.mutable)

	fileObj = contents.get()
        self.file.write(struct.pack("!HH", SUBFILE_MAGIC, len(fileName)))
        sizeLoc = self.file.tell()
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

        self.file.seek(sizeLoc, SEEK_SET)
        self.file.write(struct.pack("!I", size))
        self.file.seek(0, SEEK_END)

    def getNextFile(self):
	assert(not self.mutable)

        self.file.seek(self.next, SEEK_SET)
	name, tag, size = self.nextFile()

	if name is None:
	    return None

	fcf = util.SeekableNestedFile(self.file, size)

	self.next = self.file.tell() + size

	return (name, tag, fcf)

    def nextFile(self):
	nameLen = self.file.read(10)
	if not len(nameLen):
	    return (None, None, None)

	subMagic, nameLen, size, tagLen = struct.unpack("!HHIH", nameLen)
        assert(subMagic == SUBFILE_MAGIC)
	name = self.file.read(nameLen)
	tag = self.file.read(tagLen)
	return (name, tag, size)

    def dumpToFile(self, outF):
        def _writeNestedFile(outF, name, tag, size, f, sizeCb):
            if 'refr' == tag[2:]:
                path = f.read()
                f = open(path)
                tag = tag[0:2] + 'file'

            headerLen = sizeCb(size, tag)
            bytes = util.copyfileobj(f, outF)
            return headerLen + bytes

        self.dump(outF.write,
                    lambda name, tag, size, f, sizeCb:
                        _writeNestedFile(outF, name, tag, size, f,
                                         sizeCb))

    def dump(self, dumpString, dumpFile):
        def sizeCallback(dumpString, name, tag, size):
            hdr = struct.pack("!HHIH%ds%ds" % (len(name), len(tag)),
                        SUBFILE_MAGIC, len(name), size, len(tag), name, tag)
            dumpString(hdr)
            return len(hdr)

	assert(not self.mutable)
        pos = self.file.seek(SEEK_SET, 0)

        fileHeader = self.file.read(8)
        dumpString(fileHeader)
        s = len(fileHeader)

        next = self.getNextFile()
        while next is not None:
            (name, tag, fcf) = next
            size = fcf.size
            s += dumpFile(name, tag, size, fcf,
                     lambda size, newTag: 
                        sizeCallback(dumpString, name, newTag, size))
            next = self.getNextFile()

        return s

    def reset(self):
        """
        Reset the current position in the filecontainer to the beginning.
        """
        assert(not self.mutable)
        self.file.seek(self.contentsStart, SEEK_SET)
        self.next = self.contentsStart

    def __del__(self):
	if self.file:
	    self.close()

    def __init__(self, file):
        """
        Create a FileContainer object.
        
        @param file: an open python file object referencing the file
        container file on disk. If that file is empty (size 0) the
        file container is immediately initialized. A copy of the file
        is retained, so the caller may optionally close it.
        """
        
	# make our own copy of this file which nobody can close underneath us
	self.file = file

	self.file.seek(0, SEEK_END)
	if not self.file.tell():
	    self.file.seek(SEEK_SET, 0)
	    self.file.truncate()
	    self.file.write(FILE_CONTAINER_MAGIC)
	    self.file.write(struct.pack("!I", FILE_CONTAINER_VERSION))

	    self.mutable = True
	else:
	    self.file.seek(0, SEEK_SET)
	    try:
		self.readHeader()
	    except:
		self.file.close()
		self.file = None
		raise
	    self.mutable = False

class BadContainer(conary.errors.ConaryError):

    pass
