#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
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
  - total # of bytes in file tables
  - file table entry 1
  - file table entry 2
  -                  .
  -                  .
  -                  .
  - file table entry N
  - file 1
  - file 2
  .
  .
  .
  - file N

Everything after the file format version is gzipped (so gzip magic
appears right after the file version).

Each file table entry looks like::

  lengt
  of entry (4 bytes), not including these 4 bytes
  length of file name (4 bytes)
  file name 
  file offset (4 bytes) (in the gzipped data)
  file size (4 bytes)
  length of arbitrary data (4 bytes)
  entries in file table (4 bytes)
  arbitrary file table data
"""

import gzip
import os
import string
import struct
import sys
import types
import util

FILE_CONTAINER_MAGIC = "\xEA\x3F\x81\xBB"
FILE_CONTAINER_VERSION = 1
SEEK_SET = 0
SEEK_CUR = 1
SEEK_END = 2

class FileTableEntry:

    def write(self, file):
	rc = (struct.pack("!i", len(self.name)) + self.name +
	      struct.pack("!i", self.offset) +
	      struct.pack("!i", self.size) +
	      struct.pack("!i", len(self.data)) + self.data)
	rc = struct.pack("!i", len(rc)) + rc
	assert(len(rc) == self.tableSize())
	return file.write(rc)

    def tableSize(self):
	return len(self.name) + len(self.data) + 20 

    def setOffset(self, new):
	self.offset = new

    def writeContents(self, dest):
	util.copyfileobj(self.src, dest)

    def __init__(self, name, offset, size, data, src):
	self.offset = offset
	self.size = size
	self.name = name
	self.data = data
	self.src = src

class FileTableEntryFromFile(FileTableEntry):

    def __init__(self, f):
	# read the length of the entry
	size = f.read(4)
	(size,) = struct.unpack("!i", size)

	rest = f.read(size)

	# unpack the length of the file name
	(size,) = struct.unpack("!i", rest[0:4])
	i = 4
	# and the file name
	(self.name,) = struct.unpack("%ds" % size, rest[i:i+size])
	i = i + size
	# and the file offset
	(self.offset,) = struct.unpack("!i", rest[i:i+4])
	i = i+ 4
	# and the file size
	(self.size,) = struct.unpack("!i", rest[i:i+4])
	i = i+ 4
	# the length of the arbitrary data
	(size,) = struct.unpack("!i", rest[i:i+4])
	i = i + 4
	# and the arbitrary data
	(self.data,) = struct.unpack("%ds" % size, rest[i:])

class FileContainerFile:

    def close(self):
	pass

    def read(self, bytes = -1):
	if bytes < 0 or (self.end - self.pos) <= bytes:
	    # return the rest of the file
	    count = self.end - self.pos
	    self.pos = self.end
	    return self.file.read(count)
	else:
	    self.pos = self.pos + bytes
	    return self.file.read(bytes)

    def readlines(self):
	list = self.read().split('\n')
	list2 = []

	# cut off the last element (which wasn't newline terminated anyway)
	for item in list[:-1]:
	    list2.append(item + "\n")
	return list2

    def __init__(self, file, size):
	self.file = file
	self.size = size
	self.end = self.size
	self.pos = 0

class FileContainer:

    def readTable(self):
	magic = self.file.read(4)
	if len(magic) != 4 or magic != FILE_CONTAINER_MAGIC:
	    raise KeyError, "bad file container magic"

	version = self.file.read(2)
	if len(version) != 2:
	    raise KeyError, "bad file container version"
	version = struct.unpack("!H", version)[0]
	if version != FILE_CONTAINER_VERSION:
	    raise KeyError, "unknown file container version %d" % version

	self.gzfile = gzip.GzipFile(None, "rb", None, self.file)

	tableLen = self.gzfile.read(8)
	(tableLen, entryCount) = struct.unpack("!ii", tableLen)

	while (entryCount):
	    entry = FileTableEntryFromFile(self.gzfile)
	    self.entries[entry.name] = entry
	    entryCount = entryCount - 1

	self.contentsStart = self.gzfile.tell()

    def close(self):
	if not self.mutable:
	    self.file = None
	    return

	self.file.seek(SEEK_SET, 0)
	self.file.truncate()
	self.file.write(FILE_CONTAINER_MAGIC)
	self.file.write(struct.pack("!H", FILE_CONTAINER_VERSION))

	rest = gzip.GzipFile(None, "wb", 9, self.file)

	count = len(self.entries.keys())

	size = 0
	offset = 0			# relative to the end of the table
	for name in self.order:
	    entry = self.entries[name]
	    entry.setOffset(offset)
	    size = entry.tableSize()
	    offset += entry.size

        rest.write(struct.pack("!ii", size, count))

	for name in self.order:
	    entry = self.entries[name]
	    entry.write(rest)

	for name in self.order:
	    entry = self.entries[name]
	    entry.writeContents(rest)

	rest.close()
	self.file = None
    
    def addFile(self, fileName, fileObj, tableData, fileSize):
	assert(self.mutable)
	entry = FileTableEntry(fileName, -1, fileSize, tableData, fileObj)
	self.entries[fileName] = entry
	self.order.append(fileName)

    def getFile(self, fileName):
	# returns a file-ish object for accessing a member of the
	# container in read-only mode. the object provides a very
	# small number of functions
	assert(not self.mutable)

	if not self.entries.has_key(fileName):
	    raise KeyError, ("file %s is not in the collection") % fileName

	entry = self.entries[fileName]

	pos = self.gzfile.tell()
	offset = entry.offset + self.contentsStart
	if (pos != offset):
	    assert(pos < offset)
	    self.gzfile.seek(offset)	    # this is always SEEK_SET
	    assert(self.gzfile.tell() == offset)

	return FileContainerFile(self.gzfile, entry.size)

    def getSize(self, fileName):
	return self.entries[fileName].size

    def getTag(self, fileName):
	if not self.entries.has_key(fileName):
	    raise KeyError, ("file %s is not in the collection") % fileName
	return self.entries[fileName].data

    def hasFile(self, hash):
	return self.entries.has_key(hash)

    def __del__(self):
	if self.file:
	    self.close()

    def iterFileList(self):
	return self.entries.iterkeys()

    # file is a python file object which refers to the file container
    # if that file is empty (size 0) the file container is immediately
    # initialized. we make our own copy of the file so the caller can
    # close it if they like
    def __init__(self, file):
	# make our own copy of this file which nobody can close underneath us
	self.file = os.fdopen(os.dup(file.fileno()), file.mode)

	self.file.seek(0, SEEK_END)
	self.entries = {}
	if not self.file.tell():
	    self.order = []
	    self.mutable = True
	else:
	    self.file.seek(0, SEEK_SET)
	    try:
		self.readTable()
	    except:
		self.file.close()
		self.file = None
		raise
	    self.mutable = False
	

