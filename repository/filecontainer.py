#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

# provides a file which can store multiple files inside of it
# a table of contents is utilized, and the caller can attach arbitrary
# data to an entry in the table. no compression is used, but that could
# be changed
#
# the data attached to table entries can be modified, but doing so isn't
# particularly efficient (the entire table gets rewritten)
#
# the last file added to the container can be erased from it. this is
# to allow operations to be reverted

# the file format is:
#
# - magic
# - file 1
# - file 2
# .
# .
# .
# - file N
# - file table entry 1
# - file table entry 2
# -                  .
# -                  .
# -                  .
# - file table entry N
# total # of bytes in file tables
#
# each file table entry looks like:
#   length of entry (4 bytes), not including these 4 bytes
#   length of file name (4 bytes)
#   file name 
#   file offset (4 bytes)
#   file size (4 bytes)
#   length of arbitrary data (4 bytes)
#   entries in file table (4 bytes)
#   arbitrary file table data
#
# this could, and probably should, be reimplemented using tar as the 
# underlying format and the file table stored as a magic file at the end

import struct
import os
import sys
import types
import string

FILE_CONTAINER_MAGIC = "\xEA\x3F\x81\xBB"

# this file doesn't let itself get closed until it's ref count goes
# to zero; normal files don't do this, and we don't want our FileIsh
# objects to disappear when the FileContainer does. This only bothers
# with the methods we happen to care about here.
class PersistentFile:

    def close(self):
	# ho-hum
	pass

    def seek(self, pos, whence = 0):
	return self.f.seek(pos, whence)

    def __del__(self):
	self.f.close()

    def read(self, bytes = -1):
	return self.f.read(bytes)

    def __init__(self, f):
	self.f = f

class FileTableEntry:

    def write(self, file):
	rc = struct.pack("!i", len(self.name)) + self.name
        rc += struct.pack("!i", self.offset)
        rc += struct.pack("!i", self.size)
	rc += struct.pack("!i", len(self.data)) + self.data
	l = len(rc)
	rc = struct.pack("!i", l) + rc
	return file.write(rc)

    def __init__(self, name, offset, size, data):
	self.offset = offset
	self.size = size
	self.name = name
	self.data = data

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
	del self.file

    def seek(self, count, whence = 0):
	if whence == 0:
	    self.pos = self.beginning + count
	elif whence == 1:
	    self.pos = self.pos + count
	elif whence == 2:
	    self.pos = self.end + count
	else:
	    raise IOError, "invalid whence for seek"

	if self.pos < self.beginning:
	    self.pos = self.beginning
	elif self.pos > self.end:
	    self.pos = self.end

    def read(self, bytes = -1):
	if bytes < 0 or (self.end - self.pos) <= bytes:
	    # return the rest of the file
	    count = self.end - self.pos
	    self.file.seek(self.pos)
	    self.pos = self.end
	    return self.file.read(count)
	else:
	    self.pos = self.pos + bytes
	    self.file.seek(self.pos)
	    return self.file.read(count)

    def readlines(self):
	list = self.read().split('\n')
	list2 = []

	# cut off the last element (which wasn't newline terminated anyway)
	for item in list[:-1]:
	    list2.append(item + "\n")
	return list2
	    
    def __init__(self, file, offset, size):
	self.file = file
	self.beginning = offset
	self.size = size
	self.end = self.beginning + self.size
	self.pos = self.beginning

class FileContainer:

    # only called when self.file refers to an empty file
    def initializeTable(self):
	rc = FILE_CONTAINER_MAGIC

	# length of the table, in bytes
	rc = rc + struct.pack("!ii", 0, 0)

	self.file.write(rc)
	self.file.flush()
	self.tableOffset = 4

    def readTable(self):
	# seeks 4 bytes from the end of the file where the length
	# of the table is stored
	self.file.seek(-8, 2)
	tableLen = self.file.read(8)
	(tableLen, entryCount) = struct.unpack("!ii", tableLen)

	# seek to the start of the file table
	self.file.seek(-8 - tableLen, 2)
	self.tableOffset = self.file.tell()
	while (entryCount):
	    entry = FileTableEntryFromFile(self.file)
	    self.entries[entry.name] = entry
	    entryCount = entryCount - 1

    def writeTable(self, newEntry = None):
	# writes out the table of contents at the current
	# position in the file (which should be the end!), including
	# newEntry if it's defined
	#
	# this should be used carefully w/ a proper try/finally outside
	# of the invocation to prevent corruption
	pos = self.file.tell()
	for entry in self.entries.values():
	    entry.write(self.file)
	count = len(self.entries.keys())
	if newEntry:
	    newEntry.write(self.file)
	    count = count + 1
	newpos = self.file.tell()
	size = newpos - pos
        rc = struct.pack("!ii", size, count)
	self.file.write(rc)
	self.file.flush()
	self.tableOffset = pos
    
    # adds a new file to the container
    # if data is a string, it's used as the data for the file
    # if it's not a string, it should be a file and the entire
    # contents of that file are copied into the container 
    def addFile(self, fileName, data, tableData):
	try:
	    self.file.seek(self.tableOffset)
	    size = 0
	    if type(data) == types.StringType:
		self.file.write(data)
		size = len(data)
	    else:
		buf = data.read(1024 * 64)
		while len(buf):
		    size = size + len(buf)
		    self.file.write(buf)
		    buf = data.read(1024 * 64)

	    entry = FileTableEntry(fileName, self.tableOffset, size, tableData)

	    self.writeTable(entry)
	    self.entries[fileName] = entry
	except:
	    # if something went wrong revert the change
	    self.file.seek(self.tableOffset)
	    self.writeTable()
	    self.file.truncate()

	    e = sys.exc_info()
	    raise e[0], e[1], e[2]


    def delFile(self, fileName):
	# Erases the file named fileName from the container if it's the
	# last file. if it's not, an exception is generated. There is no
	# way to know what the last file in the container is; this is meant
	# solely to allow changes to be reverted if other parts of a
	# transaction fail
	offset = self.entries[fileName].offset
	size = self.entries[fileName].size
	if (offset + size) != self.tableOffset:
	    raise IOError, "only the last file may be removed"

	self.file.seek(offset)
	del self.entries[fileName]
	self.writeTable()
	self.file.truncate()

    def getFile(self, fileName):
	# returns a file-ish object for accessing a member of the
	# container in read-only mode. the object provides a very
	# small number of functions
	if not self.entries.has_key(fileName):
	    raise KeyError, ("file %s is not in the collection") % fileName
	entry = self.entries[fileName]
	return FileContainerFile(self.persist, entry.offset, entry.size)

    def getTag(self, fileName):
	if not self.entries.has_key(fileName):
	    raise KeyError, ("file %s is not in the collection") % fileName
	return self.entries[fileName].data

    def hasFile(self, hash):
	return self.entries.has_key(hash)

    def updateTag(self, fileName, newTag):
	if not self.entries.has_key(fileName):
	    raise KeyError, ("file %s is not in the collection") % fileName
	self.entries[fileName].data = newTag
	self.writeTable()
	
    # existing file objects coninue to work
    def close(self):
	# we don't close the file; we let the destructor of self.persist
	# do that for us
	self.file = None
	del self.persist

    def __del__(self):
	if self.file:
	    self.close()

    def fileList(self):
	return self.entries.keys()

    # file is a python file object which refers to the file container
    # if that file is empty (size 0) the file container is immediately
    # initialized. we make our own copy of the file so the caller can
    # close it if they like
    def __init__(self, file):
	# make our own copy of this file which nobody can close underneath us
	self.file = os.fdopen(os.dup(file.fileno()), file.mode)
	# this keeps us from closing the file
	self.persist = PersistentFile(self.file)

	self.file.seek(0, 2)
	self.entries = {}
	if not self.file.tell():
	    self.initializeTable()
	else:
	    # check the magic
	    self.file.seek(0)
	    magic = self.file.read(4)
	    if magic != FILE_CONTAINER_MAGIC:
		self.file.close()
		raise KeyError, "bad file container magic"
	    self.readTable()
	

