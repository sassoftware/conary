#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import os
import string
import __builtin__

class Entry:

    def read(self):
	self.current = self.current + 1
	return self.lines[self.current - 1]

    def write(self, line):
	# this is unfortunate, but will go away when we get a real file 
	# format
	if line[-1] != "\n":
	    raise IOError, "writes to versioned files must end in newline"

	# this write() does a truncate as well. rude maybe?
	del self.lines[self.current:]
	self.lines.append(line)
	self.current = self.current + 1

    def readLines(self):
	# this returns a copy of the lines, not the lines themselves
	l =  self.lines[self.current:]
	self.current = len(self.lines)
	return l

    def readAllLines(self):
	# this returns the actual list of lines, and is used to write
	# the lines back out
	return self.lines

    def seekToStart(self):
	self.current = 0

    def __init__(self, lines):
	self.lines = lines
	self.current = 0

class File:

    def open(self, filename, mode):
	self.filename = filename
	self.entries = {}
	self.readOnly = 0

	if (mode == "r"):
	    self.readOnly = 1

	if (mode == "r+" or mode == "r") and not os.path.exists(filename):
	    return
	elif mode == "w":
	    f = __builtin__.open(filename, "w")
	    f.close()
	    return
	elif mode != "r+" and mode != "r":
	    raise IOError, "bad mode for open %s" % mode

	f = __builtin__.open(filename, "r")

	lines = f.readlines()
	f.close()
	i = 0
	while i < len(lines):
	    tags = string.split(lines[i])
	    if tags[0] != "Version" or len(tags) != 3:
		raise IOError, "invalid versioned file"

	    try:
		lineCount = int(tags[2])
	    except ValueError:
		raise IOError, "invalid versioned file"

	    self.entries[tags[1]] = Entry(lines[i + 1: i + 1 + lineCount])
	    i = i + 1 + lineCount;

    def close(self):
	if self.readOnly: return

	f = __builtin__.open(self.filename, "w")
	for version in self.entries.keys():
	    l = self.entries[version].readAllLines()
	    f.write("Version %s %d\n" % (version, len(l)))
	    s = string.join(l, "")
	    f.write(s)
	f.close()

	del self.entries
	del self.filename

    def setVersion(self, version):
	self.currentEntry = self.entries[version]
	self.currentEntry.seekToStart()

    def createVersion(self, version):
	self.entries[version] = Entry([])
	self.setVersion(version)

    def hasVersion(self, version):
	return self.entries.has_key(version)

    def versionList(self):
	return self.entries.keys()

    def read(self):
	return self.currentEntry.read()

    def readLines(self):
	return self.currentEntry.readLines()

    def write(self, line):
	if self.readOnly:
	    raise IOError, "cannot write to a read-only file"

	return self.currentEntry.write(line)

    def __init__(self, filename, mode):
	return self.open(filename, mode);

def open(filename, mode):
    return File(filename, mode)

def latest(versionList):
    # for now the lastest version is the last in this list

    list = versionList[:]
    list.sort()
    return list[-1]


