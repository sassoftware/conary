#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import lib.elf
import os
import string
import struct

class Magic:
    def __init__(self, path, basedir):
	self.path = path
	self.basedir = basedir
	self.contents = {}
	self.name = self.__class__.__name__


class ELF(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
	self.contents['stripped'] = lib.elf.stripped(basedir+path)


class ar(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
	# no point in looking for __.SYMDEF because GNU ar always keeps
	# symbol table up to date

	# FIXME: ewt will write code to determine if ar archive
	# has any unstripped elements; will be part of elf module


class gzip(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
	if _char(buffer[3]) == 0x8:
	    self.contents['name'] = _string(buffer[10:])
	if buffer[8] == 4:
	    self.contents['compression'] = '9'
	else:
	    self.contents['compression'] = '1'


class bzip(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
	self.contents['compression'] = buffer[3]


def magic(path, basedir=''):
    """
    Returns a magic class with information about the file mentioned
    """
    if basedir and not basedir.endswith('/'):
	basefir += '/'
    f = file(basedir+path)
    b = f.read(4096)
    f.close()
    if _char(b[0]) == 0x7f and b[1:4] == "ELF":
	return ELF(path, basedir, b)
    elif b[0:6] == "!<arch>":
	return ar(path, basedir, b)
    elif _char(b[0]) == 0x1f and _char(b[1]) == 0x8b:
	return gzip(path, basedir, b)
    elif b[0:2] == "BZh":
	return bzip(path, basedir, b)

# internal helpers

def _string(buffer):
    return buffer[:string.find(buffer, '\0')]

def _char(c):
    return struct.unpack("B", c)[0]
