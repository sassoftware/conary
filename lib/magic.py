#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import lib.elf
import os
import string

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
	self.contents['hasDebug'] = lib.elf.hasDebug(basedir+path)


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
	if buffer[3] == '\x08':
	    self.contents['name'] = _string(buffer[10:])
	if buffer[8] == '\x02':
	    self.contents['compression'] = '9'
	else:
	    self.contents['compression'] = '1'


class bzip(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
	self.contents['compression'] = buffer[3]


class ltwrapper(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)


def magic(path, basedir=''):
    """
    Returns a magic class with information about the file mentioned
    """
    if basedir and not basedir.endswith('/'):
	basedir += '/'

    n = basedir+path
    if os.path.isdir(n) or os.path.islink(n):
	return None

    f = file(n)
    b = f.read(4096)
    f.close()
    if len(b) > 4 and b[0] == '\x7f' and b[1:4] == "ELF":
	return ELF(path, basedir, b)
    elif len(b) > 7 and b[0:7] == "!<arch>":
	return ar(path, basedir, b)
    elif len(b) > 2 and b[0] == '\x1f' and b[1] == '\x8b':
	return gzip(path, basedir, b)
    elif len(b) > 3 and b[0:3] == "BZh":
	return bzip(path, basedir, b)
    elif b.find(
	'# This wrapper script should never be moved out of the build directory.\n'
	'# If it is, it will not operate correctly.') > 0:
	return ltwrapper(path, basedir, b)

    return None

# internal helpers

def _string(buffer):
    return buffer[:string.find(buffer, '\0')]
