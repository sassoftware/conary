#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import elf
import os
import re
import stat
import string
import util

class Magic:
    def __init__(self, path, basedir):
	self.path = path
	self.basedir = basedir
	self.contents = {}
	self.name = self.__class__.__name__


class ELF(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
        fullpath = basedir+path
	self.contents['stripped'] = elf.stripped(fullpath)
        if self.__class__ is ELF:
            # ar doesn't deal with hasDebug or RPATH
            self.contents['hasDebug'] = elf.hasDebug(fullpath)
            self.contents['RPATH'] = elf.getRPATH(fullpath)
            self.contents['Type'] = elf.getType(fullpath)
	requires, provides = elf.inspect(fullpath)
        self.contents['requires'] = requires
        self.contents['provides'] = provides
        for req in requires:
            if req[0] == 'abi':
                self.contents['abi'] = req[1:]
        for prov in provides:
            if prov[0] == 'soname':
                self.contents['soname'] = prov[1]

class ar(ELF):
    def __init__(self, path, basedir='', buffer=''):
	ELF.__init__(self, path, basedir)
	# no point in looking for __.SYMDEF because GNU ar always keeps
	# symbol table up to date
        # ar archives, like ELF files, are investigated by our elf module.
        # We do still want to be able to distinguish between them via magic,
        # thus the two classes.


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


class changeset(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)


class script(Magic):
    interpreterRe = re.compile(r'^#!\s*([^\s]*)')
    lineRe = re.compile(r'^#!\s*(.*)')
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)
        m = self.interpreterRe.match(buffer)
        self.contents['interpreter'] = m.group(1)
        m = self.lineRe.match(buffer)
        self.contents['line'] = m.group(1)


class ltwrapper(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)


class CIL(Magic):
    def __init__(self, path, basedir='', buffer=''):
	Magic.__init__(self, path, basedir)


def magic(path, basedir=''):
    """
    Returns a magic class with information about the file mentioned
    """
    if basedir and not basedir.endswith('/'):
	basedir += '/'

    n = basedir+path
    if not util.isregular(n):
	return None

    oldmode = None
    mode = os.lstat(n)[stat.ST_MODE]
    if (mode & 0400) != 0400:
        oldmode = mode
        os.chmod(n, mode | 0400)

    f = file(n)
    if oldmode is not None:
        os.chmod(n, oldmode)

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
    elif len(b) > 4 and b[0:4] == "\xEA\x3F\x81\xBB":
	return changeset(path, basedir, b)
    elif len(b) > 4 and b[0:2] == "#!":
        if b.find(
            '# This wrapper script should never be moved out of the build directory.\n'
            '# If it is, it will not operate correctly.') > 0:
            return ltwrapper(path, basedir, b)
        return script(path, basedir, _line(b))
    elif (len(b) > 130
          and b[0:2] == 'MZ'
          and b[78:117] == "This program cannot be run in DOS mode."
          and b[128:130] == "PE"):
        # FIXME - this is not sufficient to detect a CIL file this
        # will match all PE executables.  See ECMA-335, partition ii,
        # section 25
        return CIL(path, basedir, b)

    return None

class magicCache(dict):
    def __init__(self, basedir=''):
	self.basedir = basedir
    def __getitem__(self, name):
	if name not in self:
	    self[name] = magic(name, self.basedir)
	return dict.__getitem__(self, name)

# internal helpers

def _string(buffer):
    return buffer[:string.find(buffer, '\0')]

def _line(buffer):
    return buffer[:string.find(buffer, '\n')]
