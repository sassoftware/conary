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

import sha
import md5
import os
import stat
import struct

def sha1FileBin(path):
    oldmode = None
    mode = os.lstat(path)[stat.ST_MODE]
    if (mode & 0400) != 0400:
        oldmode = mode
        os.chmod(path, mode | 0400)

    fd = os.open(path, os.O_RDONLY)
    if oldmode is not None:
        os.chmod(path, oldmode)

    m = sha.new()
    buf = os.read(fd, 40960)
    while len(buf):
	m.update(buf)
	buf = os.read(fd, 40960)
    os.close(fd)

    return m.digest()

def sha1String(buf):
    m = sha.new()
    m.update(buf)
    return m.digest()

def md5String(buf):
    m = md5.new()
    m.update(buf)
    return m.digest()

def sha1ToString(buf):
    assert(len(buf) == 20)
    return "%08x%08x%08x%08x%08x" % struct.unpack("!5I", buf)

def sha1FromString(val):
    assert(len(val) == 40)
    return struct.pack("!5I", int(val[ 0: 8], 16), 
			int(val[ 8:16], 16), int(val[16:24], 16), 
			int(val[24:32], 16), int(val[32:40], 16))

def md5ToString(buf):
    assert(len(buf) == 16)
    return "%08x%08x%08x%08x" % struct.unpack("!4I", buf)

def md5FromString(val):
    assert(len(val) == 32)
    return struct.pack("!4I", int(val[ 0: 8], 16), 
			int(val[ 8:16], 16), int(val[16:24], 16), 
			int(val[24:32], 16))
