#
# Copyright (c) 2011 rPath, Inc.
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
Pure Python implementations of functions from file_utils.pyx. The latter is
considered the authoritative implementation.

This need not work on Python older than version 2.6, so use of ctypes is
acceptable.
"""

import ctypes
import errno
import os
from conary.lib.ext import ctypes_utils
from ctypes import c_int, c_size_t, c_long, c_void_p


def _fileno(fobj):
    if isinstance(fobj, (int, long)):
        return fobj
    else:
        try:
            return fobj.fileno()
        except AttributeError:
            raise TypeError("Expected a file descriptor or object "
                    "implementing fileno()")


def countOpenFileDescriptors():
    raise NotImplementedError


def fchmod(fobj, mode):
    os.fchmod(_fileno(fobj), mode)


def fopenIfExists(path, mode):
    try:
        return open(path, mode)
    except IOError:
        return None


def lexists(path):
    try:
        os.lstat(path)
    except OSError, err:
        if err.errno in (errno.ENOENT, errno.ENOTDIR, errno.ENAMETOOLONG,
                errno.EACCES):
            return False
        raise
    else:
        return True


def massCloseFileDescriptors(start, count, end):
    to_close = count
    for i in range(start, end):
        if count:
            # Stopping after a contiguous number of fds
            if to_close == 0:
                break
        elif i == end:
            # Stopping at specific value
            break

        try:
            os.close(i)
        except OSError, err:
            if err.errno == errno.EBADF:
                # FD was not in use
                to_close -= 1
            raise
        else:
            # Successful close -- reset contiguous counter
            to_close = count


def mkdirIfMissing(path):
    try:
        os.mkdir(path, 0777)
    except OSError, err:
        if err.errno == errno.EEXIST:
            return False
        raise
    else:
        return True


def pread(fobj, count, offset):
    buf = ctypes.create_string_buffer(count)
    libc = ctypes_utils.get_libc()
    libc.pread.argtypes = (c_int, c_void_p, c_size_t, c_long)
    libc.pread.restype = c_int
    rc = libc.pread(_fileno(fobj), buf, count, offset)
    if rc < 0:
        ctypes_utils.throw_errno(libc)
    else:
        return buf[:rc]


def removeIfExists(path):
    try:
        os.unlink(path)
    except OSError, err:
        if err.errno in (errno.ENOENT, errno.ENAMETOOLONG):
            return False
        raise
    else:
        return True
