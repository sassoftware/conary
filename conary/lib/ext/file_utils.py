#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
    # minus one for the handle used to list the directory
    return len(os.listdir('/proc/self/fd')) - 1


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
    i = start
    while True:
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
            else:
                raise
        else:
            # Successful close -- reset contiguous counter
            to_close = count
        i += 1


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
    if offset >= 0x8000000000000000:
        raise OverflowError
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
