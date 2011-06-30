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

"""
Collection of wrappers around file-related C functions.
"""

include "common.pxi"


cdef extern from "unistd.h" nogil:
    int getdtablesize()
    int lstat(char *path, stat *buf)
    int c_fchmod "fchmod"(int fd, int mode)
    int mkdir(char *pathname, int mode)
    int unlink(char *pathname)
    ssize_t c_pread "pread"(int fd, void *buf, size_t count, off_t offset)

cdef extern from "poll.h" nogil:
    cdef struct pollfd:
        int fd
        short events
        short revents
    int poll(pollfd *fds, int nfds, int timeout)
    int POLLIN, POLLPRI, POLLOUT, POLLNVAL


def countOpenFileDescriptors():
    """Return a count of the number of open file descriptors."""
    cdef int maxfd, count, i, rc
    cdef pollfd *ufds

    maxfd = getdtablesize()
    ufds = <pollfd*>malloc(maxfd * sizeof(pollfd))
    if ufds == NULL:
        raise MemoryError

    with nogil:
        for i in range(maxfd):
            ufds[i].fd = i
            ufds[i].events = POLLIN | POLLPRI | POLLOUT

        # Loop until poll() succeeds without being interrupted by a signal
        while True:
            rc = poll(ufds, maxfd, 0)
            if rc >= 0 or errno != EINTR:
                break

    if rc < 0:
        free(ufds)
        PyErr_SetFromErrno(OSError)

    count = 0
    for i in range(maxfd):
        if ufds[i].revents != POLLNVAL:
            count += 1

    free(ufds)
    return count


def fchmod(fobj, int mode):
    """Change the permissions of an open file."""
    cdef int fd, rc
    fd = PyObject_AsFileDescriptor(fobj)
    with nogil:
        rc = c_fchmod(fd, mode)
    if rc == -1:
        PyErr_SetFromErrno(OSError)


def fopenIfExists(char *path, char *mode):
    """Open a file, or return C{None} if opening failed."""
    cdef FILE *fp

    with nogil:
        fp = fopen(path, mode)

    if fp == NULL:
        return None
    else:
        return PyFile_FromFile(fp, path, mode, fclose)


def lexists(char *path):
    """Return C{True} if C{path} exists."""
    cdef stat sb
    if lstat(path, &sb) == -1:
        if errno in (ENOENT, ENOTDIR, ENAMETOOLONG, EACCES):
            return False
        PyErr_SetFromErrnoWithFilename(OSError, path)
    return True


def massCloseFileDescriptors(int start, int count, int end):
    """Close file descriptors from C{start} to either C{end} or after C{count}
    unused descriptors have been encountered."""
    cdef int i, j, rc

    if count and end:
        raise ValueError("Exactly one of count and end must be zero.")

    rc = i = 0
    j = count
    with nogil:
        while True:
            if count:
                # Stopping after a contiguous number of fds
                if j == 0:
                    break
            elif i == end:
                # Stopping at specific value
                break

            rc = close(i)
            if rc == 0:
                # Successful close -- reset contiguous counter
                j = count
            elif errno == EBADF:
                # FD was not in use
                j -= 1
            else:
                # Some other error
                break
            i += 1

    if rc == -1:
        PyErr_SetFromErrno(OSError)


def mkdirIfMissing(char *path):
    """Make a directory at C{path} if it does not exist."""
    if mkdir(path, 0777) == -1:
        if errno == EEXIST:
            return False
        PyErr_SetFromErrnoWithFilename(OSError, path)
    return True


def pread(fobj, size_t count, off_t offset):
    """Read C{count} bytes at C{offset} in file C{fobj}."""
    cdef Py_ssize_t rc
    cdef char *data
    cdef int fd

    fd = PyObject_AsFileDescriptor(fobj)

    data = <char*>malloc(count)
    if data == NULL:
        raise MemoryError

    with nogil:
        rc = c_pread(fd, data, count, offset)

    if rc == -1:
        free(data)
        PyErr_SetFromErrno(OSError)

    ret = PyString_FromStringAndSize(data, rc)
    free(data)
    return ret


def removeIfExists(char *path):
    """Try to unlink C{path}, but don't fail if it doesn't exist."""
    if unlink(path):
        if errno in (ENOENT, ENAMETOOLONG):
            return False
        PyErr_SetFromErrnoWithFilename(OSError, path)
    return True
