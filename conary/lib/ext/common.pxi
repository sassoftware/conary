#
# Copyright (c) 2010 rPath, Inc.
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

"""Common definitions used by many modules."""


cdef extern from "sys/types.h" nogil:
    cdef struct stat:
        pass
    ctypedef unsigned long size_t
    ctypedef long ssize_t
    ctypedef long off_t

cdef extern from "errno.h" nogil:
    int errno

    int EACCES
    int EAGAIN
    int EBADF
    int EEXIST
    int EINTR
    int ENAMETOOLONG
    int ENOENT
    int ENOTDIR

cdef extern from "stdio.h" nogil:
    ctypedef struct FILE:
        pass
    FILE *fopen(char *path, char *mode)
    int fclose(FILE *fp)

cdef extern from "stdlib.h" nogil:
    void *malloc(size_t size)
    void free(void *ptr)

cdef extern from "alloca.h" nogil:
    void *alloca(size_t size)

cdef extern from "unistd.h" nogil:
    int close(int fd)

cdef extern from "Python.h":
    object PyErr_SetFromErrno(exc)
    object PyErr_SetFromErrnoWithFilename(exc, char *filename)
    object PyFile_FromFile(FILE *fp, char *name, char *mode, int (*close)(FILE*))
    int PyObject_AsFileDescriptor(obj) except -1
    object PyString_FromStringAndSize(char *str, Py_ssize_t size)
    int PyString_AsStringAndSize(object str, char **out, Py_ssize_t *size) except -1


# vim: filetype=pyrex
