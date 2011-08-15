#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


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
