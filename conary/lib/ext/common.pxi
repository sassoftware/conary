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
