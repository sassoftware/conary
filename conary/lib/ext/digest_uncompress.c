/*
 * Copyright (c) SAS Institute Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <Python.h>

#include <errno.h>
#include <malloc.h>
#include <openssl/sha.h>
#include <stdint.h>
#include <sys/param.h>
#include <zlib.h>

#include "pycompat.h"

static PyObject * sha1Copy(PyObject *self, PyObject *args);
static PyObject * sha1Uncompress(PyObject *self, PyObject *args);

static PyMethodDef methods[] = {
    { "sha1Copy", sha1Copy, METH_VARARGS },
    { "sha1Uncompress", sha1Uncompress, METH_VARARGS,
        "Uncompresses a gzipped file descriptor into another gzipped "
        "file descriptor and returns the sha1 of the uncompressed content. " },
    {NULL}  /* Sentinel */
};


/* sha1Copy - Copy a compressed stream from one file descriptor to a list of
 * file descriptors, and compute a SHA-1 digest of the decompressed contents.
 */
static PyObject * sha1Copy(PyObject *module, PyObject *args) {
    off_t inFd, inSize, inStart, inStop, inAt, to_read, to_write, to_write2;
    PyObject * outFdList, *pyInStart, *pyInSize;
    int * outFds, outFdCount, i, rc, inflate_rc;
    uint8_t inBuf[1024 * 256];
    uint8_t *inBuf_p;
    uint8_t outBuf[1024 * 256];
    SHA_CTX sha1state;
    z_stream zs;
    uint8_t sha1[20];

    if (!PyArg_ParseTuple(args, "(iOO)O!", &inFd, &pyInStart, &pyInSize,
                          &PyList_Type, &outFdList ))
        return NULL;
    if (!PYINT_CHECK_EITHER(pyInStart)) {
        PyErr_SetString(PyExc_TypeError, "second item in first argument must be an int or long");
        return NULL;
    }
    if (!PYINT_CHECK_EITHER(pyInSize)) {
        PyErr_SetString(PyExc_TypeError, "third item in first argument must be an int or long");
        return NULL;
    }

    inStart = PYLONG_AS_ULL(pyInStart);
    if (PyErr_Occurred())
        return NULL;

    inSize = PYLONG_AS_ULL(pyInSize);
    if (PyErr_Occurred())
        return NULL;

    outFdCount = PyList_Size(outFdList);
    outFds = alloca(sizeof(*outFds) * outFdCount);
    for (i = 0; i < outFdCount; i++)
        outFds[i] = PYINT_AS_LONG(PyList_GET_ITEM(outFdList, i));

    memset(&zs, 0, sizeof(zs));
    if ((rc = inflateInit2(&zs, 31)) != Z_OK) {
        PyErr_SetString(PyExc_RuntimeError, zError(rc));
        return NULL;
    }

    SHA1_Init(&sha1state);

    inStop = inSize + inStart;
    inAt = inStart;
    inflate_rc = 0;
    while (inflate_rc != Z_STREAM_END) {
        if (!zs.avail_in) {
            /* read */
            to_read = MIN(sizeof(inBuf), inStop - inAt);
            rc = pread(inFd, inBuf, to_read, inAt);
            if (rc < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                return NULL;
            }
            to_write = rc;
            inAt += rc;

            /* copy (still compressed) */
            for (i = 0; i < outFdCount; i++) {
                inBuf_p = inBuf;
                to_write2 = to_write;
                while (to_write2 > 0) {
                    rc = write(outFds[i], inBuf, to_write2);
                    if (rc < 0) {
                        PyErr_SetFromErrno(PyExc_OSError);
                        return NULL;
                    }
                    inBuf_p += rc;
                    to_write2 -= rc;
                }
            }

            /* feed to inflate */
            zs.avail_in = to_write;
            zs.next_in = inBuf;
        }

        /* inflate */
        zs.avail_out = sizeof(outBuf);
        zs.next_out = outBuf;
        if ((inflate_rc = inflate(&zs, 0)) < 0) {
            PyErr_SetString(PyExc_RuntimeError, zError(inflate_rc));
            return NULL;
        }

        /* digest */
        to_write = sizeof(outBuf) - zs.avail_out;
        SHA1_Update(&sha1state, outBuf, to_write);
    }

    if ((rc = inflateEnd(&zs)) != Z_OK) {
        PyErr_SetString(PyExc_RuntimeError, zError(rc));
        return NULL;
    }

    SHA1_Final(sha1, &sha1state);

    return PYBYTES_FromStringAndSize((char*)sha1, sizeof(sha1));
}


/* sha1Uncompress - Decompress a stream from a file descriptor to a new file
 * and simultaneously compute a SHA-1 digest of the decompressed contents.
 */
static PyObject * sha1Uncompress(PyObject *module, PyObject *args) {
    int inFd, outFd = -1, rc, inflate_rc;
    off_t inStop, inAt, inSize, inStart, to_read, to_write;
    PyObject *pyInStart, *pyInSize;
    z_stream zs;
    uint8_t inBuf[1024 * 256];
    uint8_t outBuf[1024 * 256];
    uint8_t *outBuf_p;
    SHA_CTX sha1state;
    uint8_t sha1[20];
    char * path, * baseName;
    struct stat sb;
    char * tmpPath = NULL, * targetPath;

    if (!PyArg_ParseTuple(args, "(iOO)sss", &inFd, &pyInStart, &pyInSize,
			  &path, &baseName, &targetPath))
        goto onerror;

    if (!PYINT_CHECK_EITHER(pyInStart)) {
        PyErr_SetString(PyExc_TypeError, "second item in first argument must be an int or long");
        goto onerror;
    }
    if (!PYINT_CHECK_EITHER(pyInSize)) {
        PyErr_SetString(PyExc_TypeError, "third item in first argument must be an int or long");
        goto onerror;
    }

    inStart = PYLONG_AS_ULL(pyInStart);
    if (PyErr_Occurred())
        goto onerror;

    inSize = PYLONG_AS_ULL(pyInSize);
    if (PyErr_Occurred())
        goto onerror;

    tmpPath = alloca(strlen(path) + strlen(baseName) + 10);
    sprintf(tmpPath, "%s/.ct%sXXXXXX", path, baseName);
    outFd = mkstemp(tmpPath);
    if (outFd == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto onerror;
    }

    memset(&zs, 0, sizeof(zs));
    if ((rc = inflateInit2(&zs, 31)) != Z_OK) {
        PyErr_SetString(PyExc_RuntimeError, zError(rc));
        goto onerror;
    }

    SHA1_Init(&sha1state);

    inStop = inSize + inStart;
    inAt = inStart;

    inflate_rc = 0;
    while (inflate_rc != Z_STREAM_END) {
        if (!zs.avail_in) {
            /* read */
            to_read = MIN(sizeof(inBuf), inStop - inAt);
            rc = pread(inFd, inBuf, to_read, inAt);
            if (rc < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                goto onerror;
            } else if (rc == 0) {
                PyErr_SetString(PyExc_RuntimeError, "short read");
                goto onerror;
            }
            inAt += rc;
            zs.avail_in = rc;
            zs.next_in = inBuf;
        }

        /* inflate */
        zs.avail_out = sizeof(outBuf);
        zs.next_out = outBuf;
        inflate_rc = inflate(&zs, 0);
        if (inflate_rc < 0) {
            PyErr_SetString(PyExc_RuntimeError, zError(rc));
            goto onerror;
        }

        /* digest */
        to_write = sizeof(outBuf) - zs.avail_out;
        SHA1_Update(&sha1state, outBuf, to_write);

        /* copy */
        outBuf_p = outBuf;
        while (to_write > 0) {
            rc = write(outFd, outBuf_p, to_write);
            if (rc < 0) {
                PyErr_SetFromErrno(PyExc_OSError);
                goto onerror;
            }
            to_write -= rc;
            outBuf_p += rc;
        }
    }

    if ((rc = inflateEnd(&zs)) != Z_OK) {
        PyErr_SetString(PyExc_RuntimeError, zError(rc));
        goto onerror;
    }

    SHA1_Final(sha1, &sha1state);

    if (close(outFd)) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto onerror;
    }
    outFd = -1;

    rc = lstat(targetPath, &sb);
    if (rc && (errno != ENOENT && errno != ELOOP)) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto onerror;
    } else if (!rc && S_ISDIR(sb.st_mode)) {
        if (rmdir(targetPath)) {
            PyErr_SetFromErrno(PyExc_OSError);
            goto onerror;
        }
    }

    if (rename(tmpPath, targetPath)) {
        PyErr_SetFromErrno(PyExc_OSError);
        goto onerror;
    }

    return PYBYTES_FromStringAndSize((char*)sha1, sizeof(sha1));

onerror:
    if (outFd != -1)
        close(outFd);
    if (tmpPath != NULL)
        unlink(tmpPath);
    return NULL;
}


PYMODULE_DECLARE(digest_uncompress, methods, "Accelerated functions for handling file contents");

/* vim: set sts=4 sw=4 expandtab : */
