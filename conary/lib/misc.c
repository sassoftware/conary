/*
 * Copyright (c) 2011 rPath, Inc.
 *
 * This program is distributed under the terms of the Common Public License,
 * version 1.0. A copy of this license should have been distributed with this
 * source file in a file called LICENSE. If it is not present, the license
 * is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * without any warranty; without even the implied warranty of merchantability
 * or fitness for a particular purpose. See the Common Public License for
 * full details.
 *
 */

#include <Python.h>

#include <dlfcn.h>
#include <errno.h>
#include <malloc.h>
#include <netinet/in.h>
#include <openssl/sha.h>
#include <resolv.h>
#include <zlib.h>

#include "pycompat.h"

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static PyObject * sha1Copy(PyObject *self, PyObject *args);
static PyObject * sha1Uncompress(PyObject *self, PyObject *args);
static PyObject * py_sendmsg(PyObject *self, PyObject *args);
static PyObject * py_recvmsg(PyObject *self, PyObject *args);
static PyObject * py_res_init(PyObject *self, PyObject *args);
static PyObject * rpmExpandMacro(PyObject *self, PyObject *args);

static PyMethodDef MiscMethods[] = {
    { "sha1Copy", sha1Copy, METH_VARARGS },
    { "sha1Uncompress", sha1Uncompress, METH_VARARGS,
        "Uncompresses a gzipped file descriptor into another gzipped "
        "file descriptor and returns the sha1 of the uncompressed content. " },
    { "sendmsg", py_sendmsg, METH_VARARGS },
    { "recvmsg", py_recvmsg, METH_VARARGS },
    { "res_init", py_res_init, METH_VARARGS },
    { "rpmExpandMacro", rpmExpandMacro, METH_VARARGS },
    {NULL}  /* Sentinel */
};


static PyObject * py_sendmsg(PyObject *self, PyObject *args) {
    PyObject * fdList, * dataList, * intObj, * sObj;
    struct msghdr msg;
    struct cmsghdr * ctrlMsg;
    int fd, i, bytes;
    struct iovec * vectors;
    int * sendFds;

    if (!PyArg_ParseTuple(args, "iOO", &fd, &dataList, &fdList))
        return NULL;

    if (!PyList_CheckExact(dataList)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a list");
        return NULL;
    }

    if (!PyList_CheckExact(fdList)) {
        PyErr_SetString(PyExc_TypeError, "third argument must be a list");
        return NULL;
    }

    vectors = alloca(sizeof(*vectors) * PyList_GET_SIZE(dataList));
    for (i = 0; i < PyList_GET_SIZE(dataList); i++) {
        sObj = PyList_GET_ITEM(dataList, i);
        if (!PYBYTES_Check(sObj)) {
            PyErr_SetString(PyExc_TypeError,
                            "data objects must be strings");
            return NULL;
        }

        vectors[i].iov_base = PYBYTES_AS_STRING(sObj);
        vectors[i].iov_len = PYBYTES_GET_SIZE(sObj);
    }

    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_iov = vectors;
    msg.msg_iovlen = PyList_GET_SIZE(dataList);
    msg.msg_flags = 0;

    msg.msg_controllen = sizeof(*ctrlMsg) + (sizeof(*sendFds) *
                                PyList_GET_SIZE(fdList));
    msg.msg_control = alloca(msg.msg_controllen);
    ctrlMsg = msg.msg_control;
    sendFds = (int *) CMSG_DATA(ctrlMsg);

    ctrlMsg->cmsg_len = msg.msg_controllen;
    ctrlMsg->cmsg_level = SOL_SOCKET;
    ctrlMsg->cmsg_type = SCM_RIGHTS;

    for (i = 0; i < PyList_GET_SIZE(fdList); i++) {
        intObj = PyList_GET_ITEM(fdList, i);
        if (!PYINT_Check(intObj)) {
            PyErr_SetString(PyExc_TypeError,
                            "integer file descriptor required");
            return NULL;
        }

        sendFds[i] = PYINT_AS_LONG(intObj);
    }

    if ((bytes = sendmsg(fd, &msg, 0)) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    return PYINT_FromLong(bytes);
}

static PyObject * py_recvmsg(PyObject *self, PyObject *args) {
    int fd, dataLen, fdCount;
    struct msghdr msg;
    struct cmsghdr * ctrlMsg;
    int i, expectedLen, bytes;
    struct iovec vector;
    PyObject * fdTuple, * rc;
    int * recvFds;

    if (!PyArg_ParseTuple(args, "iii", &fd, &dataLen, &fdCount))
        return NULL;

    msg.msg_name = NULL;
    msg.msg_namelen = 0;
    msg.msg_iov = &vector;
    msg.msg_iovlen = 1;
    msg.msg_flags = 0;

    if (fdCount) {
        expectedLen = sizeof(*ctrlMsg) + (sizeof(fd) * fdCount);
        msg.msg_controllen = expectedLen;
        msg.msg_control = alloca(msg.msg_controllen);
        ctrlMsg = msg.msg_control;

        ctrlMsg->cmsg_len = msg.msg_controllen;
        ctrlMsg->cmsg_level = SOL_SOCKET;
        ctrlMsg->cmsg_type = SCM_RIGHTS;
    } else {
        expectedLen = 0;
        msg.msg_controllen = expectedLen;
        msg.msg_control = NULL;
    }

    vector.iov_base = malloc(dataLen);
    vector.iov_len = dataLen;

    if ((bytes = recvmsg(fd, &msg, 0)) < 0) {
        free(vector.iov_base);
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    if (msg.msg_iovlen != 1) {
        free(vector.iov_base);
        PyErr_SetString(PyExc_IOError, "unexpected data vector count");
        return NULL;
    }

    if (msg.msg_controllen != expectedLen) {
        free(vector.iov_base);
        PyErr_SetString(PyExc_IOError, "unexpected control length");
        return NULL;
    }

    recvFds = (int *) CMSG_DATA(ctrlMsg);

    fdTuple = PyTuple_New(fdCount);
    if (!fdTuple) {
        free(vector.iov_base);
        return NULL;
    }

    for (i = 0; i < fdCount; i++) {
        PyTuple_SET_ITEM(fdTuple, i, PYINT_FromLong(recvFds[i]));
    }

    if (fdCount) {
        rc = Py_BuildValue("s#O", vector.iov_base, bytes, fdTuple);
    } else {
        rc = PYBYTES_FromStringAndSize(vector.iov_base, bytes);
    }
    free(vector.iov_base);

    return rc;
}


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

static PyObject * py_res_init(PyObject *self, PyObject *args) {
    int rc = res_init();
    return Py_BuildValue("i", rc);
}


static PyObject * rpmExpandMacro(PyObject *self, PyObject *args) {
    void * rpmso = NULL;
    static int (*expandMacro)(void * in, void * context, char * out,
                              size_t outSize) = NULL;
    char * expansion;
    char * input;
    int bufSize;
    char * libPath;

    if (!PyArg_ParseTuple(args, "ss", &libPath, &input))
        return NULL;

    bufSize = strlen(input) * 100;
    expansion = alloca(bufSize);
    strcpy(expansion, input);

    if (!expandMacro) {
        rpmso = dlopen(libPath, RTLD_LAZY);

        if (!rpmso) {
            PyErr_SetString(PyExc_TypeError,
                            "failed to load rpmModule for expandMacro");
            return NULL;
        }

        expandMacro = dlsym(rpmso, "expandMacros");

        if (!expandMacro) {
            PyErr_SetString(PyExc_TypeError,
                            "symbol expandMacro not found");
            return NULL;
        }
    }

    /* if this fails because the buffer isn't big enough, it prints a message
       and continues. nice. */
    expandMacro(NULL, NULL, expansion, bufSize);

    return PyString_FromString(expansion);

    Py_INCREF(Py_None);
    return Py_None;
}


PYMODULE_DECLARE(misc, MiscMethods, "miscellaneous low-level C functions for conary");

/* vim: set sts=4 sw=4 expandtab : */
