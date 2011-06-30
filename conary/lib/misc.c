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

#include "pycompat.h"

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static PyObject * py_sendmsg(PyObject *self, PyObject *args);
static PyObject * py_recvmsg(PyObject *self, PyObject *args);
static PyObject * rpmExpandMacro(PyObject *self, PyObject *args);

static PyMethodDef MiscMethods[] = {
    { "sendmsg", py_sendmsg, METH_VARARGS },
    { "recvmsg", py_recvmsg, METH_VARARGS },
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
