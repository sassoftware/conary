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
#include <arpa/inet.h>
#include "pycompat.h"

static PyObject * unpack(PyObject *self, PyObject *args);
static PyObject * pack(PyObject * self, PyObject * args);
static PyObject * dynamicSize(PyObject *self, PyObject *args);

static PyMethodDef methods[] = {
    { "unpack", unpack, METH_VARARGS },
    { "pack", pack, METH_VARARGS },
    { "dynamicSize", dynamicSize, METH_VARARGS },
    {NULL}  /* Sentinel */
};


int getSize(char ** s, int * val) {
    char lenStr[10];
    char * lenPtr = lenStr;
    char * ptr = *s;

    /* '\0' isn't a digit, so this check stops at the end */
    while (isdigit(*ptr) &&
           (lenPtr - lenStr) < sizeof(lenStr))
        *lenPtr++ = *ptr++;

    if ((lenPtr - lenStr) == sizeof(lenStr)) {
        PyErr_SetString(PyExc_ValueError, 
                        "length too long for S format");
        return -1;
    }

    *lenPtr = '\0';
    *s = ptr;
    *val = atoi(lenStr);

    return 0;
}

static PyObject * pack(PyObject * self, PyObject * args) {
    PyObject * formatArg, * arg;
    PyObject *ret = NULL;
    char * format, * formatPtr, * s;
    char *result = NULL;
    int argCount;
    int strLen;
    int argNum;
    int len, i;
    uint8_t oneByte;
    uint16_t twoBytes;
    uint32_t fourBytes;

    formatArg = PyTuple_GET_ITEM(args, 0);
    if (!PYBYTES_CheckExact(formatArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return NULL;
    }

    formatPtr = format = PYBYTES_AS_STRING(formatArg);

    /* walk the format twice, first to figure out the length and the second
       to build the string */
    argCount = PyTuple_GET_SIZE(args);

    if (*formatPtr != '!') {
        PyErr_SetString(PyExc_ValueError, "format must begin with !");
        return NULL;
    }
    formatPtr++;

    strLen = 0, argNum = 1;
    while (*formatPtr) {
        switch (*formatPtr++) {
            case 'B':
                arg = PyTuple_GET_ITEM(args, argNum++);
                if (!PYINT_CheckExact(arg)) {
                    PyErr_SetString(PyExc_TypeError,
                                    "argument for B format must be an int");
                    return NULL;
                }
                strLen += 1;
                break;

            case 'S':
                arg = PyTuple_GET_ITEM(args, argNum++);
                len = PYBYTES_GET_SIZE(arg);
                if (!PYBYTES_CheckExact(arg)) {
                    PyErr_SetString(PyExc_TypeError,
                                    "argument for S format must be a str");
                    return NULL;
                }
                s = PYBYTES_AS_STRING(arg);

                if (*formatPtr == 'H') {
                    strLen += 2 + len;
                    formatPtr++;
                } else if (*formatPtr == 'I') {
                    strLen += 4 + len;
                    formatPtr++;
                } else if (isdigit(*formatPtr)) {
                    if (getSize(&formatPtr, &i)) {
                        return NULL;
                    }

                    if (len != i) {
                        PyErr_SetString(PyExc_RuntimeError, "bad string size");
                        return NULL;
                    }

                    strLen += len;
                } else {
                    PyErr_SetString(PyExc_ValueError, 
                                "# must be followed by H or I in format");
                    return NULL;
                }

                break;

            default:
                PyErr_SetString(PyExc_ValueError,
                                "unknown character in pack format");
                return NULL;
        }
    }

    result = malloc(strLen);
    if (result == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    argNum = 1;
    strLen = 0;
    formatPtr = format + 1;
    while (*formatPtr) {
        switch (*formatPtr++) {
            case 'B':
                arg = PyTuple_GET_ITEM(args, argNum++);
                oneByte = PYINT_AS_LONG(arg);
                result[strLen++] = oneByte;
                break;

            case 'S':
                arg = PyTuple_GET_ITEM(args, argNum++);
                s = PYBYTES_AS_STRING(arg);
                len = PYBYTES_GET_SIZE(arg);

                if (*formatPtr == 'H') {
                    twoBytes = htons(len);
                    memcpy(result + strLen, &twoBytes, sizeof(twoBytes));
                    strLen += 2;
                    formatPtr++;
                } else if (*formatPtr == 'I') {
                    fourBytes = htonl(len);
                    memcpy(result + strLen, &fourBytes, sizeof(fourBytes));
                    strLen += 4;
                    formatPtr++;
                } else if (isdigit(*formatPtr)) {
                    if (getSize(&formatPtr, &i)) {
                        goto cleanup;
                    }
                } else {
                    PyErr_SetString(PyExc_RuntimeError,
                                    "internal pack error 1");
                    goto cleanup;
                }


                memcpy(result + strLen, s, len);
                strLen += len;
                break;

            default:
                PyErr_SetString(PyExc_RuntimeError,
                                "internal pack error 2");
                goto cleanup;
        }
    }

    ret = PYBYTES_FromStringAndSize(result, strLen);

cleanup:
    if (result != NULL) {
        free(result);
    }
    return ret;
}

static PyObject * unpack(PyObject *self, PyObject *args) {
    /* Borrowed references */
    PyObject *formatArg, *offsetArg, *dataArg;
    PyObject *retVal = NULL;
    /* Kept references */
    PyObject *retList = NULL, *dataObj = NULL;
    char * data, * format;
    char *dataPtr, *formatPtr, *limit;
    char b;
    int dataLen;
    int offset;
    unsigned int intVal;

    /* This avoids PyArg_ParseTuple because it's sloooow */
    if (PyTuple_GET_SIZE(args) != 3) {
        PyErr_SetString(PyExc_TypeError, "exactly three arguments expected");
        return NULL;
    }

    formatArg = PyTuple_GET_ITEM(args, 0);
    offsetArg = PyTuple_GET_ITEM(args, 1);
    dataArg = PyTuple_GET_ITEM(args, 2);

    if (!PYBYTES_CheckExact(formatArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return NULL;
    } else if (!PYINT_CheckExact(offsetArg)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be an int");
        return NULL;
    } else if (!PYBYTES_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "third argument must be a string");
        return NULL;
    }

    format = PYBYTES_AS_STRING(formatArg);
    offset = PYINT_AS_LONG(offsetArg);
    data = PYBYTES_AS_STRING(dataArg);
    dataLen = PYBYTES_GET_SIZE(dataArg);
    limit = data + dataLen;

    formatPtr = format;

    if (*formatPtr != '!') {
        PyErr_SetString(PyExc_ValueError, "format must begin with !");
        return NULL;
    }
    formatPtr++;

    dataPtr = data + offset;
    if (dataPtr >= limit) {
        PyErr_SetString(PyExc_ValueError, "offset out of bounds");
        return NULL;
    }
    retList = PyList_New(0);
    if (retList == NULL) {
        return NULL;
    }

    while (*formatPtr) {
        switch (*formatPtr) {
          case 'B':
            if (dataPtr + 1 > limit) {
                PyErr_SetString(PyExc_ValueError, "data too short for format string");
                goto cleanup;
            }
            intVal = (int) *dataPtr++;
            dataObj = PYINT_FromLong(intVal);
            if (PyList_Append(retList, dataObj)) {
                goto cleanup;
            }
            Py_CLEAR(dataObj);
            formatPtr++;
            break;

          case 'H':
            if (dataPtr + 2 > limit) {
                PyErr_SetString(PyExc_ValueError, "data too short for format string");
                goto cleanup;
            }
            intVal = ntohs(*((short *) dataPtr));
            dataObj = PYINT_FromLong(intVal);
            if (PyList_Append(retList, dataObj)) {
                goto cleanup;
            }
            Py_CLEAR(dataObj);
            dataPtr += 2;
            formatPtr++;
            break;

          case 'S':
            /* extension -- extract a string based on the length which
               preceeds it */
            formatPtr++;

            if (*formatPtr == 'H') {
                if (dataPtr + 2 > limit) {
                    PyErr_SetString(PyExc_ValueError, "data too short for format string");
                    goto cleanup;
                }
                intVal = ntohs(*((short *) dataPtr));
                dataPtr += 2;
                formatPtr++;
            } else if (*formatPtr == 'I') {
                if (dataPtr + 4 > limit) {
                    PyErr_SetString(PyExc_ValueError, "data too short for format string");
                    goto cleanup;
                }
                intVal = ntohl(*((int *) dataPtr));
                dataPtr += 4;
                formatPtr++;
            } else if (isdigit(*formatPtr)) {
                char lenStr[10];
                char * lenPtr = lenStr;

                /* '\0' isn't a digit, so this check stops at the end */
                while (isdigit(*formatPtr) &&
                       (lenPtr - lenStr) < sizeof(lenStr))
                    *lenPtr++ = *formatPtr++;

                if ((lenPtr - lenStr) == sizeof(lenStr)) {
                    PyErr_SetString(PyExc_ValueError, 
                                    "length too long for S format");
                    goto cleanup;
                }

                *lenPtr = '\0';

                intVal = atoi(lenStr);
            } else {
                PyErr_SetString(PyExc_ValueError, 
                                "# must be followed by H or I in format");
                goto cleanup;
            }

            if (dataPtr + intVal > limit) {
                PyErr_SetString(PyExc_ValueError, "data too short for format string");
                goto cleanup;
            }
            dataObj = PYBYTES_FromStringAndSize(dataPtr, intVal);
            if (PyList_Append(retList, dataObj)) {
                goto cleanup;
            }
            Py_CLEAR(dataObj);
            dataPtr += intVal;
            break;

          case 'D':
            /* extension -- extract a string based on the length which
               preceeds it.  the length is dynamic based on the size */
            formatPtr++;

            /* high bits of the first byte
               00: low 6 bits are value
               01: low 14 bits are value
               10: low 30 bits are value
               11: low 62 bits are value (unimplemented)
            */
            /* look at the first byte */
            b = *dataPtr;
            if ((b & 0xc0) == 0x80) {
                /* 30 bit length */
                if (dataPtr + 4 > limit) {
                    PyErr_SetString(PyExc_ValueError, "data too short for format string");
                    goto cleanup;
                }
                intVal = ntohl(*((uint32_t *) dataPtr)) & 0x3fffffff;
                dataPtr += sizeof(uint32_t);
            } else if ((b & 0xc0) == 0x40) {
                /* 14 bit length */
                if (dataPtr + 2 > limit) {
                    PyErr_SetString(PyExc_ValueError, "data too short for format string");
                    goto cleanup;
                }
                intVal = ntohs(*((uint16_t *) dataPtr)) & 0x3fff;
                dataPtr += sizeof(uint16_t);
            } else if ((b & 0xc0) == 0x00) {
                /* 6 bit length */
                if (dataPtr + 1 > limit) {
                    PyErr_SetString(PyExc_ValueError, "data too short for format string");
                    goto cleanup;
                }
                intVal = *((uint8_t *) dataPtr) & ~(1 << 6);
                dataPtr += sizeof(uint8_t);
            } else {
                PyErr_SetString(PyExc_ValueError, 
                                "unimplemented dynamic size");
                goto cleanup;
            }

            if (dataPtr + intVal > limit) {
                PyErr_SetString(PyExc_ValueError, "data too short for format string");
                goto cleanup;
            }
            dataObj = PYBYTES_FromStringAndSize(dataPtr, intVal);
            if (dataObj == NULL) {
                goto cleanup;
            }
            if (PyList_Append(retList, dataObj)) {
                goto cleanup;
            }
            Py_CLEAR(dataObj);
            dataPtr += intVal;
            break;

          default:
            PyErr_SetString(PyExc_ValueError, "unknown character in format");
            goto cleanup;
        }
    }

    retVal = Py_BuildValue("iO", dataPtr - data, retList);
    Py_CLEAR(retList);

cleanup:
    Py_XDECREF(dataObj);
    Py_XDECREF(retList);
    return retVal;
}

static PyObject * dynamicSize(PyObject *self, PyObject *args) {
    PyObject * sizeArg;
    char sizebuf[4];
    uint32_t size;
    int sizelen;

    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "exactly one argument expected");
        return NULL;
    }

    sizeArg = PyTuple_GET_ITEM(args, 0);
    if (!PYINT_CheckExact(sizeArg)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a string");
        return NULL;
    }

    size = PYINT_AS_LONG(sizeArg);
    if (size < (1 << 6)) {
        *sizebuf = (char) size;
        sizelen = sizeof(char);
    } else if (size < (1 << 14)) {
        /* mask top two bits and set them to 01 */
        *((uint16_t *) sizebuf) = htons((size & 0x3fff) | 0x4000);
        sizelen = sizeof(uint16_t);
    } else if (size < (1 << 30)) {
        /* mask top two bits and set them to 10 */
        *((uint32_t *) sizebuf) = htonl((size & 0x3fffffff) | 0x80000000);
        sizelen = sizeof(uint32_t);
    } else {
        PyErr_SetString(PyExc_ValueError, 
                        "unimplemented dynamic size");
        return NULL;
    }
    return PYBYTES_FromStringAndSize(sizebuf, sizelen);
}


PYMODULE_DECLARE(pack, methods, "Utility functions for packing binary data");

/* vim: set sts=4 sw=4 expandtab : */
