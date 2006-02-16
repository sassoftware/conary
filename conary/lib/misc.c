/*
 *
 * Copyright (c) 2004 rPath, Inc.
 *
 * This program is distributed under the terms of the Common Public License,
 * version 1.0. A copy of this license should have been distributed with this
 * source file in a file called LICENSE. If it is not present, the license
 * is always available at http://www.opensource.org/licenses/cpl.php.
 *
 * This program is distributed in the hope that it will be useful, but
 * without any waranty; without even the implied warranty of merchantability
 * or fitness for a particular purpose. See the Common Public License for
 * full details.
 *
 */

#include <Python.h>

#include <ctype.h>
#include <errno.h>
#include <malloc.h>
#include <netinet/in.h>
#include <sys/stat.h>

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static PyObject * exists(PyObject *self, PyObject *args);
static PyObject * malloced(PyObject *self, PyObject *args);
static PyObject * removeIfExists(PyObject *self, PyObject *args);
static PyObject * unpack(PyObject *self, PyObject *args);

static PyMethodDef MiscMethods[] = {
    { "exists", exists, METH_VARARGS,
        "returns a boolean reflecting whether a file (even a broken symlink) "
        "exists in the filesystem" },
    { "malloced", malloced, METH_VARARGS, 
	"amount of memory currently allocated through malloc()" },
    { "removeIfExists", removeIfExists, METH_VARARGS, 
	"unlinks a file if it exists; silently fails if it does not exist. "
	"returns a boolean indicating whether or not a file was removed" },
    { "unpack", unpack, METH_VARARGS },
    {NULL}  /* Sentinel */
};

static PyObject * malloced(PyObject *self, PyObject *args) {
    struct mallinfo ma;

    ma = mallinfo();

    /* worked */
    return Py_BuildValue("i", ma.uordblks);
}

static PyObject * exists(PyObject *self, PyObject *args) {
    char * fn;
    struct stat sb;

    if (!PyArg_ParseTuple(args, "s", &fn))
        return NULL;

    if (lstat(fn, &sb)) {
        if (errno == ENOENT || errno == ENOTDIR || errno == ENAMETOOLONG) {
            Py_INCREF(Py_False);
            return Py_False;
        }

        PyErr_SetFromErrnoWithFilename(PyExc_OSError, fn);
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject * removeIfExists(PyObject *self, PyObject *args) {
    char * fn;

    if (!PyArg_ParseTuple(args, "s", &fn))
        return NULL;

    if (unlink(fn)) {
        if (errno == ENOENT || errno == ENAMETOOLONG) {
            Py_INCREF(Py_False);
            return Py_False;
        }

        PyErr_SetFromErrnoWithFilename(PyExc_OSError, fn);
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

static PyObject * unpack(PyObject *self, PyObject *args) {
    char * data, * format;
    int dataLen;
    char * dataPtr, * formatPtr;
    int offset;
    PyObject * retList, * dataObj;
    int intVal;

    //breakpoint;

    if (!PyArg_ParseTuple(args, "sis#", &format, &offset, &data, &dataLen))
        return NULL;

    formatPtr = format;

    if (*formatPtr != '!') {
        PyErr_SetString(PyExc_ValueError, "format must begin with !");
        return NULL;
    }
    formatPtr++;

    retList = PyList_New(0);
    dataPtr = data + offset;

    while (*formatPtr) {
        switch (*formatPtr) {
          case 'H':
            intVal = ntohs(*((short *) dataPtr));
            dataObj = PyInt_FromLong(intVal);
            PyList_Append(retList, dataObj);
            Py_DECREF(dataObj);
            dataPtr += 2;
            formatPtr++;
            break;

          case 'S':
            /* extension -- extract a string based on the length which
               preceeds it */
            formatPtr++;

            if (*formatPtr == 'H') {
                intVal = ntohs(*((short *) dataPtr));
                dataPtr += 2;
                formatPtr++;
            } else if (isdigit(*formatPtr)) {
                char lenStr[10];
                char * lenPtr = lenStr;

                /* '\0' isn't a digit, so this check stops at the end */
                while (isdigit(*formatPtr) &&
                       (lenPtr - lenStr) < sizeof(lenStr))
                    *lenPtr++ = *formatPtr++;

                if ((lenPtr - lenStr) == sizeof(lenStr)) {
                    Py_DECREF(retList);
                    PyErr_SetString(PyExc_ValueError, 
                                    "length too long for S format");
                    return NULL;
                }

                *lenPtr = '\0';

                intVal = atoi(lenStr);
            } else {
                Py_DECREF(retList);
                PyErr_SetString(PyExc_ValueError, 
                                "# must be followed by H in format");
                return NULL;
            }

            dataObj = PyString_FromStringAndSize(dataPtr, intVal);
            PyList_Append(retList, dataObj);
            Py_DECREF(dataObj);
            dataPtr += intVal;
            break;

          default:
            Py_DECREF(retList);
            PyErr_SetString(PyExc_ValueError, "unknown character in format");
            return NULL;
        }
    }

    return Py_BuildValue("iO", dataPtr - data, retList);
}

PyMODINIT_FUNC
initmisc(void)
{
    Py_InitModule3("misc", MiscMethods, 
		   "miscelaneous low-level C functions for conary");
}
