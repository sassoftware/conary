/*
 * Copyright (c) 2010 rPath, Inc.
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
#include <string.h>

#include "pycompat.h"

/* Truth table for "safe" label characters. */
static char label_chars[256];
static char release_chars[256];

static PyObject * versionFromString(PyObject *self, PyObject *args);
static int vfs_appendLabel(PyObject *versionsMod, PyObject *vlist,
        PyObject *host, PyObject *namespace, PyObject *tag);
static int vfs_appendRevision(PyObject *versionsMod, PyObject *vlist,
        PyObject *timestamp, PyObject *release, PyObject *srcCount, PyObject *binCount);
static PyObject *vfs_makeSerialNumber(PyObject *versionsMod, PyObject *numList);
static PyObject *vfs_makeVersion(PyObject *versionsMod, PyObject *vlist);

static PyMethodDef VersionsMethods[] = {
    { "versionFromString", versionFromString, METH_VARARGS },
    {NULL}  /* Sentinel */
};


static PyObject * raise_parse_error(const char *msg, int offset) {
    char buffer[256];
    const char *newmsg;
    PyObject *exception, *module;

    if (offset >= 0) {
        if (snprintf(buffer, 255, "%s at offset %d", msg, offset) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "snprintf failed");
            return NULL;
        }
        newmsg = buffer;
    } else {
        newmsg = msg;
    }

    module = PyImport_ImportModule("conary.errors");
    if (module == NULL) {
        return NULL;
    }
    exception = PyObject_GetAttrString(module, "ParseError");
    Py_DECREF(module);
    if (exception == NULL) {
        return NULL;
    }

    PyErr_SetString(exception, newmsg);
    Py_DECREF(exception);
    return NULL;
}


static PyObject * versionFromString(PyObject *self, PyObject *args) {
    PyObject *inputObj = NULL, *vlist = NULL, *res = NULL;
    PyObject *lastHost = NULL, *lastNamespace = NULL, *lastTag = NULL;
    PyObject *lastTimestamp = NULL, *lastRelease = NULL;
    PyObject *lastSrcCount = NULL, *lastBinCount = NULL;
    PyObject *versionsMod = NULL;
    PyObject *label = NULL, *value = NULL, *value2 = NULL;
    const char *input, *ptr, *ptr2;
    int gotHost, gotNamespace, branchCount = 0, shadowCount;

    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "exactly one argument expected");
        goto done;
    }
    inputObj = PyTuple_GET_ITEM(args, 0);

    if (!PYBYTES_CheckExact(inputObj)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        goto done;
    }
    ptr = input = PYBYTES_AS_STRING(inputObj);

    versionsMod = PyImport_ImportModule("conary.versions");
    if (versionsMod == NULL) {
        goto done;
    }

    vlist = PyList_New(0);
    if (vlist == NULL) {
        goto done;
    }

    while (1) {
        /* Expect: branch */
        if (*ptr != '/') {
            raise_parse_error("Expected branch", ptr - input);
            goto done;
        }
        ptr++;

        /* Old-fashioned branches are rare enough, and parsing their revisions
         * is complicated enough (due to inheriting the release string and
         * source count), that it's easier to just defer to the Python
         * implementation here.
         */
        if (branchCount > 0) {
            value = PyObject_GetAttrString(versionsMod, "VersionFromString");
            if (value == NULL) {
                goto done;
            }
            value2 = PyTuple_Pack(1, inputObj);
            if (value2 == NULL) {
                goto done;
            }
            res = PyObject_Call(value, value2, NULL);
            goto done;
        }
        branchCount++;

        shadowCount = 0;
        while (1) {
            /* Expect: label (partial or full) */
            if (!*ptr) {
                raise_parse_error("Expected label, not end of string", ptr - input);
                goto done;
            } else if (*ptr == '/') {
                raise_parse_error("Expected label, not '/'", ptr - input);
                goto done;
            }

            Py_CLEAR(lastTag);
            ptr2 = ptr;
            gotHost = gotNamespace = 0;
            while (1) {
                if (*ptr2 == 0) {
                    raise_parse_error("Expected label, not end of string", ptr2 - input);
                    goto done;
                } else if (*ptr2 == '@') {
                    /* Finished host */
                    if (gotHost) {
                        raise_parse_error("Too many '@'", ptr2 - input);
                        goto done;
                    }
                    Py_XDECREF(lastHost);
                    lastHost = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                    if (lastHost == NULL) {
                        goto done;
                    }
                    Py_CLEAR(lastNamespace);
                    gotHost = 1;
                    ptr = ++ptr2;
                    continue;
                } else if (*ptr2 == ':') {
                    /* Finished namespace */
                    if (lastHost == NULL) {
                        raise_parse_error("Expected full label", ptr - input);
                        goto done;
                    } else if (gotNamespace) {
                        raise_parse_error("Too many ':'", ptr2 - input);
                        goto done;
                    }
                    Py_XDECREF(lastNamespace);
                    lastNamespace = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                    if (lastNamespace == NULL) {
                        goto done;
                    }
                    gotNamespace = 1;
                    ptr = ++ptr2;
                    continue;
                } else if (*ptr2 == '/') {
                    /* Finished tag */
                    if (lastHost == NULL) {
                        raise_parse_error("Expected full label", ptr - input);
                        goto done;
                    } else if (lastNamespace == NULL) {
                        raise_parse_error("Expected namespace before '/'", ptr2 - input);
                        goto done;
                    }
                    lastTag = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                    if (lastTag == NULL) {
                        goto done;
                    }
                    ptr = ++ptr2;
                    break;
                } else {
                    /* Copying characters for host, namespace, or tag */
                    if (!label_chars[(int)*ptr2]) {
                        raise_parse_error("Illegal character in label", ptr2 - input);
                        goto done;
                    }
                    ptr2++;
                }
            }

            /* Stuff Label object into version list */
            if (vfs_appendLabel(versionsMod, vlist, lastHost, lastNamespace, lastTag)) {
                goto done;
            }

            shadowCount++;
            if (*ptr == '/') {
                /* Shadow, next will be another label. */
                ptr++;
                continue;
            } else {
                /* Next will be a revision. */
                break;
            }
        }

        /* Expect: release with or without timestamp */
        Py_CLEAR(lastTimestamp);
        Py_CLEAR(lastRelease);
        ptr2 = ptr;
        while (1) {
            if (*ptr2 == 0) {
                raise_parse_error("Expected release, not end of string", ptr2 - input);
                goto done;
            } else if (*ptr2 == '/') {
                raise_parse_error("Expected release, not '/'", ptr2 - input);
                goto done;
            } else if (*ptr2 == ':') {
                /* Finished timestamp */
                if (lastTimestamp) {
                    raise_parse_error("Too many ':'", ptr2 - input);
                    goto done;
                }
                value = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                if (value == NULL) {
                    goto done;
                }
                lastTimestamp = PyFloat_FromString(value, NULL);
                Py_CLEAR(value);
                if (lastTimestamp == NULL) {
                    goto done;
                }
                ptr = ++ptr2;
                continue;
            } else if (*ptr2 == '-') {
                /* Finished release */
                lastRelease = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                if (lastRelease == NULL) {
                    goto done;
                }
                ptr = ++ptr2;
                break;
            } else {
                /* Copying characters for timestamp or release */
                if (!release_chars[(int)*ptr2]) {
                    raise_parse_error("Illegal character in release", ptr2 - input);
                    goto done;
                }
                ptr2++;
            }
        }

        /* Expect: source count */
        Py_XDECREF(lastSrcCount);
        lastSrcCount = PyList_New(0);
        if (lastSrcCount == NULL) {
            goto done;
        }
        while (1) {
            if (*ptr2 == '.' || *ptr2 == '/' || *ptr2 == '-' || *ptr2 == 0) {
                /* Got one "level" of the count */
                if (ptr == ptr2) {
                    raise_parse_error("Source count ended prematurely", ptr2 - input);
                    goto done;
                }
                value = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                if (value == NULL) {
                    goto done;
                }
                value2 = PyNumber_Int(value);
                Py_CLEAR(value);
                if (value2 == NULL) {
                    goto done;
                }
                if (PyList_Append(lastSrcCount, value2)) {
                    goto done;
                }
                Py_CLEAR(value2);
                if (*ptr2 != '.') {
                    /* Finished count */
                    break;
                }
                ptr = ++ptr2;
            } else if (*ptr2 >= '0' && *ptr2 <= '9') {
                /* Copying digits for count */
                ptr2++;
            } else {
                raise_parse_error("Illegal character in source count", ptr2 - input);
                goto done;
            }
        }

        /* This call steals the only reference to the old lastSrcCount and
         * returns a new one. */
        lastSrcCount = vfs_makeSerialNumber(versionsMod, lastSrcCount);
        if (lastSrcCount == NULL) {
            goto done;
        }

        Py_XDECREF(lastBinCount);
        if (*ptr2 == 0 || *ptr2 == '/') {
            Py_INCREF(Py_None);
            lastBinCount = Py_None;
        } else {
            /* Expect: build count */
            ptr = ++ptr2;
            lastBinCount = PyList_New(0);
            if (lastBinCount == NULL) {
                goto done;
            }
            while (1) {
                if (*ptr2 == '-') {
                    raise_parse_error("Too many '-'", ptr2 - input);
                    goto done;
                } else if (*ptr2 == '.' || *ptr2 == '/' || *ptr2 == 0) {
                    /* Got one "level" of the count */
                    if (ptr == ptr2) {
                        /* No count present */
                        raise_parse_error("Build count ended prematurely", ptr2 - input);
                        goto done;
                    }
                    value = PYBYTES_FromStringAndSize(ptr, ptr2 - ptr);
                    if (value == NULL) {
                        goto done;
                    }
                    value2 = PyNumber_Int(value);
                    Py_CLEAR(value);
                    if (value2 == NULL) {
                        goto done;
                    }
                    if (PyList_Append(lastBinCount, value2)) {
                        goto done;
                    }
                    Py_CLEAR(value2);
                    if (*ptr2 != '.') {
                        /* Finished count */
                        break;
                    }
                    ptr = ++ptr2;
                } else if (*ptr2 >= '0' && *ptr2 <= '9') {
                    /* Copying digits for count */
                    ptr2++;
                } else {
                    raise_parse_error("Illegal character in build count", ptr2 - input);
                    goto done;
                }
            }

            /* This call steals the only reference to the old lastBinCount and
             * returns a new one. */
            lastBinCount = vfs_makeSerialNumber(versionsMod, lastBinCount);
            if (lastBinCount == NULL) {
                goto done;
            }
        }

        if (vfs_appendRevision(versionsMod, vlist, lastTimestamp, lastRelease,
                lastSrcCount, lastBinCount)) {
            goto done;
        }

        ptr = ptr2;
        if (*ptr2 == 0) {
            /* End of string */
            break;
        } else {
            /* Old-fashioned branch */
            continue;
        }
    }

    res = vfs_makeVersion(versionsMod, vlist);
    Py_CLEAR(vlist);

done:
    Py_XDECREF(lastHost);
    Py_XDECREF(lastNamespace);
    Py_XDECREF(lastTag);
    Py_XDECREF(lastTimestamp);
    Py_XDECREF(lastRelease);
    Py_XDECREF(lastSrcCount);
    Py_XDECREF(lastBinCount);
    Py_XDECREF(label);
    Py_XDECREF(value);
    Py_XDECREF(value2);
    Py_XDECREF(vlist);
    Py_XDECREF(versionsMod);
    return res;
}


static int vfs_appendLabel(PyObject *versionsMod, PyObject *vlist,
        PyObject *host, PyObject *namespace, PyObject *tag) {
    PyObject *objTypeRaw = NULL, *args = NULL, *obj = NULL;
    PyTypeObject *objType;
    int res = -1;

    objTypeRaw = PyObject_GetAttrString(versionsMod, "Label");
    if (objTypeRaw == NULL) {
        goto done;
    }
    objType = (PyTypeObject *)objTypeRaw;
    if (!PyType_Check(objType)) {
        PyErr_SetString(PyExc_TypeError,
                "conary.versions.Label is not a type object");
        goto done;
    }
    args = PyTuple_New(0);
    if (args == NULL) {
        goto done;
    }
    obj = objType->tp_new(objType, args, NULL);
    if (obj == NULL) {
        goto done;
    }

    if (PyObject_SetAttrString(obj, "host", host)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "namespace", namespace)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "branch", tag)) {
        goto done;
    }

    if (PyList_Append(vlist, obj)) {
        goto done;
    }

    res = 0;

done:
    Py_XDECREF(objTypeRaw);
    Py_XDECREF(args);
    Py_XDECREF(obj);
    return res;
}


static int vfs_appendRevision(PyObject *versionsMod, PyObject *vlist,
        PyObject *timestamp, PyObject *release, PyObject *srcCount, PyObject *binCount) {
    PyObject *objTypeRaw = NULL, *args = NULL, *obj = NULL;
    PyTypeObject *objType;
    int res = -1;

    objTypeRaw = PyObject_GetAttrString(versionsMod, "Revision");
    if (objTypeRaw == NULL) {
        goto done;
    }
    objType = (PyTypeObject *)objTypeRaw;
    if (!PyType_Check(objType)) {
        PyErr_SetString(PyExc_TypeError,
                "conary.versions.Revision is not a type object");
        goto done;
    }
    args = PyTuple_New(0);
    if (args == NULL) {
        goto done;
    }
    obj = objType->tp_new(objType, args, NULL);
    if (obj == NULL) {
        goto done;
    }

    if (timestamp == NULL) {
        timestamp = PYINT_FromLong(0);
    }
    if (PyObject_SetAttrString(obj, "timeStamp", timestamp)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "version", release)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "sourceCount", srcCount)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "buildCount", binCount)) {
        goto done;
    }

    if (PyList_Append(vlist, obj)) {
        goto done;
    }

    res = 0;

done:
    Py_XDECREF(objTypeRaw);
    Py_XDECREF(args);
    Py_XDECREF(obj);
    return res;
}


static PyObject *vfs_makeSerialNumber(PyObject *versionsMod, PyObject *numList) {
    /* This function steals a reference to numList */
    PyObject *objTypeRaw = NULL, *args = NULL, *obj = NULL, *res = NULL;
    PyTypeObject *objType;

    objTypeRaw = PyObject_GetAttrString(versionsMod, "SerialNumber");
    if (objTypeRaw == NULL) {
        goto done;
    }
    objType = (PyTypeObject *)objTypeRaw;
    if (!PyType_Check(objType)) {
        PyErr_SetString(PyExc_TypeError,
                "conary.versions.SerialNumber is not a type object");
        goto done;
    }
    args = PyTuple_New(0);
    if (args == NULL) {
        goto done;
    }
    obj = objType->tp_new(objType, args, NULL);
    if (obj == NULL) {
        goto done;
    }

    if (PyObject_SetAttrString(obj, "numList", numList)) {
        goto done;
    }
    Py_CLEAR(numList);

    res = obj;
    obj = NULL;

done:
    Py_XDECREF(objTypeRaw);
    Py_XDECREF(args);
    Py_XDECREF(obj);
    Py_XDECREF(numList);
    return res;
}


static PyObject *vfs_makeVersion(PyObject *versionsMod, PyObject *vlist) {
    PyObject *objTypeRaw = NULL, *args = NULL, *obj = NULL, *res = NULL;
    PyTypeObject *objType;

    objTypeRaw = PyObject_GetAttrString(versionsMod, "Version");
    if (objTypeRaw == NULL) {
        goto done;
    }
    objType = (PyTypeObject *)objTypeRaw;
    if (!PyType_Check(objType)) {
        PyErr_SetString(PyExc_TypeError,
                "conary.versions.Version is not a type object");
        goto done;
    }
    args = PyTuple_New(0);
    if (args == NULL) {
        goto done;
    }
    obj = objType->tp_new(objType, args, NULL);
    if (obj == NULL) {
        goto done;
    }

    if (PyObject_SetAttrString(obj, "versions", vlist)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "cached", Py_False)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "hash", Py_None)) {
        goto done;
    }
    if (PyObject_SetAttrString(obj, "strRep", Py_None)) {
        goto done;
    }

    res = obj;
    obj = NULL;

done:
    Py_XDECREF(objTypeRaw);
    Py_XDECREF(args);
    Py_XDECREF(obj);
    return res;
}


static PyModuleDef VersionsModule = {
    PyModuleDef_HEAD_INIT,
    "_versions",
    "Accelerated Conary version parser",
    -1,
    VersionsMethods
};

PYMODULE_INIT(_versions)
{
    int i;
    char alnum[256];
    PyObject *m = PYMODULE_CREATE(&VersionsModule);

    for (i = 0; i < 256; i++)
        alnum[i] = 0;
    for (i = '0'; i <= '9'; i++)
        alnum[i] = 1;
    for (i = 'A'; i <= 'Z'; i++)
        alnum[i] = 1;
    for (i = 'a'; i <= 'z'; i++)
        alnum[i] = 1;

    memcpy(label_chars, alnum, 256);
    label_chars['-'] = 1;
    label_chars['_'] = 1;
    label_chars['.'] = 1;

    memcpy(release_chars, label_chars, 256);
    release_chars['('] = 1;
    release_chars[')'] = 1;
    release_chars['+'] = 1;
    release_chars[','] = 1;
    release_chars['.'] = 1;
    release_chars[';'] = 1;
    release_chars['_'] = 1;
    release_chars['~'] = 1;

    PYMODULE_RETURN(m);
}

/* vim: set sts=4 sw=4 expandtab : */
