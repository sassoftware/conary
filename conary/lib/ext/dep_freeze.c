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
#include "pycompat.h"

static PyObject * depSetSplit(PyObject *self, PyObject *args);
static PyObject * depSplit(PyObject *self, PyObject *args);
static PyObject * depSetFreeze(PyObject *self, PyObject *args);

static PyMethodDef methods[] = {
    { "depSetSplit", depSetSplit, METH_VARARGS },
    { "depSplit", depSplit, METH_VARARGS },
    { "depSetFreeze", depSetFreeze, METH_VARARGS },
    {NULL}  /* Sentinel */
};


static PyObject * depSetSplit(PyObject *self, PyObject *args) {
    /* Borrowed references */
    PyObject *offsetArg, *dataArg;
    Py_ssize_t dataSize;
    char *data, *dataPtr, *endPtr, *limit;
    int offset, tag;

    /* This avoids PyArg_ParseTuple because it's sloooow */
    if (PyTuple_GET_SIZE(args) != 2) {
        PyErr_SetString(PyExc_TypeError, "exactly two arguments expected");
        return NULL;
    }

    offsetArg = PyTuple_GET_ITEM(args, 0);
    dataArg = PyTuple_GET_ITEM(args, 1);

    if (!PYINT_CheckExact(offsetArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be an int");
        return NULL;
    } else if (!PYBYTES_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a string");
        return NULL;
    }

    offset = PYINT_AS_LONG(offsetArg);
    PYBYTES_AsStringAndSize(dataArg, &data, &dataSize);
    limit = data + dataSize;

    if (offset > dataSize) {
        PyErr_SetString(PyExc_ValueError, "offset out of bounds");
        return NULL;
    }
    dataPtr = data + offset;
    if (*dataPtr == 0) {
        PyErr_SetString(PyExc_ValueError, "invalid frozen dependency");
        return NULL;
    }
    endPtr = dataPtr;

    tag = 0;
    /* Grab the tag first. Go ahead an convert it to an int while we're
       grabbing it. */
    while (*endPtr && *endPtr != '#') {
        tag *= 10;
        tag += *endPtr - '0';
        endPtr++;
    }
    dataPtr = endPtr + 1;
    if (dataPtr > limit) {
        PyErr_SetString(PyExc_ValueError, "invalid frozen dependency");
        return NULL;
    }

    /* Now look for the frozen dependency */
    /* Grab the tag first */
    while (*endPtr && *endPtr != '|') {
        endPtr++;
    }
    if (endPtr > limit) {
        PyErr_SetString(PyExc_ValueError, "invalid frozen dependency");
        return NULL;
    }

    return Py_BuildValue("iis#",
            endPtr - data + 1, tag, dataPtr, endPtr - dataPtr);

}

static PyObject * depSplit(PyObject *self, PyObject *args) {
    /* Borrowed references */
    PyObject *dataArg;
    PyObject *ret = NULL;
    /* Kept references */
    PyObject *flags = NULL, *name = NULL;
    PyObject *flag = NULL;
    char *origData, *data = NULL, *chptr, *endPtr;

    /* This avoids PyArg_ParseTuple because it's sloooow */
    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "exactly one argument expected");
        goto cleanup;
    }

    dataArg = PyTuple_GET_ITEM(args, 0);

    if (!PYBYTES_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        goto cleanup;
    }

    origData = PYBYTES_AS_STRING(dataArg);

    /* Copy the original string over, replace single : with a '\0' and
       double :: with a single :, and \X with X (where X is anything,
       including backslash)  */
    endPtr = data = malloc(strlen(origData) + 1);
    if (data == NULL) {
        PyErr_NoMemory();
        goto cleanup;
    }
    chptr = origData;
    while (*chptr) {
        if (*chptr == ':') {
            chptr++;
            if (*chptr == ':') {
                *endPtr++ = ':';
                chptr++;
            } else {
                *endPtr++ = '\0';
            }
        } else if (*chptr == '\\') {
            chptr++;
            *endPtr++ = *chptr++;
        } else { 
            *endPtr++ = *chptr++;
        }
    }

    *endPtr++ = '\0';

    /* We're left with a '\0' separated list of name, flag1, ..., flagN. Get
       the name first. */
    chptr = data;
    name = PYBYTES_FromString(chptr);
    if (name == NULL) {
        goto cleanup;
    }
    chptr += strlen(data) + 1;

    flags = PyList_New(0);
    if (flags == NULL) {
        goto cleanup;
    }
    while (chptr < endPtr) {
        flag = PYBYTES_FromString(chptr);
        if (flag == NULL) {
            goto cleanup;
        }
        if (PyList_Append(flags, flag)) {
            goto cleanup;
        }
        Py_CLEAR(flag);
        chptr += strlen(chptr) + 1;
    }

    ret = PyTuple_Pack(2, name, flags);

cleanup:
    Py_XDECREF(name);
    Py_XDECREF(flags);
    Py_XDECREF(flag);
    if (data != NULL) {
        free(data);
    }
    return ret;
}

static void escapeName(char ** sPtr, PyObject * strObj) {
    int size;
    char * s;
    char * r = *sPtr;

    /* dep names get : turned into :: */

    s = PYBYTES_AS_STRING(strObj);
    size = PYBYTES_GET_SIZE(strObj);

    while (size--) {
        if (*s == ':')
            *r++ = ':';
        *r++ = *s++;
    }

    *sPtr = r;
}

static void escapeFlags(char ** sPtr, PyObject * strObj) {
    int size;
    char * s;
    char * r = *sPtr;

    /* Flags get : turned to \: */

    s = PYBYTES_AS_STRING(strObj);
    size = PYBYTES_GET_SIZE(strObj);

    while (size--) {
        if (*s == ':')
            *r++ = '\\';
        *r++ = *s++;
    }

    *sPtr = r;
}

struct depFlag {
    PyObject * flag;
    int sense;
};

static int flagSort(const void * a, const void * b) {
    return strcmp(PYBYTES_AS_STRING( ((struct depFlag *) a)->flag),
                  PYBYTES_AS_STRING( ((struct depFlag *) b)->flag) );
}

static int depFreezeRaw(PyObject * nameObj, PyObject * dict,
                        char ** resultPtr, int * size) {
    /* Borrowed references */
    PyObject *itemTuple;
    PyObject *senseObj;
    /* Kept references */
    PyObject *itemList = NULL;
    int itemCount;
    int itemSize;
    int i, rc = -1;
    char * next, *result = NULL;
    struct depFlag *flags = NULL;

    if (!PYBYTES_CheckExact(nameObj)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return -1;
    }

    if (!PyDict_CheckExact(dict)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a dict");
        return -1;
    }

    itemList = PyDict_Items(dict);
    if (itemList == NULL) {
        return -1;
    }
    itemCount = PyList_GET_SIZE(itemList);
    flags = malloc(itemCount * sizeof(*flags));
    if (flags == NULL) {
        PyErr_NoMemory();
        goto cleanup;
    }
    memset(flags, 0, itemCount * sizeof(*flags));

    itemSize = 0;
    for (i = 0; i < itemCount; i++) {
        itemTuple = PyList_GET_ITEM(itemList, i);

        flags[i].flag = PyTuple_GET_ITEM(itemTuple, 0);
        senseObj = PyTuple_GET_ITEM(itemTuple, 1);

        if (!PYBYTES_CheckExact(flags[i].flag)) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be strings");
            goto cleanup;
        }

        if (!PYINT_CheckExact(senseObj)) {
            PyErr_SetString(PyExc_TypeError, "dict values must be ints");
            goto cleanup;
        }

        flags[i].sense = PYINT_AS_LONG(senseObj);
        itemSize += PYBYTES_GET_SIZE(flags[i].flag);
    }

    qsort(flags, itemCount, sizeof(*flags), flagSort);

    /* Frozen form is name:SENSEflag:SENSEflag. Worst case size for name/flag
       is * 2 due to : expansion */
    result = malloc((PYBYTES_GET_SIZE(nameObj) * 2) + 1 +
                    (itemSize * 2) + itemCount * 3);
    if (result == NULL) {
        PyErr_NoMemory();
        goto cleanup;
    }
    next = result;
    escapeName(&next, nameObj);

    for (i = 0; i < itemCount; i++) {
        *next++ = ':';
        switch (flags[i].sense) {
            case 1:                   /* PY_SENSE_REQUIRED */
                break;
            case 2:                   /* PY_SENSE_PREFERRED */
                *next++ = '~';
                break;
            case 3:                   /* PY_SENSE_PREFERNOT */
                *next++ = '~';
                *next++ = '!';
                break;
            case 4:                   /* PY_SENSE_DISALLOWED */
                *next++ = '!';
                break;
            default:
                PyErr_SetString(PyExc_TypeError, "unknown sense");
                goto cleanup;
        }

        escapeFlags(&next, flags[i].flag);
    }

    *size = next - result;
    *resultPtr = result;
    result = NULL;
    rc = 0;

cleanup:
    Py_XDECREF(itemList);
    if (result != NULL) {
        free(result);
    }
    if (flags != NULL) {
        free(flags);
    }
    return rc;
}

struct depList {
    char * className;
    PyObject * dep;
    char * frz;
    int frzSize;
};

static int depListSort(const void * a, const void * b) {
    return strcmp( ((struct depList *) a)->className,
                   ((struct depList *) b)->className);
}


static int depClassFreezeRaw(PyObject * tagObj, PyObject * dict,
                          char ** resultPtr, int * resultSizePtr) {
    /* Borrowed references */
    PyObject *tuple;
    /* Kept references */
    PyObject *depObjList = NULL;
    PyObject *nameObj = NULL, *flagsObj = NULL;
    int depCount, i, rc = -1;
    struct depList *depList = NULL;
    int totalSize, tagLen;
    char *next, *result = NULL;
    char tag[12];

    if (!PYINT_CheckExact(tagObj)) {
        PyErr_SetString(PyExc_TypeError, "'tag' attribute of dep class object must be an int");
        return -1;
    }

    if (!PyDict_CheckExact(dict)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a dict");
        return -1;
    }

    tagLen = snprintf(tag, sizeof(tag) - 1, "%d#", (int) PYINT_AS_LONG(tagObj));

    depObjList = PyDict_Items(dict);
    if (depObjList == NULL) {
        return -1;
    }
    depCount = PyList_GET_SIZE(depObjList);
    if (!depCount) {
        rc = 0;
        *resultPtr = NULL;
        *resultSizePtr = 0;
        goto cleanup;
    }

    depList = malloc(depCount * sizeof(*depList));
    if (!depList) {
        PyErr_NoMemory();
        goto cleanup;
    }
    memset(depList, 0, depCount * sizeof(*depList));
    for (i = 0; i < depCount; i++) {
        tuple = PyList_GET_ITEM(depObjList, i);
        if (!PYBYTES_CheckExact(PyTuple_GET_ITEM(tuple, 0))) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be strings");
            rc = -1;
            goto cleanup;
        }
        depList[i].className = PYBYTES_AS_STRING(PyTuple_GET_ITEM(tuple, 0));
        depList[i].dep = PyTuple_GET_ITEM(tuple, 1);
    }

    Py_CLEAR(depObjList);

    qsort(depList, depCount, sizeof(*depList), depListSort);

    totalSize = 0;
    for (i = 0; i < depCount; i++) {
        if (!(nameObj = PyObject_GetAttrString(depList[i].dep, "name"))) {
            rc = -1;
            goto cleanup;
        }
        if (!(flagsObj = PyObject_GetAttrString(depList[i].dep, "flags"))) {
            rc = -1;
            goto cleanup;
        }
        rc = depFreezeRaw(nameObj, flagsObj, &depList[i].frz, &depList[i].frzSize);
        Py_CLEAR(nameObj);
        Py_CLEAR(flagsObj);
        if (rc == -1) {
            goto cleanup;
        }
        /* Leave room for the tag and separator */
        totalSize += tagLen + depList[i].frzSize + 1;
    }

    result = malloc(totalSize);
    if (result == NULL) {
        PyErr_NoMemory();
        rc = -1;
        goto cleanup;
    }
    next = result;
    for (i = 0; i < depCount; i++) {
        memcpy(next, tag, tagLen);
        next += tagLen;
        memcpy(next, depList[i].frz, depList[i].frzSize);
        next += depList[i].frzSize;
        *next++ = '|';
    }

    /* chop off the trailing | */
    next--;

    *resultPtr = result;
    *resultSizePtr = next - result;
    result = NULL;
    rc = 0;

cleanup:
    Py_XDECREF(nameObj);
    Py_XDECREF(flagsObj);
    Py_XDECREF(depObjList);
    for (i = 0; i < depCount; i++) {
        if (depList[i].frz != NULL) {
            free(depList[i].frz);
        }
    }
    if (depList != NULL) {
        free(depList);
    }
    if (result != NULL) {
        free(result);
    }
    return rc;
}

struct depClassList {
    int tag;
    char * frz;
    int frzSize;
};

static int depClassSort(const void * a, const void * b) {
    int one = ((struct depClassList *) a)->tag;
    int two = ((struct depClassList *) b)->tag;

    if (one < two)
        return -1;
    else if (one == two)
        return 0;

    return 1;
}


static PyObject * depSetFreeze(PyObject * self, PyObject * args) {
    /* Borrowed references */
    PyObject *memberObjs;
    PyObject *depClass, *tuple, *rc = NULL;
    /* Kept references */
    PyObject *memberList = NULL;
    PyObject *tagObj = NULL;
    PyObject *classMembers = NULL;
    struct depClassList *members = NULL;
    int memberCount;
    char *result = NULL, *next;
    int i, totalSize;

    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "exactly one argument expected");
        return NULL;
    }

    memberObjs = PyTuple_GET_ITEM(args, 0);
    if (!PyDict_CheckExact(memberObjs)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a dict");
        return NULL;
    }

    memberList = PyDict_Items(memberObjs);
    memberCount = PyList_GET_SIZE(memberList);
    if (!memberCount) {
        Py_DECREF(memberList);
        return PYBYTES_FromString("");
    }

    members = malloc(sizeof(*members) * memberCount);
    if (members == NULL) {
        PyErr_NoMemory();
        goto cleanup;
    }
    memset(members, 0, sizeof(*members) * memberCount);

    totalSize = 0;
    for (i = 0; i < memberCount; i++) {
        tuple = PyList_GET_ITEM(memberList, i);

        if (!PYINT_CheckExact(PyTuple_GET_ITEM(tuple, 0))) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be ints");
            goto cleanup;
        }

        members[i].tag = PYINT_AS_LONG(PyTuple_GET_ITEM(tuple, 0));
        depClass = PyTuple_GET_ITEM(tuple, 1);

        if (!(tagObj = PyObject_GetAttrString(depClass, "tag"))) {
            goto cleanup;
        }

        if (!(classMembers =
                    PyObject_GetAttrString(depClass, "members"))) {
            goto cleanup;
        }

        if (depClassFreezeRaw(tagObj, classMembers, &members[i].frz,
                              &members[i].frzSize)) {
            goto cleanup;
        }

        totalSize += members[i].frzSize + 1;
        Py_CLEAR(classMembers);
        Py_CLEAR(tagObj);
    }

    Py_CLEAR(memberList);

    next = result = malloc(totalSize);
    if (result == NULL) {
        PyErr_NoMemory();
        goto cleanup;
    }
    qsort(members, memberCount, sizeof(*members), depClassSort);

    for (i = 0; i < memberCount; i++) {
        memcpy(next, members[i].frz, members[i].frzSize);
        next += members[i].frzSize;
        *next++ = '|';
    }

    /* chop off the trailing | */
    next--;

    rc = PYBYTES_FromStringAndSize(result, next - result);

cleanup:
    Py_XDECREF(classMembers);
    Py_XDECREF(tagObj);
    Py_XDECREF(memberList);
    if (result != NULL) {
        free(result);
    }
    if (members != NULL) {
        for (i = 0; i < memberCount; i++) {
            if (members[i].frz != NULL) {
                free(members[i].frz);
            }
        }
        free(members);
    }
    return rc;
}


PYMODULE_DECLARE(dep_freeze, methods, "Accelerated dep parsing and freezing");

/* vim: set sts=4 sw=4 expandtab : */
