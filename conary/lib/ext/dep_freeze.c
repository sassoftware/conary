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
    char * data, * dataPtr, * endPtr;
    int offset, tag;
    PyObject * retVal;
    PyObject * offsetArg, * dataArg;

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
    data = PYBYTES_AS_STRING(dataArg);

    dataPtr = data + offset;
    /* this while is a cheap goto for the error case */
    while (*dataPtr) {
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

        /* Now look for the frozen dependency */
        /* Grab the tag first */
        while (*endPtr && *endPtr != '|')
            endPtr++;

        retVal = Py_BuildValue("iis#", endPtr - data + 1, tag, dataPtr,
                                endPtr - dataPtr);
        return retVal;
    }

    PyErr_SetString(PyExc_ValueError, "invalid frozen dependency");
    return NULL;
}

static PyObject * depSplit(PyObject *self, PyObject *args) {
    char * origData, * data, * chptr, * endPtr;
    PyObject * flags, * flag, * name, * ret, * dataArg;

    /* This avoids PyArg_ParseTuple because it's sloooow */
    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "exactly one argument expected");
        return NULL;
    }

    dataArg = PyTuple_GET_ITEM(args, 0);

    if (!PYBYTES_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return NULL;
    }

    origData = PYBYTES_AS_STRING(dataArg);

    /* Copy the original string over, replace single : with a '\0' and
       double :: with a single :, and \X with X (where X is anything,
       including backslash)  */
    endPtr = data = malloc(strlen(origData) + 1);
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
    name = PYBYTES_FromString(data);
    chptr = data + strlen(data) + 1;

    flags = PyList_New(0);

    while (chptr < endPtr) {
        flag = PYBYTES_FromString(chptr);
        PyList_Append(flags, flag);
        Py_DECREF(flag);
        chptr += strlen(chptr) + 1;
    }

    ret = PyTuple_Pack(2, name, flags);
    Py_DECREF(name);
    Py_DECREF(flags);
    free(data);
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
    PyObject * itemList;
    PyObject * itemTuple;
    PyObject * senseObj;
    int itemCount;
    int itemSize;
    int i;
    char * next, * result;
    struct depFlag * flags;

    if (!PYBYTES_CheckExact(nameObj)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return -1;
    }

    if (!PyDict_CheckExact(dict)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a dict");
        return -1;
    }

    itemList = PyDict_Items(dict);
    itemCount = PyList_GET_SIZE(itemList);
    flags = malloc(itemCount * sizeof(*flags));
    itemSize = 0;
    for (i = 0; i < itemCount; i++) {
        itemTuple = PyList_GET_ITEM(itemList, i);

        flags[i].flag = PyTuple_GET_ITEM(itemTuple, 0);
        senseObj = PyTuple_GET_ITEM(itemTuple, 1);

        if (!PYBYTES_CheckExact(flags[i].flag)) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be strings");
            Py_DECREF(itemList);
            return -1;
        }

        if (!PYINT_CheckExact(senseObj)) {
            PyErr_SetString(PyExc_TypeError, "dict values must be ints");
            Py_DECREF(itemList);
            return -1;
        }

        flags[i].sense = PYINT_AS_LONG(senseObj);
        itemSize += PYBYTES_GET_SIZE(flags[i].flag);
    }

    qsort(flags, itemCount, sizeof(*flags), flagSort);

    /* Frozen form is name:SENSEflag:SENSEflag. Worst case size for name/flag
       is * 2 due to : expansion */
    result = malloc((PYBYTES_GET_SIZE(nameObj) * 2) + 1 +
                    (itemSize * 2) + itemCount * 3);
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
                free(result);
                free(flags);
                Py_DECREF(itemList);
                PyErr_SetString(PyExc_TypeError, "unknown sense");
                return -1;
        }

        escapeFlags(&next, flags[i].flag);
    }

    *size = next - result;
    *resultPtr = result;
    free(flags);
    Py_DECREF(itemList);

    return 0;
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

/* This leaks memory on error. Oh well. */
static int depClassFreezeRaw(PyObject * tagObj, PyObject * dict,
                          char ** resultPtr, int * resultSizePtr) {
    PyObject * depObjList, * tuple;
    PyObject * nameObj, * flagsObj;
    int depCount, i, rc;
    struct depList * depList;
    int totalSize, tagLen;
    char * result, * next;
    char tag[12];

    if (!PYINT_CheckExact(tagObj)) {
        PyErr_SetString(PyExc_TypeError, "'tag' attribute of dep class object must be an int");
        return -1;
    }

    if (!PyDict_CheckExact(dict)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a dict");
        return -1;
    }

    tagLen = sprintf(tag, "%d#", (int) PYINT_AS_LONG(tagObj));

    depObjList = PyDict_Items(dict);
    depCount = PyList_GET_SIZE(depObjList);
    if (!depCount) {
        Py_DECREF(depObjList);
        *resultPtr = NULL;
        *resultSizePtr = 0;
        return 0;
    }

    depList = malloc(depCount * sizeof(*depList));
    for (i = 0; i < depCount; i++) {
        tuple = PyList_GET_ITEM(depObjList, i);
        if (!PYBYTES_CheckExact(PyTuple_GET_ITEM(tuple, 0))) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be strings");
            Py_DECREF(depObjList);
            free(depList);
            return -1;
        }
        depList[i].className = PYBYTES_AS_STRING(PyTuple_GET_ITEM(tuple, 0));
        depList[i].dep = PyTuple_GET_ITEM(tuple, 1);
    }

    Py_DECREF(depObjList);

    qsort(depList, depCount, sizeof(*depList), depListSort);

    totalSize = 0;
    for (i = 0; i < depCount; i++) {
        if (!(nameObj = PyObject_GetAttrString(depList[i].dep, "name"))) {
            free(depList);
            return -1;
        }

        if (!(flagsObj = PyObject_GetAttrString(depList[i].dep, "flags"))) {
            free(depList);
            return -1;
        }

        rc = depFreezeRaw(nameObj, flagsObj, &depList[i].frz, &depList[i].frzSize);

        Py_DECREF(nameObj);
        Py_DECREF(flagsObj);

        if (rc == -1) {
            free(depList);
            return -1;
        }

        totalSize += depList[i].frzSize;
    }

    /* 15 leaves plenty of room for the tag integer and the # */
    result = malloc((depCount * 15) + totalSize);
    next = result;
    for (i = 0; i < depCount; i++) {
        /* is sprintf really the best we can do? */
        strcpy(next, tag);
        next += tagLen;
        memcpy(next, depList[i].frz, depList[i].frzSize);
        free(depList[i].frz);
        next += depList[i].frzSize;
        *next++ = '|';
    }

    /* chop off the trailing | */
    next--;

    *resultPtr = result;
    *resultSizePtr = next - result;

    return 0;
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

/* leaks memory on error */
static PyObject * depSetFreeze(PyObject * self, PyObject * args) {
    PyObject * memberObjs, * memberList;
    PyObject * depClass, * tuple, * rc;
    PyObject * tagObj, * classMembers;
    struct depClassList * members;
    int memberCount;
    char * result, * next;
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

    totalSize = 0;
    for (i = 0; i < memberCount; i++) {
        tuple = PyList_GET_ITEM(memberList, i);

        if (!PYINT_CheckExact(PyTuple_GET_ITEM(tuple, 0))) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be ints");
            Py_DECREF(memberList);
            free(members);
            return NULL;
        }

        members[i].tag = PYINT_AS_LONG(PyTuple_GET_ITEM(tuple, 0));
        depClass = PyTuple_GET_ITEM(tuple, 1);

        if (!(tagObj = PyObject_GetAttrString(depClass, "tag"))) {
            free(members);
            Py_DECREF(memberList);
            return NULL;
        }

        if (!(classMembers =
                    PyObject_GetAttrString(depClass, "members"))) {
            free(members);
            Py_DECREF(memberList);
            Py_DECREF(tagObj);
            return NULL;
        }

        if (depClassFreezeRaw(tagObj, classMembers, &members[i].frz,
                              &members[i].frzSize)) {
            Py_DECREF(memberList);
            Py_DECREF(tagObj);
            Py_DECREF(classMembers);
            free(members);
            return NULL;
        }

        totalSize += members[i].frzSize + 1;
    }

    Py_DECREF(memberList);

    next = result = malloc(totalSize);
    qsort(members, memberCount, sizeof(*members), depClassSort);

    for (i = 0; i < memberCount; i++) {
        memcpy(next, members[i].frz, members[i].frzSize);
        next += members[i].frzSize;
        *next++ = '|';
        free(members[i].frz);
    }

    /* chop off the trailing | */
    next--;

    free(members);
    rc = PYBYTES_FromStringAndSize(result, next - result);
    free(result);
    return rc;
}


PYMODULE_DECLARE(dep_freeze, methods, "Accelerated dep parsing and freezing");

/* vim: set sts=4 sw=4 expandtab : */
