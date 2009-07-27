/*
 *
 * Copyright (c) 2004-2008 rPath, Inc.
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

#include <ctype.h>
#include <errno.h>
#include <malloc.h>
#include <netinet/in.h>
#include <openssl/sha.h>
#include <resolv.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <unistd.h>
#include <zlib.h>

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static PyObject * depSetSplit(PyObject *self, PyObject *args);
static PyObject * depSplit(PyObject *self, PyObject *args);
static PyObject * depSetFreeze(PyObject *self, PyObject *args);
static PyObject * exists(PyObject *self, PyObject *args);
static PyObject * malloced(PyObject *self, PyObject *args);
static PyObject * removeIfExists(PyObject *self, PyObject *args);
static PyObject * mkdirIfMissing(PyObject *self, PyObject *args);
static PyObject * unpack(PyObject *self, PyObject *args);
static PyObject * pack(PyObject * self, PyObject * args);
static PyObject * dynamicSize(PyObject *self, PyObject *args);
static PyObject * sha1Copy(PyObject *self, PyObject *args);
static PyObject * sha1Uncompress(PyObject *self, PyObject *args);
static PyObject * py_pread(PyObject *self, PyObject *args);
static PyObject * py_massCloseFDs(PyObject *self, PyObject *args);
static PyObject * py_sendmsg(PyObject *self, PyObject *args);
static PyObject * py_recvmsg(PyObject *self, PyObject *args);
static PyObject * py_countOpenFDs(PyObject *self, PyObject *args);
static PyObject * py_res_init(PyObject *self, PyObject *args);
static PyObject * pyfchmod(PyObject *self, PyObject *args);

static PyMethodDef MiscMethods[] = {
    { "depSetSplit", depSetSplit, METH_VARARGS },
    { "depSplit", depSplit, METH_VARARGS },
    { "depSetFreeze", depSetFreeze, METH_VARARGS },
    { "exists", exists, METH_VARARGS,
        "returns a boolean reflecting whether a file (even a broken symlink) "
        "exists in the filesystem" },
    { "malloced", malloced, METH_VARARGS, 
	"amount of memory currently allocated through malloc()" },
    { "removeIfExists", removeIfExists, METH_VARARGS, 
	"unlinks a file if it exists; silently fails if it does not exist. "
	"returns a boolean indicating whether or not a file was removed" },
    { "mkdirIfMissing", mkdirIfMissing, METH_VARARGS,
        "Creates a directory if the file does not already exist. EEXIST "
        "is ignored." },
    { "sha1Copy", sha1Copy, METH_VARARGS },
    { "sha1Uncompress", sha1Uncompress, METH_VARARGS,
        "Uncompresses a gzipped file descriptor into another gzipped "
        "file descriptor and returns the sha1 of the uncompressed content. " },
    { "unpack", unpack, METH_VARARGS },
    { "pack", pack, METH_VARARGS },
    { "dynamicSize", dynamicSize, METH_VARARGS },
    { "pread", py_pread, METH_VARARGS },
    { "massCloseFileDescriptors", py_massCloseFDs, METH_VARARGS },
    { "sendmsg", py_sendmsg, METH_VARARGS },
    { "recvmsg", py_recvmsg, METH_VARARGS },
    { "countOpenFileDescriptors", py_countOpenFDs, METH_VARARGS },
    { "res_init", py_res_init, METH_VARARGS },
    { "fchmod", pyfchmod, METH_VARARGS },
    {NULL}  /* Sentinel */
};

static PyObject * malloced(PyObject *self, PyObject *args) {
    struct mallinfo ma;

    ma = mallinfo();

    /* worked */
    return Py_BuildValue("i", ma.uordblks);
}

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

    if (!PyInt_CheckExact(offsetArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be an int");
        return NULL;
    } else if (!PyString_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a string");
        return NULL;
    }

    offset = PyInt_AS_LONG(offsetArg);
    data = PyString_AS_STRING(dataArg);

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

    if (!PyString_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return NULL;
    }

    origData = PyString_AS_STRING(dataArg);

    /* Copy the original string over, replace single : with a '\0' and
       double :: with a single : */
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
        } else { 
            *endPtr++ = *chptr++;
        }
    }

    *endPtr++ = '\0';

    /* We're left with a '\0' separated list of name, flag1, ..., flagN. Get
       the name first. */
    name = PyString_FromString(data);
    chptr = data + strlen(data) + 1;

    flags = PyList_New(0);

    while (chptr < endPtr) {
        flag = PyString_FromString(chptr);
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

static void copyColonStr(char ** sPtr, PyObject * strObj) {
    int size;
    char * s;
    char * r = *sPtr;

    s = PyString_AS_STRING(strObj);
    size = PyString_GET_SIZE(strObj);

    while (size--) {
        if (*s == ':')
            *r++ = ':';
        *r++ = *s++;
    }

    *sPtr = r;
}

struct depFlag {
    PyObject * flag;
    int sense;
};

static int flagSort(const void * a, const void * b) {
    return strcmp(PyString_AS_STRING( ((struct depFlag *) a)->flag),
                  PyString_AS_STRING( ((struct depFlag *) b)->flag) );
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

    if (!PyString_CheckExact(nameObj)) {
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

        if (!PyString_CheckExact(flags[i].flag)) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be strings");
            Py_DECREF(itemList);
            return -1;
        }

        if (!PyInt_CheckExact(senseObj)) {
            PyErr_SetString(PyExc_TypeError, "dict values must be ints");
            Py_DECREF(itemList);
            return -1;
        }

        flags[i].sense = PyInt_AS_LONG(senseObj);
        itemSize += PyString_GET_SIZE(flags[i].flag);
    }

    qsort(flags, itemCount, sizeof(*flags), flagSort);

    /* Frozen form is name:SENSEflag:SENSEflag. Worst case size for name/flag
       is * 2 due to : expansion */
    result = malloc((PyString_GET_SIZE(nameObj) * 2) + 1 +
                    (itemSize * 2) + itemCount * 3);
    next = result;
    copyColonStr(&next, nameObj);

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

        copyColonStr(&next, flags[i].flag);
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

    if (!PyInt_CheckExact(tagObj)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be an int");
        free(depList);
        return -1;
    }

    if (!PyDict_CheckExact(dict)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a dict");
        return -1;
    }

    tagLen = sprintf(tag, "%d#", (int) PyInt_AS_LONG(tagObj));

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
        if (!PyString_CheckExact(PyTuple_GET_ITEM(tuple, 0))) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be strings");
            Py_DECREF(depObjList);
            free(depList);
            return -1;
        }
        depList[i].className = PyString_AS_STRING(PyTuple_GET_ITEM(tuple, 0));
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
        return PyString_FromString("");
    }

    members = malloc(sizeof(*members) * memberCount);

    totalSize = 0;
    for (i = 0; i < memberCount; i++) {
        tuple = PyList_GET_ITEM(memberList, i);

        if (!PyInt_CheckExact(PyTuple_GET_ITEM(tuple, 0))) {
            PyErr_SetString(PyExc_TypeError, "dict keys must be ints");
            Py_DECREF(memberList);
            free(members);
            return NULL;
        }

        members[i].tag = PyInt_AS_LONG(PyTuple_GET_ITEM(tuple, 0));
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
    rc = PyString_FromStringAndSize(result, next - result);
    free(result);
    return rc;
}

static PyObject * exists(PyObject *self, PyObject *args) {
    char * fn;
    struct stat sb;

    if (!PyArg_ParseTuple(args, "s", &fn))
        return NULL;

    if (lstat(fn, &sb)) {
        if (errno == ENOENT || errno == ENOTDIR || errno == ENAMETOOLONG || errno == EACCES) {
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

static PyObject * mkdirIfMissing(PyObject *self, PyObject *args) {
    char * fn;

    if (!PyArg_ParseTuple(args, "s", &fn))
        return NULL;

    /* 0777 lets umask do it's thing */
    if (mkdir(fn, 0777)) {
        if (errno == EEXIST) {
            Py_INCREF(Py_False);
            return Py_False;
        }

        PyErr_SetFromErrnoWithFilename(PyExc_OSError, fn);
        return NULL;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

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
    PyObject * formatArg, * arg, * resultObj;
    char * format, * formatPtr, * s, * result;
    int argCount;
    int strLen;
    int argNum;
    int len, i;
    unsigned char oneByte;
    unsigned short twoBytes;
    unsigned int fourBytes;

    formatArg = PyTuple_GET_ITEM(args, 0);
    if (!PyString_CheckExact(formatArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return NULL;
    }

    formatPtr = format = PyString_AS_STRING(formatArg);

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
                if (!PyInt_CheckExact(arg)) {
                    PyErr_SetString(PyExc_TypeError,
                                    "argument for B format must be an int");
                    return NULL;
                }
                strLen += 1;
                break;

            case 'S':
                arg = PyTuple_GET_ITEM(args, argNum++);
                len = PyString_GET_SIZE(arg);
                if (!PyString_CheckExact(arg)) {
                    PyErr_SetString(PyExc_TypeError,
                                    "argument for S format must be a str");
                    return NULL;
                }
                s = PyString_AS_STRING(arg);

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
    argNum = 1;
    strLen = 0;
    formatPtr = format + 1;
    while (*formatPtr) {
        switch (*formatPtr++) {
            case 'B':
                arg = PyTuple_GET_ITEM(args, argNum++);
                oneByte = PyInt_AS_LONG(arg);
                result[strLen++] = oneByte;
                break;

            case 'S':
                arg = PyTuple_GET_ITEM(args, argNum++);
                s = PyString_AS_STRING(arg);
                len = PyString_GET_SIZE(arg);

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
                        return NULL;
                    }
                } else {
                    PyErr_SetString(PyExc_RuntimeError,
                                    "internal pack error 1");
                    return NULL;
                }


                memcpy(result + strLen, s, len);
                strLen += len;
                break;

            default:
                PyErr_SetString(PyExc_RuntimeError,
                                "internal pack error 2");
                return NULL;
        }
    }

    resultObj = PyString_FromStringAndSize(result, strLen);
    return resultObj;
}

static PyObject * unpack(PyObject *self, PyObject *args) {
    char * data, * format;
    char * dataPtr, * formatPtr;
    char b;
    int dataLen;
    int offset;
    PyObject * retList, * dataObj;
    unsigned int intVal;
    PyObject * formatArg, * offsetArg, * dataArg, * retVal;

    /* This avoids PyArg_ParseTuple because it's sloooow */
    if (PyTuple_GET_SIZE(args) != 3) {
        PyErr_SetString(PyExc_TypeError, "exactly three arguments expected");
        return NULL;
    }

    formatArg = PyTuple_GET_ITEM(args, 0);
    offsetArg = PyTuple_GET_ITEM(args, 1);
    dataArg = PyTuple_GET_ITEM(args, 2);

    if (!PyString_CheckExact(formatArg)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be a string");
        return NULL;
    } else if (!PyInt_CheckExact(offsetArg)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be an int");
        return NULL;
    } else if (!PyString_CheckExact(dataArg)) {
        PyErr_SetString(PyExc_TypeError, "third argument must be a string");
        return NULL;
    }

    format = PyString_AS_STRING(formatArg);
    offset = PyInt_AS_LONG(offsetArg);
    data = PyString_AS_STRING(dataArg);
    dataLen = PyString_GET_SIZE(dataArg);

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
          case 'B':
            intVal = (int) *dataPtr++;
            dataObj = PyInt_FromLong(intVal);
            PyList_Append(retList, dataObj);
            Py_DECREF(dataObj);
            formatPtr++;
            break;

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
            } else if (*formatPtr == 'I') {
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
                                "# must be followed by H or I in format");
                return NULL;
            }

            dataObj = PyString_FromStringAndSize(dataPtr, intVal);
            PyList_Append(retList, dataObj);
            Py_DECREF(dataObj);
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
		intVal = ntohl(*((uint32_t *) dataPtr)) & 0x3fffffff;
                dataPtr += sizeof(uint32_t);
	    } else if ((b & 0xc0) == 0x40) {
		/* 14 bit length */
		intVal = ntohs(*((uint16_t *) dataPtr)) & 0x3fff;
		dataPtr += sizeof(uint16_t);
	    } else if ((b & 0xc0) == 0x00) {
		/* 6 bit length */
		intVal = *((uint8_t *) dataPtr) & ~(1 << 6);
		dataPtr += sizeof(uint8_t);
	    } else {
		PyErr_SetString(PyExc_ValueError, 
				"unimplemented dynamic size");
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

    retVal = Py_BuildValue("iO", dataPtr - data, retList);
    Py_DECREF(retList);

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
    if (!PyInt_CheckExact(sizeArg)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be a string");
        return NULL;
    }

    size = PyInt_AS_LONG(sizeArg);
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
    return PyString_FromStringAndSize(sizebuf, sizelen);
}

static PyObject * py_pread(PyObject *self, PyObject *args) {
    void * data;
    int fd;
    size_t size;
    off_t offset, rc;
    PyObject *pysize, *pyfd, *pyoffset, *buf;

    if (PyTuple_GET_SIZE(args) != 3) {
        PyErr_SetString(PyExc_TypeError, "exactly three arguments expected");
        return NULL;
    }

    pyfd = PyTuple_GET_ITEM(args, 0);
    pysize = PyTuple_GET_ITEM(args, 1);
    pyoffset = PyTuple_GET_ITEM(args, 2);

    if (!PyInt_CheckExact(pyfd)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be an int");
        return NULL;
    } else if (!PyInt_CheckExact(pysize) &&
	       !PyLong_CheckExact(pysize)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be an int or long");
        return NULL;
    } else if (!PyInt_CheckExact(pyoffset) &&
	       !PyLong_CheckExact(pyoffset)) {
        PyErr_SetString(PyExc_TypeError, "third argument must be an int or long");
        return NULL;
    }

    fd = PyInt_AS_LONG(pyfd);
    size = PyLong_AsUnsignedLong(pysize);
    if (PyErr_Occurred())
        return NULL;

    /* sizeof(off_t) is 8 (same as long long) */
    if (PyInt_CheckExact(pyoffset))
        offset = PyLong_AsUnsignedLong(pyoffset);
    else /* A PyLong_Type to be converted to a long long */
        offset = PyLong_AsUnsignedLongLong(pyoffset);
    if (PyErr_Occurred())
        return NULL;

    data = malloc(size);

    if (NULL == data) {
	PyErr_NoMemory();
	return NULL;
    }

    rc = pread(fd, data, size, offset);
    if (-1 == rc) {
	free(data);
        PyErr_SetFromErrno(PyExc_OSError);
	return NULL;
    }

    buf = PyString_FromStringAndSize(data, rc);
    free(data);
    return buf;
}

static PyObject * py_massCloseFDs(PyObject *self, PyObject *args) {
    int start, contcount, end, i, count;
    PyObject *pystart, *pycontcount, *pyend;

    if (PyTuple_GET_SIZE(args) != 3) {
        PyErr_SetString(PyExc_TypeError, "exactly three arguments expected");
        return NULL;
    }

    pystart = PyTuple_GET_ITEM(args, 0);
    pycontcount = PyTuple_GET_ITEM(args, 1);
    pyend = PyTuple_GET_ITEM(args, 2);

    if (!PyInt_CheckExact(pystart)) {
        PyErr_SetString(PyExc_TypeError, "first argument must be an int");
        return NULL;
    } else if (!PyInt_CheckExact(pycontcount)) {
        PyErr_SetString(PyExc_TypeError, "second argument must be an int");
        return NULL;
    } else if (!PyInt_CheckExact(pyend)) {
        PyErr_SetString(PyExc_TypeError, "third argument must be an int");
        return NULL;
    }

    start = PyLong_AsUnsignedLong(pystart);
    contcount = PyLong_AsUnsignedLong(pycontcount);
    end = PyLong_AsUnsignedLong(pyend);
    if (PyErr_Occurred())
        return NULL;

    if (((contcount ? 1 : 0) ^ (end ? 1 : 0)) == 0) {
        PyErr_SetString(PyExc_TypeError, "Exactly one of the second and third "
                                         "argument must be zero");
        return NULL;
    }

    i = start - 1;
    count = contcount;
    while (1) {
        int ret;
        i++;
        if (contcount) {
            /* Requested to stop after a continous number of closed fds */
            if (count == 0) {
                break;
            }
        } else if (i == end) {
            /* Requested to stop at the end */
            break;
        }
        ret = close(i);
        if (ret == 0) {
            /* Successful close; reset continous count */
            count = contcount;
            continue;
        }
        if (errno == EBADF) {
            count--;
            continue;
        }
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

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
        if (!PyString_Check(sObj)) {
            PyErr_SetString(PyExc_TypeError,
                            "data objects must be strings");
            return NULL;
        }

        vectors[i].iov_base = PyString_AS_STRING(sObj);
        vectors[i].iov_len = PyString_GET_SIZE(sObj);
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
        if (!PyInt_Check(intObj)) {
            PyErr_SetString(PyExc_TypeError,
                            "integer file descriptor required");
            return NULL;
        }

        sendFds[i] = PyInt_AS_LONG(intObj);
    }

    if ((bytes = sendmsg(fd, &msg, 0)) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    return PyInt_FromLong(bytes);
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
        PyTuple_SET_ITEM(fdTuple, i, PyInt_FromLong(recvFds[i]));
    }

    if (fdCount) {
        rc = Py_BuildValue("s#O", vector.iov_base, bytes, fdTuple);
    } else {
        rc = PyString_FromStringAndSize(vector.iov_base, bytes);
    }
    free(vector.iov_base);

    return rc;
}

static PyObject * py_countOpenFDs(PyObject *module, PyObject *args)
{
    int vfd, i, maxfd, ret;
    struct pollfd *ufds;

    /* Count the number of open file descriptors */

    maxfd = getdtablesize();
    /* Don't worry about freeing ufds */
    ufds = (struct pollfd *)alloca(maxfd * sizeof(struct pollfd));

    for (i = 0; i < maxfd; i++)
      {
        ufds[i].fd = i;
        ufds[i].events = POLLIN | POLLPRI | POLLOUT;
      }

    /* We need to loop, in case poll is interrupted by a signal */
    while (1)
      {
        ret = poll(ufds, maxfd, 0);
        if (ret >= 0) /* No error */
            break;
        /* ret == -1 */
        if (errno == EINTR) /* A signal occurred. Retry */
            continue;
        /* Real failure */
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
      }

    for (i = 0, vfd = 0; i < maxfd; i++)
        if (ufds[i].revents != POLLNVAL)
            vfd++;

    return PyInt_FromLong(vfd);
}

static PyObject * sha1Copy(PyObject *module, PyObject *args) {
    off_t inFd, inSize, inStart, inStop, inAt;
    PyObject * outFdList, *pyInStart, *pyInSize;
    int * outFds, outFdCount, i, rc, inflate_rc;
    uint8_t inBuf[1024 * 256];
    uint8_t outBuf[1024 * 256];
    SHA_CTX sha1state;
    z_stream zs;
    uint8_t sha1[20];

    if (!PyArg_ParseTuple(args, "(iOO)O!", &inFd, &pyInStart, &pyInSize,
                          &PyList_Type, &outFdList ))
        return NULL;
    if (!PyInt_CheckExact(pyInStart) &&
	!PyLong_CheckExact(pyInStart)) {
        PyErr_SetString(PyExc_TypeError, "second item in first argument must be an int or long");
        return NULL;
    }
    if (!PyInt_CheckExact(pyInSize) &&
	!PyLong_CheckExact(pyInSize)) {
        PyErr_SetString(PyExc_TypeError, "third item in first argument must be an int or long");
        return NULL;
    }

    if (PyInt_CheckExact(pyInStart))
	inStart = PyLong_AsUnsignedLong(pyInStart);
    else
	inStart = PyLong_AsUnsignedLongLong(pyInStart);
    if (inStart == (off_t) -1)
	return NULL;

    if (PyInt_CheckExact(pyInSize))
	inSize = PyLong_AsUnsignedLong(pyInSize);
    else
	inSize = PyLong_AsUnsignedLongLong(pyInSize);
    if (inSize == (off_t) -1)
	return NULL;

    outFdCount = PyList_Size(outFdList);
    outFds = alloca(sizeof(*outFds) * outFdCount);
    for (i = 0; i < outFdCount; i++)
        outFds[i] = PyInt_AS_LONG(PyList_GET_ITEM(outFdList, i));

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
            zs.avail_in = MIN(sizeof(inBuf), inStop - inAt);
            zs.next_in = inBuf;
            rc = pread(inFd, inBuf, zs.avail_in, inAt);
            inAt += zs.avail_in;
            if (rc == -1) {
                PyErr_SetFromErrno(PyExc_OSError);
                return NULL;
            }
            if (rc != zs.avail_in) {
                PyErr_SetString(PyExc_RuntimeError, "short pread");
                return NULL;
            }

            for (i = 0; i < outFdCount; i++) {
                rc = write(outFds[i], inBuf, zs.avail_in); 
                if (rc == -1) {
                    PyErr_SetFromErrno(PyExc_OSError);
                    return NULL;
                }
                if (rc != zs.avail_in) {
                    PyErr_SetString(PyExc_RuntimeError, "short write");
                    return NULL;
                }
            }
        }

        zs.avail_out = sizeof(outBuf);
        zs.next_out = outBuf;
        inflate_rc = inflate(&zs, 0);
        if (inflate_rc < 0) {
            PyErr_SetString(PyExc_RuntimeError, zError(rc));
            return NULL;
        }

        i = sizeof(outBuf) - zs.avail_out;
        SHA1_Update(&sha1state, outBuf, i);
    }

    if ((rc = inflateEnd(&zs)) != Z_OK) {
        PyErr_SetString(PyExc_RuntimeError, zError(rc));
        return NULL;
    }

    SHA1_Final(sha1, &sha1state);

    return PyString_FromStringAndSize((char*)sha1, sizeof(sha1));
}

static PyObject * sha1Uncompress(PyObject *module, PyObject *args) {
    int inFd, outFd = -1, i, rc, inflate_rc;
    off_t inStop, inAt, inSize, inStart;
    PyObject *pyInStart, *pyInSize;
    z_stream zs;
    uint8_t inBuf[1024 * 256];
    uint8_t outBuf[1024 * 256];
    SHA_CTX sha1state;
    uint8_t sha1[20];
    char * path, * baseName;
    struct stat sb;
    char * tmpPath = NULL, * targetPath;

    if (!PyArg_ParseTuple(args, "(iOO)sss", &inFd, &pyInStart, &pyInSize,
			  &path, &baseName, &targetPath))
        goto onerror;

    if (!PyInt_CheckExact(pyInStart) &&
	!PyLong_CheckExact(pyInStart)) {
        PyErr_SetString(PyExc_TypeError, "second item in first argument must be an int or long");
        goto onerror;
    }
    if (!PyInt_CheckExact(pyInSize) &&
	       !PyLong_CheckExact(pyInSize)) {
        PyErr_SetString(PyExc_TypeError, "third item in first argument must be an int or long");
        goto onerror;
    }

    if (PyInt_CheckExact(pyInStart))
	inStart = PyLong_AsUnsignedLong(pyInStart);
    else
	inStart = PyLong_AsUnsignedLongLong(pyInStart);
    if (inStart == (off_t) -1)
        goto onerror;

    if (PyInt_CheckExact(pyInSize))
	inSize = PyLong_AsUnsignedLong(pyInSize);
    else
	inSize = PyLong_AsUnsignedLongLong(pyInSize);
    if (inSize == (off_t) -1)
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
            zs.avail_in = MIN(sizeof(inBuf), inStop - inAt);
            zs.next_in = inBuf;
            rc = pread(inFd, inBuf, zs.avail_in, inAt);
            inAt += zs.avail_in;
            if (rc == -1) {
                PyErr_SetFromErrno(PyExc_OSError);
                goto onerror;
            }
            if (rc != zs.avail_in) {
                PyErr_SetString(PyExc_RuntimeError, "short pread");
                goto onerror;
            }
        }

        zs.avail_out = sizeof(outBuf);
        zs.next_out = outBuf;
        inflate_rc = inflate(&zs, 0);
        if (inflate_rc < 0) {
            PyErr_SetString(PyExc_RuntimeError, zError(rc));
            goto onerror;
        }

        i = sizeof(outBuf) - zs.avail_out;
        SHA1_Update(&sha1state, outBuf, i);
        rc = write(outFd, outBuf, i);
        if (rc == -1) {
            PyErr_SetFromErrno(PyExc_OSError);
            goto onerror;
        }
        if (rc != i) {
            PyErr_SetString(PyExc_RuntimeError, "short write");
            goto onerror;
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

    rc = stat(targetPath, &sb);
    if (rc && errno != ENOENT) {
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

    return PyString_FromStringAndSize((char*)sha1, sizeof(sha1));

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

static PyObject * pyfchmod(PyObject *self, PyObject *args) {
    int fd, mode;

    if (!PyArg_ParseTuple(args, "ii", &fd, &mode))
        return NULL;

    if (fchmod(fd, mode)) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

PyMODINIT_FUNC
initmisc(void)
{
    Py_InitModule3("misc", MiscMethods, 
		   "miscelaneous low-level C functions for conary");
}


/* vim: set sts=4 sw=4 expandtab : */
