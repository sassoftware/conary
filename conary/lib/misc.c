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
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/poll.h>
#include <unistd.h>

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static PyObject * depSetSplit(PyObject *self, PyObject *args);
static PyObject * depSplit(PyObject *self, PyObject *args);
static PyObject * exists(PyObject *self, PyObject *args);
static PyObject * malloced(PyObject *self, PyObject *args);
static PyObject * removeIfExists(PyObject *self, PyObject *args);
static PyObject * mkdirIfMissing(PyObject *self, PyObject *args);
static PyObject * unpack(PyObject *self, PyObject *args);
static PyObject * dynamicSize(PyObject *self, PyObject *args);
static PyObject * py_pread(PyObject *self, PyObject *args);
static PyObject * py_massCloseFDs(PyObject *self, PyObject *args);
static PyObject * py_sendmsg(PyObject *self, PyObject *args);
static PyObject * py_recvmsg(PyObject *self, PyObject *args);
static PyObject * py_countOpenFDs(PyObject *self, PyObject *args);

static PyMethodDef MiscMethods[] = {
    { "depSetSplit", depSetSplit, METH_VARARGS },
    { "depSplit", depSplit, METH_VARARGS },
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
    { "unpack", unpack, METH_VARARGS },
    { "dynamicSize", dynamicSize, METH_VARARGS },
    { "pread", py_pread, METH_VARARGS },
    { "massCloseFileDescriptors", py_massCloseFDs, METH_VARARGS },
    { "sendmsg", py_sendmsg, METH_VARARGS },
    { "recvmsg", py_recvmsg, METH_VARARGS },
    { "countOpenFileDescriptors", py_countOpenFDs, METH_VARARGS },
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
    int fd, i;
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
        Py_DECREF(intObj);
    }

    if (sendmsg(fd, &msg, 0) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
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

    expectedLen = sizeof(*ctrlMsg) + (sizeof(fd) * fdCount);
    msg.msg_controllen = expectedLen;
    msg.msg_control = alloca(msg.msg_controllen);
    ctrlMsg = msg.msg_control;

    ctrlMsg->cmsg_len = msg.msg_controllen;
    ctrlMsg->cmsg_level = SOL_SOCKET;
    ctrlMsg->cmsg_type = SCM_RIGHTS;

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
        PyErr_SetString(PyExc_IOError, "unexpected control data size");
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

    rc = Py_BuildValue("s#O", vector.iov_base, bytes, fdTuple);
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

PyMODINIT_FUNC
initmisc(void)
{
    Py_InitModule3("misc", MiscMethods, 
		   "miscelaneous low-level C functions for conary");
}
