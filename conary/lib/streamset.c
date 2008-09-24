/*
 * Copyright (c) 2005-2007 rPath, Inc.
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
 *
 */

#include <Python.h>
#include <netinet/in.h>

#include "cstreams.h"

#include <stdio.h>

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

#define ALLOCA_CUTOFF 2048

staticforward PyTypeObject StreamSetDefType;

/* ------------------------------------- */
/* Object definitions                    */

struct tagInfo {
    unsigned int tag;
    int size;
    PyObject * name;
    PyObject * type;
};

typedef struct {
    PyObject_HEAD
    struct tagInfo * tags;
    int tagCount;
} StreamSetDefObject;

typedef struct {
    PyObject_HEAD
    int unknownCount;
    struct unknownTags {
        unsigned int tag;
        int sizeType;
        PyObject * data;
    } * unknownTags;
} StreamSetObject;

static int Thaw_raw(PyObject * self, StreamSetDefObject * ssd,
		    char * data, int dataLen, int offset);

/* ------------------------------------- */
/* StreamSetDef Implementation           */

static void StreamSetDef_Dealloc(PyObject * self) {
    StreamSetDefObject * ssd = (StreamSetDefObject *) self;
    free(ssd->tags);
    self->ob_type->tp_free(self);
}

static int StreamSetDef_Init(PyObject * self, PyObject * args,
			     PyObject * kwargs) {
    static char * kwlist[] = { "spec", NULL };
    StreamSetDefObject * ssd = (void *) self;
    PyObject * spec;
    PyListObject * items;
    int i, j;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!", kwlist,
				     &PyDict_Type, &spec)) {
        return -1;
    }

    items = (PyListObject *) PyDict_Items(spec);
    assert(PyList_Check(items));

    ssd->tagCount = items->ob_size;
    ssd->tags = malloc(items->ob_size * sizeof(*ssd->tags));
    if (ssd->tags == NULL) {
	PyErr_NoMemory();
	return -1;
    }

    for (i = 0; i < items->ob_size; i++) {
	int tag;
	PyObject * streamType;
	int size;
	char * name;

	if (!PyArg_ParseTuple(items->ob_item[i], "i(iOs)",
			      &tag, &size, &streamType, &name)) {
	    return -1;
	}

	ssd->tags[i].tag = tag;
	ssd->tags[i].size = size;
	ssd->tags[i].name = PyString_FromString(name);
	ssd->tags[i].type = streamType;
	Py_INCREF(streamType);
    }

    /* simple bubble sort */
    for (i = 0; i < ssd->tagCount - 1; i++) {
	for (j = 0; j < ssd->tagCount - i - 1; j++) {
	    if (ssd->tags[j + 1].tag < ssd->tags[j].tag) {
		struct tagInfo tmp;
		tmp = ssd->tags[j];
		ssd->tags[j] = ssd->tags[j + 1];
		ssd->tags[j + 1] = tmp;
	    }
	}
    }

    return 0;
}

/* ------------------------------------- */
/* StreamSet Implementation              */

static StreamSetDefObject * StreamSet_GetSSD(PyTypeObject *o) {
    /* returns a borrowed reference to the ssd */
    PyObject *sd;
    int rc;
    PyObject *arg;
    StreamSetDefObject * ssd;
    
    /* This looks in the class itself, not in the object or in parent classes */
    ssd = (void *) PyDict_GetItemString(o->tp_dict, "_streamDict");
    if (ssd) {
	if (ssd->ob_type != &StreamSetDefType) {
	    PyErr_SetString(PyExc_TypeError,
			    "_streamDict attribute must be a "
			    "cstreams.StreamSetDef");
	    return NULL;
	}
	return (StreamSetDefObject *) ssd;
    }
    /* no ssd yet -- let's create it */
    /* clear the old error */
    PyErr_Clear();

    sd = PyObject_GetAttrString((PyObject *) o, "streamDict");
    if (!sd) {
	char *buf;
	int len = 50 + strlen(o->tp_name);
	buf = malloc(len);
	if (buf == NULL) {
	    PyErr_NoMemory();
	    return NULL;
	}
	len = snprintf(buf, len,
		       "%s class is missing the streamDict class variable",
		       o->tp_name);
	PyErr_SetString(PyExc_ValueError, buf);
	free(buf);
	return NULL;
    }

    ssd = (void *) PyObject_New(StreamSetDefObject, &StreamSetDefType);
    if (NULL == ssd)
	return NULL;
    arg = PyTuple_New(1);
    PyTuple_SetItem(arg, 0, sd);
    rc = StreamSetDef_Init((PyObject *) ssd, arg, NULL);
    Py_DECREF(arg);
    if (-1 == rc)
	return NULL;
    PyObject_SetAttrString((PyObject *) o, "_streamDict",
			   (PyObject *) ssd);
    return (StreamSetDefObject *) ssd;
}

static int _StreamSet_doEq(PyObject * self,
			   PyObject * args,
			   PyObject * kwargs) {
    /* returns -1 on error, 0 if the objects are the same, 1
       if they are different */
    StreamSetDefObject *ssd;
    int i;
    PyObject *attr, *otherAttr, *rc;
    PyObject *skipSet=Py_None, *other;
    static char * kwlist[] = { "other", "skipSet", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist,
				     &other, &skipSet))
        return -1;

    if (skipSet != Py_None && skipSet->ob_type != &PyDict_Type) {
        PyErr_SetString(PyExc_TypeError, "skipSet must be None or a dict");
	return -1;
    }

    if (self->ob_type != other->ob_type) return 1;

    ssd = StreamSet_GetSSD(self->ob_type);
    if (ssd == NULL) {
	return -1;
    }

    for (i = 0; i < ssd->tagCount; i++) {
	if (skipSet != Py_None && PyDict_Contains(skipSet, ssd->tags[i].name)) 
	    continue;

	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
	otherAttr = PyObject_GetAttr((PyObject *) other, ssd->tags[i].name);
	rc = PyObject_CallMethod(attr, "__eq__", "OO", otherAttr, skipSet);
	Py_DECREF(attr);
	Py_DECREF(otherAttr);
	if (!rc) return -1;

	if (!PyInt_AsLong(rc)) {
	    /* this is non zero if they are the same, zero if they are
	       different */
	    Py_DECREF(rc);
	    return 1;
	}

	Py_DECREF(rc);
    }

    return 0;
}

#define SIZE_SMALL sizeof(char) + sizeof(short)
#define SIZE_LARGE sizeof(char) + sizeof(long)

/* constants for includeEmpty argument */
#define EXCLUDE_EMPTY 0
#define INCLUDE_EMPTY 1

static inline int addTag(char** buf, int tag, int valSize, int valLen) {
    unsigned char *chptr = (unsigned char*) *buf;
    int len;

    if (tag > UCHAR_MAX) {
	PyErr_SetString(PyExc_TypeError, "tag number overflow. max value is uchar");
	return -1;
    }
    /* first char is the tag */
    *chptr++ = tag;

    /* next is the size.  if a small value is specified, use 15
       bits for the size.  if a large value is specified, use 31
       bits for the size and set the high bit to 1 */
    if (valSize == DYNAMIC) {
	if (valLen <= SHRT_MAX) {
	    valSize = SMALL;
	} else {
	    valSize = LARGE;
	}
    }
    if (valSize == SMALL) {
	if (valLen > SHRT_MAX) {
	    PyErr_SetString(PyExc_TypeError, "short int overflow");
	    return -1;
	}
	*((int16_t *) chptr) = htons(valLen);
	chptr += sizeof(int16_t);
    } else if (valSize == LARGE) {
	if (valLen > LONG_MAX) {
	    PyErr_SetString(PyExc_TypeError, "long int overflow");
	    return -1;
	}
	/* set the high bit to 1, which signifies that we're
	   using 31 bits for the size */
	*((uint32_t *) chptr) = htonl(valLen | (1 << 31));
	chptr += sizeof(uint32_t);
    } else {
	/* unreachable */
	assert(0);
    }

    /* figure out how much space we used */
    len = (int) chptr - (int) *buf;

    /* move the buffer pointer to the current posititon */
    *buf = (char *) chptr;

    /* return the amount of space consumed */
    return len;
}

static inline void getTag(char** buf, unsigned int *tag, int *valSize,
                          int *valLen) {
    unsigned char *chptr = (unsigned char *) *buf;
    char b;

    *tag = *chptr;
    chptr++;
    /* read the next byte */
    b = *chptr;
    if (b & (1 << 7)) {
	/* if the high bit is set, this is a 31 bit size */
	*valLen = ntohl(*((uint32_t *) chptr)) & ~(1 << 31);
        *valSize = LARGE;
	chptr += sizeof(uint32_t);
    } else {
	/* otherwise it's a 15 bit size.  mask out the high bit */
	*valLen = ntohs(*((int16_t *) chptr));
	chptr += sizeof(int16_t);
        *valSize = SMALL;
    }
    *buf = (char *) chptr;
}

static inline int unknownInSkipSet(int idx, struct unknownTags * unknownTags,
			       PyObject *skipSet) {
    int rc;
    PyObject *pyTag = PyInt_FromLong(unknownTags[idx].tag);

    if (skipSet == NULL || skipSet == Py_None) {
	return 0;
    }

    if (PyDict_Contains(skipSet, pyTag)) {
	rc = 1;
    } else {
	rc = 0;
    }

    Py_DECREF(pyTag);
    return rc;
}

static PyObject *concatStrings(StreamSetDefObject *ssd,
			       PyObject ** vals,
                               int unknownCount,
                               struct unknownTags * unknownTags,
			       PyObject *skipSet,
			       int includeEmpty) {
    char *final, *chptr;
    int len, valLen, rc, useAlloca = 0;
    int tagIdx, unknownIdx;
    PyObject * result;
    int isEmpty = 1;

    assert(includeEmpty == INCLUDE_EMPTY || includeEmpty == EXCLUDE_EMPTY);

    /* first calculate the amount of space needed (worst case) */
    len = 0;
    for (tagIdx = 0; tagIdx < ssd->tagCount; tagIdx++) {
	if (vals[tagIdx] != Py_None)  {
	    valLen = PyString_GET_SIZE(vals[tagIdx]);
	    len += valLen + SIZE_LARGE;
	}
    }

    for (unknownIdx = 0; unknownIdx < unknownCount; unknownIdx++) {
        if (unknownInSkipSet(unknownIdx, unknownTags, skipSet)) {
            continue;
        }
        valLen = PyString_GET_SIZE(unknownTags[unknownIdx].data);
        len += valLen + SIZE_LARGE;
    }

    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	final = alloca(len);
    } else
	final = malloc(len);

    if (final == NULL)
	/* FIXME: memory leak.  DECREF vals here */
	return PyErr_NoMemory();

    /* Both the tag list and the unknown list are sorted; walk them together
       to get an ordered concatenation */
    len = 0;
    chptr = final;
    tagIdx = 0;
    unknownIdx = 0;
    while (tagIdx < ssd->tagCount || unknownIdx < unknownCount) {
        if ((tagIdx < ssd->tagCount && unknownIdx == unknownCount) ||
            (tagIdx < ssd->tagCount &&
                ssd->tags[tagIdx].tag < unknownTags[unknownIdx].tag)) {

            if (vals[tagIdx] == Py_None)  {
                tagIdx++;
                continue;
            }

            valLen = PyString_GET_SIZE(vals[tagIdx]);
            len += valLen;
            /* do not include zero length frozen data unless requested */
            if (valLen > 0 || includeEmpty) {
                /* either we have data or including empty data was
                   requested */
                isEmpty = 0;
                rc = addTag(&chptr, ssd->tags[tagIdx].tag,
                            ssd->tags[tagIdx].size, valLen);
                if (-1 == rc)
                    goto error;
                len += rc;
                memcpy(chptr, PyString_AS_STRING(vals[tagIdx]), valLen);
                chptr += valLen;
            }

            Py_DECREF(vals[tagIdx]);
            tagIdx++;
        } else if ((tagIdx == ssd->tagCount && unknownIdx < unknownCount) ||
            (unknownIdx < unknownCount &&
                ssd->tags[tagIdx].tag > unknownTags[unknownIdx].tag)) {
            if (unknownInSkipSet(unknownIdx, unknownTags, skipSet)) {
                unknownIdx++;
                continue;
            }
            valLen = PyString_GET_SIZE(unknownTags[unknownIdx].data);
            len += valLen;
            rc = addTag(&chptr, unknownTags[unknownIdx].tag,
                        unknownTags[unknownIdx].sizeType, valLen);
            if (-1 == rc)
                goto error;
            len += rc;
            memcpy(chptr, PyString_AS_STRING(unknownTags[unknownIdx].data),
                   valLen);
            chptr += valLen;

            unknownIdx++;
        } else {
            assert(0);
        }
    }

    result = PyString_FromStringAndSize(final, len);

    if (!useAlloca)
	free(final);

    if (isEmpty && includeEmpty == INCLUDE_EMPTY) {
        Py_DECREF(result);
        Py_INCREF(Py_None);
        result = Py_None;
    }

    return result;

 error:
    {
	int j;
	for (j = tagIdx; j < ssd->tagCount; j++) {
	    Py_DECREF(vals[j]);
	}

	if (!useAlloca)
	    free(final);
	return NULL;
    }
}

static PyObject * StreamSet_DeepCopy(PyObject * self, PyObject * args) {
    PyObject * frz, * obj;

    frz = PyObject_CallMethod(self, "freeze", "");
    if (!frz)
        return NULL;

    if (!(obj = PyObject_CallFunction((PyObject *) self->ob_type, "O", frz))) {
	Py_DECREF(frz);
        return NULL;
    }

    Py_DECREF(frz);

    return obj;
}

static PyObject * StreamSet_Diff(StreamSetObject * self, PyObject * args,
				 PyObject * kwargs) {
    PyObject ** vals;
    StreamSetDefObject * ssd;
    int i, len, useAlloca = 0;
    PyObject * attr, * otherAttr, *rc, *ignoreUnknown = Py_False;
    StreamSetObject * other;
    static char * kwlist[] = { "other", "ignoreUnknown", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!|O", kwlist,
				     self->ob_type, &other, &ignoreUnknown))
        return NULL;

    if (ignoreUnknown != Py_False && ignoreUnknown != Py_True) {
        PyErr_SetString(PyExc_TypeError, "ignoreUnknown must be boolean");
        return NULL;
    }

    if (ignoreUnknown != Py_True && (self->unknownCount || other->unknownCount)) {
        PyErr_SetString(PyExc_ValueError,
                        "Cannot diff streams with unknown tags");
        return NULL;
    }

    ssd = StreamSet_GetSSD(self->ob_type);
    if (ssd == NULL) {
	return NULL;
    }

    len = sizeof(PyObject *) * ssd->tagCount;
    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	vals = alloca(len);
    } else
	vals = malloc(len);

    for (i = 0; i < ssd->tagCount; i++) {
	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
	otherAttr = PyObject_GetAttr((PyObject *) other, ssd->tags[i].name);

	if (PyObject_IsInstance((PyObject *) attr,
				(PyObject *) &allStreams[STREAM_SET].pyType)) {
	    vals[i] = PyObject_CallMethod(attr, "diff", "OO", otherAttr,
					  ignoreUnknown);
	} else {
	    vals[i] = PyObject_CallMethod(attr, "diff", "O", otherAttr);
	}

	Py_DECREF(attr);
	Py_DECREF(otherAttr);

	if (!vals[i]) {
	    int j;
	    for (j = 0; j < i; j++)
		Py_DECREF(vals[j]);

	    if (!useAlloca)
		free(vals);
	    return NULL;
	}
    }

    /* note that, unlike freeze(), diff() includes diffs that
       are zero length.  they have special meaning in some
       stream types (usually that the stored value is None) */
    rc = concatStrings(ssd, vals, 0, NULL, NULL, INCLUDE_EMPTY);

    if (!useAlloca)
	free(vals);
    return rc;
}

static PyObject * StreamSet_Eq(PyObject * self,
                               PyObject * args,
                               PyObject * kwargs) {
    int rc;

    rc = _StreamSet_doEq(self, args, kwargs);
    if (rc == -1)
	return NULL;

    if (rc == 0) {
        Py_INCREF(Py_True);
        return Py_True;
    }

    Py_INCREF(Py_False);
    return Py_False;
}

static PyObject * StreamSet_Ne(PyObject * self,
                               PyObject * args,
                               PyObject * kwargs) {
    int rc;

    rc = _StreamSet_doEq(self, args, kwargs);
    if (rc == -1)
	return NULL;

    if (rc == 0) {
        Py_INCREF(Py_False);
        return Py_False;
    }

    Py_INCREF(Py_True);
    return Py_True;
}

/* this is a python class method */
static PyObject * StreamSet_Find(PyObject * objClass, PyObject * args) {
    char * data;
    int dataLen;
    char * chptr, * end;
    int targetStreamId, size, sizeType;
    unsigned int streamId;
    int ssdIdx;
    StreamSetDefObject * ssd;

    if (!PyArg_ParseTuple(args, "is#", &targetStreamId, 
                          &data, &dataLen))
        return NULL;

    ssd = StreamSet_GetSSD((PyTypeObject *) objClass);
    if (ssd == NULL) {
	return NULL;
    }

    /* find the target stream from our stream definition */
    for (ssdIdx = 0; ssdIdx < ssd->tagCount; ssdIdx++)
        if (ssd->tags[ssdIdx].tag == targetStreamId) break;

    if (ssdIdx == ssd->tagCount) {
        PyErr_SetString(PyExc_ValueError, "unknown tag in stream set");
        return NULL;
    }

    chptr = data;
    end = data + dataLen;

    while (chptr < end) {
        PyObject * obj;

        getTag(&chptr, &streamId, &sizeType, &size);
        if (streamId != targetStreamId) {
            chptr += size;
            continue;
        }

        if (!(obj = PyObject_CallFunction(ssd->tags[ssdIdx].type, "s#",
                                          chptr, size)))
            return NULL;

        return obj;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject * StreamSet_Freeze(StreamSetObject * self, 
                                   PyObject * args,
                                   PyObject * kwargs) {
    StreamSetObject * sset = (StreamSetObject *) self;
    static char * kwlist[] = { "skipSet", "freezeKnown", "freezeUnknown",
                               NULL };
    PyObject ** vals;
    StreamSetDefObject * ssd;
    int i, len, useAlloca = 0;
    PyObject * attr, *rc, * skipSet = Py_None;
    PyObject * freezeKnown = Py_True, * freezeUnknown = Py_True;
    int unknownCount;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOO", kwlist, &skipSet,
                                     &freezeKnown, &freezeUnknown, NULL))
        return NULL;

    if (skipSet != Py_None && !PyDict_Check(skipSet)) {
        PyErr_SetString(PyExc_TypeError, "skipSet must be None or a dict");
	return NULL;
    }

    if (freezeKnown != Py_False && freezeKnown != Py_True) {
        PyErr_SetString(PyExc_TypeError, "freezeKnown must be boolean");
        return NULL;
    }

    if (freezeUnknown != Py_False && freezeUnknown != Py_True) {
        PyErr_SetString(PyExc_TypeError, "freezeUnknown must be boolean");
        return NULL;
    }

    if (freezeKnown == Py_False && freezeUnknown == Py_False)
        return PyString_FromString("");

    ssd = StreamSet_GetSSD(self->ob_type);
    if (ssd == NULL) {
	return NULL;
    }
    
    len = sizeof(PyObject *) * ssd->tagCount;
    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	vals = alloca(len);
    } else
	vals = malloc(len);

    for (i = 0; i < ssd->tagCount; i++) {
        if (freezeKnown == Py_False ||
                    (skipSet != Py_None &&
                     PyDict_Contains(skipSet, ssd->tags[i].name))) {
            Py_INCREF(Py_None);
            vals[i] = Py_None;
	    continue;
        }

	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
        if (PyObject_IsInstance((PyObject *) attr,
                                (PyObject *) &allStreams[STREAM_SET].pyType)) {
            vals[i] = PyObject_CallMethod(attr, "freeze", "OOO", skipSet,
                                          freezeKnown, freezeUnknown);
        } else {
            vals[i] = PyObject_CallMethod(attr, "freeze", "O", skipSet);
        }
	Py_DECREF(attr);

	if (!vals[i]) {
	    /* an error occurred when calling the freeze method for the
	       object.  Free memory and return NULL */
	    int j;

	    for (j = 0; j < i; j++)
		Py_DECREF(vals[j]);
	    if (!useAlloca)
		free(vals);
	    return NULL;
	}
    }

    if (freezeUnknown == Py_True)
        unknownCount = sset->unknownCount;
    else
        unknownCount = 0;

    /* do not include zero length frozen data */
    rc = concatStrings(ssd, vals, unknownCount, sset->unknownTags, skipSet,
                       EXCLUDE_EMPTY);
    if (!useAlloca)
	free(vals);
    return rc;
}

static int StreamSet_Init(PyObject * o, PyObject * args,
			  PyObject * kwargs) {
    static char * kwlist[] = { "data", "offset", NULL };
    StreamSetObject *self = (StreamSetObject *) o;
    StreamSetDefObject * ssd;
    int i;
    int offset = 0;
    char * data = NULL;
    int dataLen;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|z#i", kwlist, &data, 
				     &dataLen, &offset)) {
        return -1;
    }
    ssd = StreamSet_GetSSD(o->ob_type);
    if (!ssd)
	return -1;

    self->unknownCount = 0;
    self->unknownTags = NULL;

    for (i = 0; i < ssd->tagCount; i++) {
	PyObject * obj;

        if (!(obj = PyObject_CallFunction(ssd->tags[i].type, NULL)))
	    return -1;

	if (PyObject_SetAttr(o, ssd->tags[i].name, obj)) {
	    Py_DECREF(obj);
	    return -1;
	}
	/* we keep our reference in our dict */
	Py_DECREF(obj);
    }

    if (!data)
	return 0;

    if (Thaw_raw(o, ssd, data, dataLen, offset))
	return -1;

    return 0;
}

static void StreamSet_Dealloc(PyObject * self) {
    StreamSetObject * sset = (StreamSetObject *) self;
    int i;

    for (i = 0; i < sset->unknownCount; i++) {
        Py_DECREF(sset->unknownTags[i].data);
    }

    if (sset->unknownCount)
        free(sset->unknownTags);
    self->ob_type->tp_free(self);
}

static PyObject * StreamSet_Thaw(PyObject * o, PyObject * args) {
    char * data = NULL;
    int dataLen;
    StreamSetDefObject * ssd;

    if (!PyArg_ParseTuple(args, "s#", &data, &dataLen))
        return NULL;

    ssd = StreamSet_GetSSD(o->ob_type);
    if (ssd == NULL) {
	return NULL;
    }

    if (Thaw_raw(o, ssd, data, dataLen, 0))
	return NULL;

    Py_INCREF(Py_None);
    return Py_None;
}

static int Thaw_raw(PyObject * self, StreamSetDefObject * ssd,
		    char * data, int dataLen, int offset) {
    char * streamData, * chptr, * end;
    int size, i, sizeType;
    PyObject * attr, * ro;
    int ignoreUnknown = -1;
    unsigned int streamId;
    StreamSetObject * sset = (StreamSetObject *) self;

    end = data + dataLen;
    chptr = data + offset;
    while (chptr < end) {
	getTag(&chptr, &streamId, &sizeType, &size);
	streamData = chptr;
	chptr += size;

	/* find the matching stream from our stream definition */
	for (i = 0; i < ssd->tagCount; i++)
	    if (ssd->tags[i].tag == streamId) break;
	if (i == ssd->tagCount) {
	    PyObject * obj;

	    if (ignoreUnknown == -1) {
		obj = PyObject_GetAttrString(self, "ignoreUnknown");
		if (obj != NULL)
		    ignoreUnknown = PyInt_AsLong(obj);
                else
                    ignoreUnknown = 0;
            }

            if (ignoreUnknown == SKIP_UNKNOWN) {
                continue;
            } else if (ignoreUnknown != PRESERVE_UNKNOWN) {
                PyErr_SetString(PyExc_ValueError, "unknown tag in stream set");
                return -1;
            }

            sset->unknownTags = realloc(sset->unknownTags,
                                        sizeof(*sset->unknownTags) *
                                            (sset->unknownCount + 1));
            sset->unknownTags[sset->unknownCount].tag = streamId;
            sset->unknownTags[sset->unknownCount].sizeType = sizeType;
            sset->unknownTags[sset->unknownCount].data =
                    PyString_FromStringAndSize(streamData, size);
            sset->unknownCount++;

            continue;
	}

	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
	ro = PyObject_CallMethod(attr, "thaw", "s#", streamData, size);
	Py_DECREF(attr);
	if (!ro) {
	    return -1;
	}
	Py_DECREF(ro);
    }

    if (chptr != end) {
	PyErr_SetString(PyExc_AssertionError,
			"chptr != end in Thaw_raw");
	return -1;
    }
    assert(chptr == end);

    return 0;
}

PyObject *StreamSet_split(PyObject *self, PyObject *args) {
    char *data, *streamData, *chptr, *end;
    int size, sizeType, dataLen;
    unsigned int streamId;
    PyObject *pydata, *l, *t;

    if (PyTuple_GET_SIZE(args) != 1) {
        PyErr_SetString(PyExc_TypeError, "exactly 1 argument expected");
        return NULL;
    }
    pydata = PyTuple_GET_ITEM(args, 0);
    data = PyString_AsString(pydata);
    dataLen = PyString_Size(pydata);
    end = data + dataLen;
    chptr = data;
    l = PyList_New(0);
    while (chptr < end) {
	getTag(&chptr, &streamId, &sizeType, &size);
	streamData = chptr;
	t = PyTuple_New(2);
	PyTuple_SetItem(t, 0, PyInt_FromLong(streamId));
	PyTuple_SetItem(t, 1, PyString_FromStringAndSize(streamData, size));
	PyList_Append(l, t);
	Py_DECREF(t);
	chptr += size;
    }

    if (chptr != end) {
	Py_DECREF(l);
	PyErr_SetString(PyExc_AssertionError,
			"chptr != end in Thaw_raw");
	return NULL;
    }
    assert(chptr == end);

    return l;
}

PyObject *StreamSet_remove(PyObject *self, PyObject *args) {
    char *data, *chptr, *end, *newdata, *newchptr;
    int size, sizeType, dataLen, rc, skipId, len;
    unsigned int streamId;
    PyObject *pydata, *pyskipid, *s;

    if (PyTuple_GET_SIZE(args) != 2) {
        PyErr_SetString(PyExc_TypeError, "exactly 2 arguments expected");
        return NULL;
    }
    pydata = PyTuple_GET_ITEM(args, 0);
    pyskipid = PyTuple_GET_ITEM(args, 1);
    skipId = PyInt_AsLong(pyskipid);
    data = PyString_AsString(pydata);
    dataLen = PyString_Size(pydata);
    newdata = malloc(dataLen);
    if (NULL == newdata) {
	PyErr_NoMemory();
	return NULL;
    }
    end = data + dataLen;
    chptr = data;
    newchptr = newdata;
    len = 0;
    while (chptr < end) {
	getTag(&chptr, &streamId, &sizeType, &size);
	if (streamId == skipId) {
	    chptr += size;
	    continue;
	}
	rc = addTag(&newchptr, streamId, sizeType, size);
	if (-1 == rc)
	    goto error;
	len += rc;
	memcpy(newchptr, chptr, size);
	chptr += size;
	newchptr += size;
	len += size;
    }
    s = PyString_FromStringAndSize(newdata, len);
    free(newdata);
    return s;
 error:
    free(newdata);
    return NULL;
}

static PyObject * StreamSet_Twm(StreamSetObject * self, PyObject * args,
                                PyObject * kwargs) {
    char * kwlist[] = { "diff", "base", "skip", NULL };
    char * diff;
    int diffLen;
    StreamSetObject * base;
    PyObject * skipSet = Py_None;
    StreamSetDefObject * ssd;
    char * end, * chptr, * streamData;
    int i;
    int size, sizeType;
    unsigned int streamId;
    PyObject * attr, * baseAttr, * ro;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z#O!|O", kwlist, 
				     &diff, &diffLen, self->ob_type,
				     (PyObject *) &base, &skipSet))
        return NULL;

    if (skipSet != Py_None && !PyDict_Check(skipSet)) {
        PyErr_SetString(PyExc_TypeError, "skipSet must be None or a dict");
	return NULL;
    }

    if (!diff) {
        Py_INCREF(Py_False);
        return Py_False;
    }

    ssd = StreamSet_GetSSD(self->ob_type);
    if (ssd == NULL) {
	return NULL;
    }
    end = diff + diffLen;
    chptr = diff;
    while (chptr < end) {
	getTag(&chptr, &streamId, &sizeType, &size);
	streamData = chptr;
	chptr += size;

	for (i = 0; i < ssd->tagCount; i++)
	    if (ssd->tags[i].tag == streamId) break;

	if (i == ssd->tagCount) {
	    PyObject * obj;
	    int ignoreUnknown = 0;
            int unknownIdx;

	    obj = PyObject_GetAttrString((PyObject *)self, "ignoreUnknown");

	    if (obj != NULL)
		ignoreUnknown = PyInt_AsLong(obj);

            if (ignoreUnknown == SKIP_UNKNOWN) {
                continue;
            } else if (ignoreUnknown != PRESERVE_UNKNOWN) {
                PyErr_SetString(PyExc_ValueError, "unknown tag for merge");
                return NULL;
            }

            /* we only support merging unknown types if self == base */
            if (self != base) {
                PyErr_SetString(PyExc_ValueError,
                                "Cannot merge unknown streams");
                return NULL;
            }

            for (unknownIdx = 0; unknownIdx < self->unknownCount;
                 unknownIdx++)
                if (self->unknownTags[unknownIdx].tag >= streamId) break;

            if (unknownIdx < self->unknownCount &&
                    self->unknownTags[unknownIdx].tag == streamId) {
                Py_DECREF(self->unknownTags[unknownIdx].data);
                self->unknownTags[unknownIdx].data =
                        PyString_FromStringAndSize(streamData, size);
            } else {
                /* We don't have an entry for this tag at all. Make a new
                   one in the proper sorted order */
                /* append this new item to the end of the unknown tags. */
                self->unknownTags = realloc(self->unknownTags,
                                            sizeof(*self->unknownTags) *
                                                (self->unknownCount + 1));
                if (unknownIdx < self->unknownCount)
                    memmove(self->unknownTags + (unknownIdx + 1),
                            self->unknownTags + unknownIdx,
                            sizeof(*self->unknownTags) *
                                (self->unknownCount - unknownIdx));
                self->unknownCount++;

                self->unknownTags[unknownIdx].tag = streamId;
                self->unknownTags[unknownIdx].sizeType = sizeType;
                self->unknownTags[unknownIdx].data =
                        PyString_FromStringAndSize(streamData, size);
            }

            continue;
        }

	if (skipSet != Py_None && PyDict_Contains(skipSet, ssd->tags[i].name))
	    continue;

	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
	baseAttr = PyObject_GetAttr((PyObject *) base,
				    ssd->tags[i].name);
	ro = PyObject_CallMethod(attr, "twm", "s#O", streamData, size, 
				 baseAttr);
	Py_DECREF(attr);
	Py_DECREF(baseAttr);

	if (!ro)
	    return NULL;

	Py_DECREF(ro);

    }

    Py_INCREF(Py_None);
    return Py_None;
}

static long StreamSet_Hash(PyObject * self) {
    PyObject *frozen, *args, *kwargs;
    long rc;

    args = PyTuple_New(0);
    kwargs = PyDict_New();
    frozen = StreamSet_Freeze((StreamSetObject *) self, args, kwargs);
    if (!frozen) {
	return -1;
    }
    rc = PyObject_Hash(frozen);
    Py_DECREF(args);
    Py_DECREF(kwargs);
    Py_DECREF(frozen);
    return rc;
}

/* ------------------------------------- */
/* Type and method definition            */

static PyMethodDef StreamSetDefMethods[] = {
    {NULL}  /* Sentinel */
};

static PyTypeObject StreamSetDefType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.StreamSetDef",        /*tp_name*/
    sizeof(StreamSetDefObject),     /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    StreamSetDef_Dealloc,           /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    0,				    /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    0,                              /*tp_hash */
    0,				    /*tp_call*/
    0,                              /*tp_str*/
    0,                              /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/     
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,             /*tp_flags*/
    NULL,                           /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    StreamSetDefMethods,            /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    StreamSetDef_Init,              /* tp_init */
};

static PyMethodDef StreamSetMethods[] = {
    { "__deepcopy__", (PyCFunction) StreamSet_DeepCopy, METH_VARARGS         },
    { "diff",   (PyCFunction) StreamSet_Diff,   METH_VARARGS | METH_KEYWORDS },
    { "__eq__", (PyCFunction) StreamSet_Eq,     METH_VARARGS | METH_KEYWORDS },
    { "__ne__", (PyCFunction) StreamSet_Ne,     METH_VARARGS | METH_KEYWORDS },
    { "find",   (PyCFunction) StreamSet_Find,   METH_VARARGS | METH_CLASS    },
    { "freeze", (PyCFunction) StreamSet_Freeze, METH_VARARGS | METH_KEYWORDS },
    { "thaw",   (PyCFunction) StreamSet_Thaw,   METH_VARARGS                 },
    { "twm",    (PyCFunction) StreamSet_Twm,    METH_VARARGS | METH_KEYWORDS },
    {NULL}  /* Sentinel */
};

PyTypeObject StreamSetType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.StreamSet",           /*tp_name*/
    sizeof(StreamSetObject),        /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    StreamSet_Dealloc,              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    0,				    /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    StreamSet_Hash,                 /*tp_hash */
    0,				    /*tp_call*/
    0,                              /*tp_str*/
    0,                              /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,             /*tp_flags*/
    NULL,                           /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    StreamSetMethods,               /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    StreamSet_Init,                 /* tp_init */
};

#define REGISTER_TYPE(name) \
    if (PyType_Ready(&name ## Type) < 0) \
        return; \
    Py_INCREF(&name ## Type); \
    PyModule_AddObject(m, #name, (PyObject *) &name ## Type);

void streamsetinit(PyObject * m) {
    StreamSetDefType.tp_new = PyType_GenericNew;
    REGISTER_TYPE(StreamSetDef);
    allStreams[STREAM_SET].pyType = StreamSetType;
}
