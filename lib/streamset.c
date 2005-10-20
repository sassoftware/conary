/*
 * Copyright (c) 2005 rPath, Inc.
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
    int tag;
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
    PyObject_HEAD;
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

static int _StreamSet_doCmp(PyObject * self, PyObject * other,
			    PyObject * skipSet) {
    StreamSetDefObject * ssd;
    int i;
    PyObject * attr, * otherAttr, * rc;

    if (self->ob_type != other->ob_type) return 1;

    ssd = (void *) PyObject_GetAttrString((PyObject *)self->ob_type,
					  "_streamDict");

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

static int StreamSet_Cmp(PyObject * self, PyObject * other) {
    return _StreamSet_doCmp(self, other, Py_None);
}

#define SIZE_SMALL sizeof(char) + sizeof(short)
#define SIZE_LARGE sizeof(char) + sizeof(long)

/* constants for includeEmpty argument */
#define EXCLUDE_EMPTY 0
#define INCLUDE_EMPTY 1

static inline int addTag(char** buf, int tag, int valSize, int valLen) {
    char *chptr = *buf;
    int len;

    /* first char is the tag */
    *chptr++ = tag;

    /* next is the size.  if a small value is specified, use 15
       bits for the size.  if a large value is specified, use 31
       bits for the size and set the high bit to 1 */
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
    len = chptr - *buf;

    /* move the buffer pointer to the current posititon */
    *buf = chptr;

    /* return the amount of space consumed */
    return len;
}

static inline void getTag(char** buf, int *tag, int *size) {
    char *chptr = *buf;
    char b;

    *tag = *chptr;
    chptr++;
    /* read the next byte */
    b = *chptr;
    if (b & (1 << 7)) {
	/* if the high bit is set, this is a 31 bit size */
	*size = ntohl(*((uint32_t *) chptr)) & ~(1 << 31);
	chptr += sizeof(uint32_t);
    } else {
	/* otherwise it's a 15 bit size.  mask out the high bit */
	*size = ntohs(*((int16_t *) chptr));
	chptr += sizeof(int16_t);
    }
    *buf = chptr;
}

static PyObject *concatStrings(StreamSetDefObject *ssd,
			       PyObject ** vals,
			       int includeEmpty) {
    char *final, *chptr;
    int i, len, valLen, rc, useAlloca = 0;
    PyObject * result;

    assert(includeEmpty == INCLUDE_EMPTY || includeEmpty == EXCLUDE_EMPTY);

    /* first calculate the amount of space needed (worst case) */
    len = 0;
    for (i = 0; i < ssd->tagCount; i++) {
	if (vals[i] != Py_None)  {
	    valLen = PyString_GET_SIZE(vals[i]);
	    len += valLen + SIZE_LARGE;
	}
    }

    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	final = alloca(len);
    } else
	final = malloc(len);

    if (final == NULL)
	/* FIXME: memory leak.  DECREF vals here */
	return PyErr_NoMemory();

    len = 0;
    chptr = final;
    for (i = 0; i < ssd->tagCount; i++) {
	if (vals[i] != Py_None)  {
	    valLen = PyString_GET_SIZE(vals[i]);
	    len += valLen;
	    /* do not include zero length frozen data if requested */
	    if (valLen > 0 || includeEmpty) {
		/* either we have data or including empty data was
		   requested */
		rc = addTag(&chptr, ssd->tags[i].tag, ssd->tags[i].size,
			    valLen);
		if (-1 == rc)
		    goto error;
		len += rc;
		memcpy(chptr, PyString_AS_STRING(vals[i]), valLen);
		chptr += valLen;
	    }
	}

	Py_DECREF(vals[i]);
    }

    result = PyString_FromStringAndSize(final, len);

    if (!useAlloca)
	free(final);
    return result;

 error:
    {
	int j;
	for (j = i; j < ssd->tagCount; j++) {
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

static PyObject * StreamSet_Diff(StreamSetObject * self, PyObject * args) {
    PyObject ** vals;
    StreamSetDefObject * ssd;
    int i, len, useAlloca = 0;
    PyObject * attr, * otherAttr, *rc;
    StreamSetObject * other;

    if (!PyArg_ParseTuple(args, "O!", self->ob_type, &other))
        return NULL;

    ssd = (void *) PyObject_GetAttrString((PyObject *) self->ob_type,
					  "_streamDict");

    len = sizeof(PyObject *) * ssd->tagCount;
    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	vals = alloca(len);
    } else
	vals = malloc(len);

    for (i = 0; i < ssd->tagCount; i++) {
	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
	otherAttr = PyObject_GetAttr((PyObject *) other, ssd->tags[i].name);

	vals[i] = PyObject_CallMethod(attr, "diff", "O", otherAttr);

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
    rc = concatStrings(ssd, vals, INCLUDE_EMPTY);

    if (!useAlloca)
	free(vals);
    return rc;
}

static PyObject * StreamSet_Eq(PyObject * self, 
                               PyObject * args,
                               PyObject * kwargs) {
    static char * kwlist[] = { "other", "skipSet", NULL };
    PyObject * other;
    PyObject * skipSet = Py_None;
    int rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O!|O", kwlist,
				     self->ob_type, &other, &skipSet))
        return NULL;

    if (skipSet != Py_None && skipSet->ob_type != &PyDict_Type) {
        PyErr_SetString(PyExc_TypeError, "skipSet must be None or a dict");
	return NULL;
    }

    rc = _StreamSet_doCmp(self, other, skipSet);
    if (rc < 0 && PyErr_Occurred())
	return NULL;

    if (!rc) {
        Py_INCREF(Py_True);
        return Py_True;
    }

    Py_INCREF(Py_False);
    return Py_False;
}

static PyObject * StreamSet_Freeze(StreamSetObject * self, 
                                   PyObject * args,
                                   PyObject * kwargs) {
    static char * kwlist[] = { "skipSet", NULL };
    PyObject ** vals;
    StreamSetDefObject * ssd;
    int i, len, useAlloca = 0;
    PyObject * attr, *rc, * skipSet = Py_None;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &skipSet))
        return NULL;

    if (skipSet != Py_None && !PyDict_Check(skipSet)) {
        PyErr_SetString(PyExc_TypeError, "skipSet must be None or a dict");
	return NULL;
    }

    ssd = (void *) PyObject_GetAttrString((PyObject *) self->ob_type,
					  "_streamDict");

    len = sizeof(PyObject *) * ssd->tagCount;
    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	vals = alloca(len);
    } else
	vals = malloc(len);

    for (i = 0; i < ssd->tagCount; i++) {
	if (skipSet != Py_None && PyDict_Contains(skipSet, ssd->tags[i].name)) {
            Py_INCREF(Py_None);
            vals[i] = Py_None;
	    continue;
        }

	attr = PyObject_GetAttr((PyObject *) self, ssd->tags[i].name);
	vals[i] = PyObject_CallMethod(attr, "freeze", "O", skipSet);
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

    /* do not include zero length frozen data */
    rc = concatStrings(ssd, vals, EXCLUDE_EMPTY);
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

    ssd = (void *) PyObject_GetAttrString((PyObject *) o->ob_type,
					  "_streamDict");

    if (!ssd) {
	PyObject *sd;
	int rc;
	PyObject *arg;

	/* clear the old error */
	PyErr_Clear();
	sd = PyObject_GetAttrString((PyObject *) o->ob_type, "streamDict");
	if (!sd) {
	    char *buf;
	    int len = 50 + strlen(self->ob_type->tp_name);
	    buf = malloc(len);
	    if (buf == NULL) {
		PyErr_NoMemory();
		return -1;
	    }
	    len = snprintf(buf, len,
			   "%s class is missing the streamDict class variable",
			   o->ob_type->tp_name);
	    PyErr_SetString(PyExc_ValueError, buf);
	    free(buf);
	    return -1;
	}

	ssd = (void *) PyObject_New(StreamSetDefObject, &StreamSetDefType);
	if (NULL == ssd)
	    return -1;
	arg = PyTuple_New(1);
	PyTuple_SetItem(arg, 0, sd);
	rc = StreamSetDef_Init((PyObject *) ssd, arg, NULL);
	Py_DECREF(arg);
	if (-1 == rc)
	    return -1;
	PyObject_SetAttrString((PyObject *) o->ob_type, "_streamDict",
			       (PyObject *) ssd);
    } else if (ssd->ob_type != &StreamSetDefType) {
        PyErr_SetString(PyExc_TypeError, 
			"_streamDict attribute must be a cstreams.StreamSetDef");
	return -1;
    }

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

static PyObject * StreamSet_Thaw(PyObject * o, PyObject * args) {
    char * data = NULL;
    int dataLen;
    StreamSetDefObject * ssd;

    if (!PyArg_ParseTuple(args, "s#", &data, &dataLen))
        return NULL;

    ssd = (void *) PyObject_GetAttrString((PyObject *) o->ob_type,
					  "_streamDict");

    if (Thaw_raw(o, ssd, data, dataLen, 0))
	return NULL;

    Py_INCREF(Py_None);
    return Py_None;
}

static int Thaw_raw(PyObject * self, StreamSetDefObject * ssd,
		    char * data, int dataLen, int offset) {
    char * streamData, * chptr, * end;
    int size, i;
    PyObject * attr, * ro;
    int ignoreUnknown = -1;
    int streamId;

    end = data + dataLen;
    chptr = data + offset;
    while (chptr < end) {
	getTag(&chptr, &streamId, &size);
	streamData = chptr;
	chptr += size;

	/* find the matching stream from our stream definition */
	for (i = 0; i < ssd->tagCount; i++)
	    if (ssd->tags[i].tag == streamId) break;
	if (i == ssd->tagCount) {
	    PyObject * obj;

	    if (ignoreUnknown == 1)
		continue;
	    if (ignoreUnknown == -1) {
		obj = PyObject_GetAttrString(self, "ignoreUnknown");
		if (obj != NULL) {
		    ignoreUnknown = PyInt_AsLong(obj);
		}

		if (ignoreUnknown == 1)
		    continue;

		PyErr_SetString(PyExc_ValueError, "unknown tag in stream set");
		return -1;
	    }
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
	printf("An internal error has occurred.  Halting execution.\n");
	fflush(stdout);
	i = 1;
	while (i) ;
    }
    assert(chptr == end);

    return 0;
}

static PyObject * StreamSet_Twm(StreamSetObject * self, PyObject * args,
                                PyObject * kwargs) {
    char * kwlist[] = { "diff", "base", "skip", NULL };
    char * diff;
    int diffLen;
    PyObject * base, * skipSet = Py_None;
    StreamSetDefObject * ssd;
    char * end, * chptr, * streamData;
    int i;
    int size;
    int streamId;
    PyObject * attr, * baseAttr, * ro;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z#O!|O", kwlist, 
				     &diff, &diffLen, self->ob_type,
				     &base, &skipSet))
        return NULL;

    if (skipSet != Py_None && !PyDict_Check(skipSet)) {
        PyErr_SetString(PyExc_TypeError, "skipSet must be None or a dict");
	return NULL;
    }

    if (!diff) {
        Py_INCREF(Py_False);
        return Py_False;
    }

    ssd = (void *) PyObject_GetAttrString((PyObject *) self->ob_type,
					  "_streamDict");
    end = diff + diffLen;
    chptr = diff;
    while (chptr < end) {
	getTag(&chptr, &streamId, &size);
	streamData = chptr;
	chptr += size;

	for (i = 0; i < ssd->tagCount; i++)
	    if (ssd->tags[i].tag == streamId) break;

	if (i == ssd->tagCount) {
	    PyObject * obj;
	    int ignoreUnknown = 0;

	    obj = PyObject_GetAttrString((PyObject *)self, "ignoreUnknown");

	    if (obj != NULL)
		ignoreUnknown = PyInt_AsLong(obj);

	    if (ignoreUnknown == 1)
		continue;

	    PyErr_SetString(PyExc_ValueError, "unknown tag for merge");
	    return NULL;
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
    { "diff",   (PyCFunction) StreamSet_Diff,   METH_VARARGS                 },
    { "__eq__", (PyCFunction) StreamSet_Eq,     METH_VARARGS | METH_KEYWORDS },
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
    0,                              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    StreamSet_Cmp,		    /*tp_compare*/
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
