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
    PyObject * name;
    PyObject * type;
};

typedef struct {
    PyObject_HEAD
    struct tagInfo * tags;
    int tagCount;
    int size;
} StreamSetDefObject;

typedef struct {
    PyObject_HEAD;
} StreamSetObject;

static int Thaw_raw(PyObject * self, StreamSetDefObject * ssd,
		    char * data, int dataLen, int offset, int streamSize);

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
	char * name;

	if (!PyArg_ParseTuple(items->ob_item[i], "i(Os)",
			 &tag, &streamType, &name)) {
	    return -1;
	}

	ssd->tags[i].tag = tag;
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
    
    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "_streamDict");
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

/* constants for size argument */
#define SIZE_SMALL sizeof(char) + sizeof(short)
#define SIZE_LARGE sizeof(short) + sizeof(int)

/* constants for includeEmpty argument */
#define EXCLUDE_EMPTY 0
#define INCLUDE_EMPTY 1

static inline int addSmallTag(char** buf, int tag, int valLen) {
    char *chptr = *buf;
    *chptr++ = tag;
    if (valLen > USHRT_MAX) {
        PyErr_SetString(PyExc_TypeError, "unsigned short overflow");
	return -1;
    }
    *((unsigned short *) chptr) = htons(valLen);
    chptr += sizeof(unsigned short);
    *buf = chptr;
    return 0;
}

static inline int addLargeTag(char** buf, int tag, int valLen) {
    char *chptr = *buf;
    *((short *) chptr) = htons(tag);
    chptr += sizeof(short);
    *((unsigned int *) chptr) = htonl(valLen);
    chptr += sizeof(int);
    *buf = chptr;
    return 0;
}

static PyObject *concatStrings(StreamSetDefObject *ssd,
			       PyObject ** vals,
			       int len,
			       int includeEmpty,
			       int size) {
    char *final, *chptr;
    int i, valLen, useAlloca = 0;
    PyObject * result;
    int (*addTag)(char**,int,int);

    assert(size == SIZE_SMALL || size == SIZE_LARGE);
    assert(includeEmpty == INCLUDE_EMPTY || includeEmpty == EXCLUDE_EMPTY);

    if (size == SIZE_SMALL)
	addTag = addSmallTag;
    else
	addTag = addLargeTag;

    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	final = alloca(len);
    } else
	final = malloc(len);

    if (final == NULL)
	/* FIXME: memory leak.  DECREF vals here */
	return PyErr_NoMemory();

    chptr = final;
    for (i = 0; i < ssd->tagCount; i++) {
	if (vals[i] != Py_None)  {
	    valLen = PyString_GET_SIZE(vals[i]);
	    /* do not include zero length frozen data if requested */
	    if (valLen > 0 || includeEmpty) {
		/* either we have data or including empty data was
		   requested */
		if (addTag(&chptr, ssd->tags[i].tag, valLen))
		    goto error;
		memcpy(chptr, PyString_AS_STRING(vals[i]), valLen);
		chptr += valLen;
	    } else {
		/* otherwise we need to reduce the total size because
		   we are excluding tags */
		len -= size;
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

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "_streamDict");
    len = sizeof(PyObject *) * ssd->tagCount;
    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	vals = alloca(len);
    } else
	vals = malloc(len);
    len = 0;

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
	} else if (vals[i] != Py_None)
	    len += ssd->size + PyString_GET_SIZE(vals[i]);
    }

    /* note that, unlike freeze(), diff() includes diffs that
       are zero length.  they have special meaning in some
       stream types (usually that the stored value is None) */
    rc = concatStrings(ssd, vals, len, INCLUDE_EMPTY, ssd->size);

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

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "_streamDict");
    len = sizeof(PyObject *) * ssd->tagCount;
    if (len < ALLOCA_CUTOFF) {
	useAlloca = 1;
	vals = alloca(len);
    } else
	vals = malloc(len);
    len = 0;

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

        if (vals[i] != Py_None)
            len += PyString_GET_SIZE(vals[i]) + ssd->size;
    }

    /* do not include zero length frozen data */
    rc = concatStrings(ssd, vals, len, EXCLUDE_EMPTY, ssd->size);
    if (!useAlloca)
	free(vals);
    return rc;
}

static int StreamSet_Init_Common(PyObject * o, PyObject * args,
				 PyObject * kwargs, int size) {
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

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "_streamDict");
    if (!ssd) {
	char *buf;
	int len = 50 + strlen(self->ob_type->tp_name);
	buf = malloc(len);
	if (buf == NULL) {
	    PyErr_NoMemory();
	    return -1;
	}
	len = snprintf(buf, len, "%s class is missing the _streamDict class attribute", self->ob_type->tp_name);
        PyErr_SetString(PyExc_ValueError, buf);
	free(buf);
	return -1;
    } else if (ssd->ob_type != &StreamSetDefType) {
        PyErr_SetString(PyExc_TypeError, 
			"_streamDict attribute must be a cstreams.StreamSetDef");
	return -1;
    }

    ssd->size = size;

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

    if (Thaw_raw(o, ssd, data, dataLen, offset, ssd->size))
	return -1;

    return 0;
}

static int StreamSet_Init(PyObject * self, PyObject * args,
			  PyObject * kwargs) {
    return StreamSet_Init_Common(self, args, kwargs, SIZE_SMALL);
}

static PyObject * StreamSet_Thaw(PyObject * o, PyObject * args) {
    char * data = NULL;
    int dataLen;
    StreamSetDefObject * ssd;
    StreamSetObject *self = (StreamSetObject *) o;

    if (!PyArg_ParseTuple(args, "s#", &data, &dataLen))
        return NULL;

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "_streamDict");

    if (Thaw_raw(o, ssd, data, dataLen, 0, ssd->size))
	return NULL;

    Py_INCREF(Py_None);
    return Py_None;
}

static inline void getSmallTag(char** buf, int *tag, int *size) {
    char *chptr = *buf;
    *tag = *chptr;
    chptr++;
    *size = ntohs(*((unsigned short *) chptr));
    chptr += sizeof(unsigned short);
    *buf = chptr;
}

static inline void getLargeTag(char** buf, int *tag, int *size) {
    char *chptr = *buf;
    *tag = ntohs(*((unsigned short *) chptr));
    chptr += sizeof(unsigned short);
    *size = ntohl(*((unsigned int *) chptr));
    chptr += sizeof(unsigned int);
    *buf = chptr;
}

static int Thaw_raw(PyObject * self, StreamSetDefObject * ssd,
		    char * data, int dataLen, int offset,
		    int streamSize) {
    char * streamData, * chptr, * end;
    int size, i;
    PyObject * attr, * ro;
    int ignoreUnknown = -1;
    int streamId;
    void (*getTag)(char**,int*,int*);

    assert(streamSize == SIZE_SMALL || streamSize == SIZE_LARGE);

    if (streamSize == SIZE_SMALL)
	getTag = getSmallTag;
    else
	getTag = getLargeTag;

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
		obj = PyDict_GetItemString(self->ob_type->tp_dict, 
					   "ignoreUnknown");
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
    void (*getTag)(char**,int*,int*);

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

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "_streamDict");

    if (ssd->size == SIZE_SMALL)
	getTag = getSmallTag;
    else
	getTag = getLargeTag;

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

	    obj = PyDict_GetItemString(self->ob_type->tp_dict, 
				       "ignoreUnknown");

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
/* LargeStreamSet Implementation         */

static int LStreamSet_Init(PyObject * self, PyObject * args,
			   PyObject * kwargs) {
    return StreamSet_Init_Common(self, args, kwargs, SIZE_LARGE);
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

static PyMethodDef LargeStreamSetMethods[] = {
    { "__deepcopy__", (PyCFunction) StreamSet_DeepCopy, METH_VARARGS         },
    { "diff",   (PyCFunction) StreamSet_Diff,   METH_VARARGS                 },
    { "__eq__", (PyCFunction) StreamSet_Eq,     METH_VARARGS | METH_KEYWORDS },
    { "freeze", (PyCFunction) StreamSet_Freeze, METH_VARARGS | METH_KEYWORDS },
    { "thaw",   (PyCFunction) StreamSet_Thaw,   METH_VARARGS                 },
    { "twm",    (PyCFunction) StreamSet_Twm,    METH_VARARGS | METH_KEYWORDS },
    {NULL}  /* Sentinel */
};

PyTypeObject LargeStreamSetType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.LargeStreamSet",      /*tp_name*/
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
    LargeStreamSetMethods,          /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    LStreamSet_Init,                /* tp_init */
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
    allStreams[LARGE_STREAM_SET].pyType = LargeStreamSetType;
}
