/*
 * Copyright (c) 2005 Specifix, Inc.
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

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

staticforward PyTypeObject StreamSetDefType;
staticforward PyTypeObject StreamSetType;

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
} StreamSetDefObject;

typedef struct {
    PyObject_HEAD
} StreamSetObject;

/* ------------------------------------- */
/* StreamSetDef Implementation           */

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

static PyObject * StreamSet_concatStrings(StreamSetDefObject * ssd,
					  PyObject ** vals, int len) {
    char * final, * chptr;
    int i;

    final = alloca(len);
    chptr = final;
    for (i = 0; i < ssd->tagCount; i++) {
	if (vals[i] != Py_None)  {
	    *chptr++ = ssd->tags[i].tag;
	    *((short *) chptr) = htons(PyString_GET_SIZE(vals[i]));
	    chptr += 2;

	    memcpy(chptr, PyString_AS_STRING(vals[i]), 
		   PyString_GET_SIZE(vals[i]));
	    chptr += PyString_GET_SIZE(vals[i]);
	}

	Py_DECREF(vals[i]);
    }

    return PyString_FromStringAndSize(final, len);
}

static PyObject * StreamSet_Freeze(StreamSetObject * self, 
                                   PyObject * args,
                                   PyObject * kwargs) {
    static char * kwlist[] = { "skipset", NULL };
    PyObject ** vals;
    StreamSetDefObject * ssd;
    int i;
    PyObject * attr, * skipSet = Py_None;
    int len = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O!", kwlist,
				     PyDict_Type, &skipSet))
        return NULL;

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "streamDict");
    vals = alloca(sizeof(PyObject *) * ssd->tagCount);

    for (i = 0; i < ssd->tagCount; i++) {
	if (skipSet != Py_None && PyDict_Contains(skipSet, ssd->tags[i].name))
	    continue;

	attr = self->ob_type->tp_getattro((PyObject *) self, 
					  ssd->tags[i].name);
	vals[i] = PyObject_CallMethod(attr, "freeze", "O", skipSet);
	Py_DECREF(attr);
	if (!vals[i])
	    return NULL;
	len += PyString_GET_SIZE(vals[i]) + 3;
    }

    return StreamSet_concatStrings(ssd, vals, len);
}

static PyObject * StreamSet_Diff(StreamSetObject * self, PyObject * args) {
    PyObject ** vals;
    StreamSetDefObject * ssd;
    int i, len;
    PyObject * attr, * otherAttr;
    StreamSetObject * other;
    
    if (!PyArg_ParseTuple(args, "O!", self->ob_type, &other))
        return NULL;

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "streamDict");
    vals = alloca(sizeof(PyObject *) * ssd->tagCount);
    len = 0;

    for (i = 0; i < ssd->tagCount; i++) {
	attr = self->ob_type->tp_getattro((PyObject *) self, 
					  ssd->tags[i].name);
	otherAttr = other->ob_type->tp_getattro((PyObject *) other, 
						ssd->tags[i].name);

	vals[i] = PyObject_CallMethod(attr, "diff", "O", otherAttr);
	if (!vals[i])
	    return NULL;
	else if (vals[i] != Py_None)
	    len += PyString_GET_SIZE(vals[i]) + 3;

	Py_DECREF(attr);
    }

    return StreamSet_concatStrings(ssd, vals, len);
}

static int StreamSet_Init(PyObject * self, PyObject * args,
			  PyObject * kwargs) {
    static char * kwlist[] = { "data", NULL };
    StreamSetDefObject * ssd;
    int i;
    char * data = NULL;
    int dataLen;
    char * end, * chptr, * streamData;
    int streamId, size;
    int ignoreUnknown = -1;
    PyObject * attr;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|s#", kwlist, &data, 
				     &dataLen)) {
        return -1;
    }

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "streamDict");
    if (!ssd) {
        PyErr_SetString(PyExc_ValueError, 
		"StreamSets must have streamDict class attribute");
	return -1;
    } else if (ssd->ob_type != &StreamSetDefType) {
        PyErr_SetString(PyExc_TypeError, 
		"streamDict attribute must be a cstreams.StreamSetDef");
	return -1;
    }

    for (i = 0; i < ssd->tagCount; i++) {
	PyObject * obj;

        if (!(obj = PyObject_CallFunction(ssd->tags[i].type, NULL)))
	    return -1;
	
	if (self->ob_type->tp_setattro(self, ssd->tags[i].name, 
				       obj))
	    return -1;
    }

    if (!data)
	return 0;

    end = data + dataLen;
    chptr = data;
    while (chptr < end) {
	streamId = *chptr;
	chptr++;
	size = ntohs(*((short *) chptr));
	chptr += sizeof(short);
	streamData = chptr;
	chptr += size;

	for (i = 0; i < ssd->tagCount; i++)
	    if (ssd->tags[i].tag == streamId) break;
	if (i == ssd->tagCount) {
	    PyObject * obj;

	    if (ignoreUnknown == 1)
		continue;
	    if (ignoreUnknown == -1) {
		obj = PyDict_GetItemString(self->ob_type->tp_dict, 
					   "ignoreUnknown");
		if (obj)
		    ignoreUnknown = PyInt_AsLong(obj);

		if (ignoreUnknown == 1)
		    continue;

		PyErr_SetString(PyExc_ValueError, "unknown tag in stream set");
		return -1;
	    }
	}

	attr = self->ob_type->tp_getattro(self, ssd->tags[i].name);
	if (!PyObject_CallMethod(attr, "thaw", "s#", streamData, size))
	    return -1;
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
    PyObject * attr, * baseAttr;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s#O!|O!", kwlist, 
				     &diff, &diffLen, self->ob_type,
				     base, &PyDict_Type, &skipSet))
        return NULL;

    ssd = (void *) PyDict_GetItemString(self->ob_type->tp_dict, "streamDict");

    end = diff + diffLen;
    chptr = diff;
    while (chptr < end) {
	streamId = *chptr++;
	size = ntohs(*((short *) chptr));
	chptr += sizeof(short);
	streamData = chptr;
	chptr += size;

	if (skipSet != Py_None && PyDict_Contains(skipSet, ssd->tags[i].name))
	    continue;

	for (i = 0; i < ssd->tagCount; i++)
	    if (ssd->tags[i].tag == streamId) break;
	if (i == ssd->tagCount) {
	    PyErr_SetString(PyExc_ValueError, "Unknown tag for merge");
	    return NULL;
	}

	attr = self->ob_type->tp_getattro((PyObject *) self, 
					  ssd->tags[i].name);
	baseAttr = base->ob_type->tp_getattro((PyObject *) base, 
					       ssd->tags[i].name);

	if (!PyObject_CallMethod(attr, "twm", "s#O", 
				 streamData, size, baseAttr))
	    return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
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
    0,                              /*tp_dealloc*/
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
    { "diff",   (PyCFunction) StreamSet_Diff,   METH_VARARGS },
    { "freeze", (PyCFunction) StreamSet_Freeze, METH_VARARGS | METH_KEYWORDS },
    { "twm",    (PyCFunction) StreamSet_Twm,    METH_VARARGS | METH_KEYWORDS },
    {NULL}  /* Sentinel */
};

static PyTypeObject StreamSetType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.StreamSet",           /*tp_name*/
    sizeof(StreamSetObject),        /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    0,                              /*tp_dealloc*/
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
    StreamSetType.tp_new = PyType_GenericNew;
    REGISTER_TYPE(StreamSet);
    PyDict_SetItemString(StreamSetType.tp_dict, "__slots__", PyList_New(0));

    StreamSetDefType.tp_new = PyType_GenericNew;
    REGISTER_TYPE(StreamSetDef);
}
