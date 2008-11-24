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

#if PY_MINOR_VERSION < 5
typedef int Py_ssize_t;
#endif

#include "cstreams.h"

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

/* ------------------------------------- */
/* Type object static declarations       */

#define SET(o, type, newVal) (((type##StreamObject *) (o))->val) = newVal
#define ISNONE(o) (((NumericStreamObject *) (o))->isNone)
#define VALUE(o, type) (((type##StreamObject *) (o))->val)

#define NUMERICSTREAM_SET(o, val) \
    if (STREAM_CHECK(o, INT_STREAM))            \
        SET(o, Int, val);                       \
    else if (STREAM_CHECK(o, SHORT_STREAM))     \
        SET(o, Short, val);                     \
    else if (STREAM_CHECK(o, BYTE_STREAM))      \
        SET(o, Byte, val);                      \
    else if (STREAM_CHECK(o, LONG_LONG_STREAM)) \
        SET(o, LongLong, val);                  \
    else                                        \
        assert(0);

/* ------------------------------------- */
/* Object definitions                    */

/* abstract numeric stream object */

#define NumericStreamObject_HEAD \
    PyObject_HEAD \
    char isNone;

typedef struct {
    NumericStreamObject_HEAD
} NumericStreamObject;

typedef struct {
    NumericStreamObject_HEAD
    int32_t val;
} IntStreamObject;

typedef struct {
    NumericStreamObject_HEAD
    int16_t val;
} ShortStreamObject;

typedef struct {
    NumericStreamObject_HEAD
    unsigned char val;
} ByteStreamObject;

typedef struct {
    NumericStreamObject_HEAD
    uint64_t val;
} LongLongStreamObject;

/* ------------------------------------- */
/* NumericStream Implementation          */

static inline PyObject * raw_IntStream_Freeze(IntStreamObject * o);
static inline PyObject * raw_ShortStream_Freeze(ShortStreamObject * o);
static inline PyObject * raw_ByteStream_Freeze(ByteStreamObject * o);
static inline PyObject * raw_LongLongStream_Freeze(LongLongStreamObject * o);

static int raw_NumericStream_Thaw(NumericStreamObject * self, char * frozen,
                                  int frozenLen);

static PyObject * NumericStream_Call(PyObject * self, PyObject * args,
                                     PyObject * kwargs) {
    NumericStreamObject * o = (void *) self;
    static char * kwlist[] = { NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "", kwlist))
        return NULL;

    if (o->isNone) {
        Py_INCREF(Py_None);
        return Py_None;
    }

    if (STREAM_CHECK(self, INT_STREAM)) {
        IntStreamObject * o = (void *) self;
        return PyInt_FromLong(o->val);
    } else if (STREAM_CHECK(self, SHORT_STREAM)) {
        ShortStreamObject * o = (void *) self;
        return PyInt_FromLong(o->val);
    } else if (STREAM_CHECK(self, BYTE_STREAM)) {
        ByteStreamObject * o = (void *) self;
        return PyInt_FromLong(o->val);
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        LongLongStreamObject * o = (void *) self;
        return PyLong_FromUnsignedLongLong(o->val);
    }

    PyErr_SetString(PyExc_TypeError, "invalid type");
    return NULL;
}

static int NumericStream_Cmp(PyObject * self, PyObject * other) {
    if (self->ob_type != other->ob_type) {
        PyErr_SetString(PyExc_TypeError, "invalid type");
        return -1;
    }

    if (ISNONE(self) && ISNONE(other)) {
        return 0;
    } else if (STREAM_CHECK(self, INT_STREAM)) {
        if (VALUE(self, Int) == VALUE(other, Int))
            return 0;
        else if (VALUE(self, Int) < VALUE(other, Int))
            return -1;

        return 1;
    } else if (STREAM_CHECK(self, SHORT_STREAM)) {
        if (VALUE(self, Short) == VALUE(other, Short))
            return 0;
        else if (VALUE(self, Short) < VALUE(other, Short))
            return -1;

        return 1;
    } else if (STREAM_CHECK(self, BYTE_STREAM)) {
        if (VALUE(self, Byte) == VALUE(other, Byte))
            return 0;
        else if (VALUE(self, Byte) < VALUE(other, Byte))
            return -1;

        return 1;
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        if (VALUE(self, LongLong) == VALUE(other, LongLong))
            return 0;
        else if (VALUE(self, LongLong) < VALUE(other, LongLong))
            return -1;

        return 1;
    }
    assert(0);
}

static PyObject * NumericStream_Diff(PyObject * self, PyObject * args) {
    PyObject * them;

    if (!PyArg_ParseTuple(args, "O", &them))
        return NULL;

    if (self->ob_type != them->ob_type) {
        PyErr_SetString(PyExc_ValueError, "mismatched types for diff");
        return NULL;
    }

    if (STREAM_CHECK(self, INT_STREAM)) {
        IntStreamObject * o1 = (void *) self;
        IntStreamObject * o2 = (void *) them;

        if ((o1->isNone != o2->isNone) || (o1->val != o2->val))
            return raw_IntStream_Freeze(o1);
    } else if (STREAM_CHECK(self, SHORT_STREAM)) {
        ShortStreamObject * o1 = (void *) self;
        ShortStreamObject * o2 = (void *) them;

        if ((o1->isNone != o2->isNone) || (o1->val != o2->val))
            return raw_ShortStream_Freeze(o1);
    } else if (STREAM_CHECK(self, BYTE_STREAM)) {
        ByteStreamObject * o1 = (void *) self;
        ByteStreamObject * o2 = (void *) them;

        if ((o1->isNone != o2->isNone) || (o1->val != o2->val))
            return raw_ByteStream_Freeze(o1);
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        LongLongStreamObject * o1 = (void *) self;
        LongLongStreamObject * o2 = (void *) them;

        if ((o1->isNone != o2->isNone) || (o1->val != o2->val))
            return raw_LongLongStream_Freeze(o1);
    } else {
        PyErr_SetString(PyExc_TypeError, "invalid type");
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject * NumericStream_Eq(PyObject * self, PyObject * args,
                                   PyObject * kwargs) {
    static char * kwlist[] = { "other", "skipSet", NULL };
    PyObject * other, * skipSet = NULL;
    int rc;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O", kwlist, &other,
                                     &skipSet))
        return NULL;

    /* We just ignore skipSet */
    rc = NumericStream_Cmp(self, other);
    /* check for an exception */
    if (rc < 0 && PyErr_Occurred())
        return NULL;

    if (!rc) {
        Py_INCREF(Py_True);
        return Py_True;
    }

    Py_INCREF(Py_False);
    return Py_False;
}

static long IntStream_Hash(PyObject * self) {
    IntStreamObject * o = (void *) self;
    return o->val;
}

static long ShortStream_Hash(PyObject * self) {
    ShortStreamObject * o = (void *) self;
    return o->val;
}

static long ByteStream_Hash(PyObject * self) {
    ByteStreamObject * o = (void *) self;
    return o->val;
}

static long LongLongStream_Hash(PyObject * self) {
    LongLongStreamObject * o = (void *) self;
    return PyObject_Hash(PyLong_FromUnsignedLongLong(o->val));
}

static inline PyObject * raw_IntStream_Freeze(IntStreamObject * o) {
    int ordered = htonl(o->val);
    char buffer[20];

    if (o->isNone)
        return PyString_FromString("");

    memcpy(buffer, &ordered, sizeof(ordered));
    return PyString_FromStringAndSize(buffer, sizeof(ordered));
}

static inline PyObject * raw_ShortStream_Freeze(ShortStreamObject * o) {
    short ordered = htons(o->val);
    char buffer[20];

    if (o->isNone)
        return PyString_FromString("");

    memcpy(buffer, &ordered, sizeof(ordered));
    return PyString_FromStringAndSize(buffer, sizeof(ordered));
}

static inline PyObject * raw_ByteStream_Freeze(ByteStreamObject * o) {
    if (o->isNone)
        return PyString_FromString("");

    return (PyObject *) PyString_FromStringAndSize((const char *) &o->val, 1);
}

static inline PyObject * raw_LongLongStream_Freeze(LongLongStreamObject * o) {
    int high = htonl(o->val >> 32 & 0xffffffff);
    int low = htonl(o->val & 0xffffffff);
    char buffer[40];

    if (o->isNone)
        return PyString_FromString("");

    memcpy(buffer, &high, sizeof(high));
    memcpy(buffer + sizeof(high), &low, sizeof(low));
    return PyString_FromStringAndSize(buffer, sizeof(low) * 2);
}

static PyObject * NumericStream_Freeze(NumericStreamObject * self, 
                                       PyObject * args,
                                       PyObject * kwargs) {
    PyObject * skipSet = NULL;
    static char * kwlist[] = { "skipSet", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &skipSet))
        return NULL;

    if (self->isNone)
        return PyString_FromString("");

    if (STREAM_CHECK(self, INT_STREAM)) {
        return raw_IntStream_Freeze((IntStreamObject *) self);
    } else if (STREAM_CHECK(self, SHORT_STREAM)) {
        return raw_ShortStream_Freeze((ShortStreamObject *) self);
    } else if (STREAM_CHECK(self, BYTE_STREAM)) {
        return raw_ByteStream_Freeze((ByteStreamObject *) self);
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        return raw_LongLongStream_Freeze((LongLongStreamObject *) self);
    } else {
        PyErr_SetString(PyExc_TypeError, "invalid type");
        return NULL;
    }
    
    Py_INCREF(Py_None);
    return Py_None;
}

static int NumericStream_Init(PyObject * self, PyObject * args,
                              PyObject * kwargs) {
    NumericStreamObject * o = (void *) self;
    PyObject * initObj = NULL;
    static char * kwlist[] = { "frozen", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", kwlist, &initObj)) {
        return -1;
    }

    if (initObj && PyString_CheckExact(initObj)) {
        char * frozen;
        Py_ssize_t frozenLen;

        PyString_AsStringAndSize(initObj, &frozen, &frozenLen);
        if (!raw_NumericStream_Thaw(o, frozen, frozenLen))
            return 1;
    } else if (initObj && PyInt_Check(initObj)) {
        NUMERICSTREAM_SET(self, PyInt_AsLong(initObj));
    } else if (initObj && PyLong_Check(initObj) && (STREAM_CHECK(o, LONG_LONG_STREAM))) {
        SET(o, LongLong, PyLong_AsUnsignedLongLong(initObj));
    } else if (initObj == Py_None) {
        o->isNone = 1;
    } else if (initObj) {
        PyErr_SetString(PyExc_TypeError, "invalid type for initialization");
        return -1;
    } else {
        o->isNone = 1;
    }

    return 0;
}

static PyObject * NumericStream_Set(PyObject * self, PyObject * args) {
    PyObject *pval;
    int val;
    NumericStreamObject * o = (void *) self;

    o->isNone = 0;

    if (!PyArg_ParseTuple(args, "O", &pval))
	return NULL;

    if (pval == Py_None) {
	Py_DECREF(pval);
	NUMERICSTREAM_SET(self, 0);
	o->isNone = 1;
	Py_INCREF(Py_None);
	return Py_None;
    }
    if (STREAM_CHECK(self, INT_STREAM)
	|| STREAM_CHECK(self, SHORT_STREAM)
	|| STREAM_CHECK(self, BYTE_STREAM)) {
	PyObject *ival;
	if (!PyNumber_Check(pval)) {
	    PyErr_SetString(PyExc_TypeError, "invalid type");
	    return NULL;
	}
	ival = PyNumber_Int(pval);
	val = PyInt_AsLong(ival);
	NUMERICSTREAM_SET(self, val);
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        LongLongStreamObject * o = (void *) self;
	unsigned long long lval;
	if (!PyLong_Check(pval) && !PyNumber_Check(pval)) {
	    PyErr_SetString(PyExc_TypeError, "invalid type");
	    return NULL;
	}
	if (PyLong_Check(pval))
	    lval = PyLong_AsUnsignedLongLong(pval);
	else {
	    PyObject *ival = PyNumber_Int(pval);
	    lval = PyInt_AsLong(ival);
	}
        o->val = lval;
    } else {
        PyErr_SetString(PyExc_TypeError, "unknown numeric stream type");
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static int raw_NumericStream_Thaw(NumericStreamObject * self, char * frozen,
                                  int frozenLen) {
    self->isNone = 0;

    if (STREAM_CHECK(self, INT_STREAM)) {
        IntStreamObject * o = (void *) self;

        if (frozenLen && frozenLen != 4) {
            PyErr_SetString(PyExc_ValueError,
                    "Frozen int stream must be 4 bytes long or empty");
            return 0;
        }

        if (!frozenLen)
            o->isNone = 1;
        else
            o->val = ntohl(*((int32_t *) frozen));
    } else if (STREAM_CHECK(self, SHORT_STREAM)) {
        ShortStreamObject * o = (void *) self;

        if (frozenLen && frozenLen != 2) {
            PyErr_SetString(PyExc_ValueError,
                    "Frozen short stream must be 2 bytes long or empty");
            return 0;
        }

        if (!frozenLen)
            o->isNone = 1;
        else
            o->val = ntohs(*((int16_t *) frozen));
    } else if (STREAM_CHECK(self, BYTE_STREAM)) {
        ByteStreamObject * o = (void *) self;

        if (frozenLen && frozenLen != 1) {
            PyErr_SetString(PyExc_ValueError,
			    "Frozen byte stream must be 1 byte long or empty");
            return 0;
        }

        if (!frozenLen)
            o->isNone = 1;
        else
            o->val = (*((char *) frozen));
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        LongLongStreamObject * o = (void *) self;

        if (frozenLen && frozenLen != 8) {
            PyErr_SetString(PyExc_ValueError,
                    "Frozen long long stream must be 8 bytes long or empty");
            return 0;
        }

        if (!frozenLen)
            o->isNone = 1;
        else {
	    o->val = (uint64_t) ntohl(*((uint32_t *) frozen)) << 32;
	    frozen += 4;
	    o->val |= (uint64_t) ntohl(*((uint32_t *) frozen));
	    return 0;
	}
    } else {
        PyErr_SetString(PyExc_TypeError, "invalid type");
        return 1;
    }

    return 0;
}

static PyObject * NumericStream_Thaw(PyObject * self, PyObject * args) {
    char * frozen;
    int frozenLen;

    if (!PyArg_ParseTuple(args, "s#", &frozen, &frozenLen))
        return NULL;

    if (raw_NumericStream_Thaw((NumericStreamObject *) self, frozen, frozenLen))
        return NULL;
    
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject * NumericStream_Twm(PyObject * self, PyObject * args) {
    char * diff;
    int diffLen;
    NumericStreamObject * other;
    PyObject * retVal;

    if (!PyArg_ParseTuple(args, "s#O", &diff, &diffLen, &other,
                          NumericStreamType))
        return NULL;

    if (self->ob_type != other->ob_type) {
        PyErr_SetString(PyExc_TypeError, "stream type mistmatch");
        return NULL;
    }

    if (STREAM_CHECK(self, INT_STREAM)) {
        IntStreamObject * o = (void *) self;
        IntStreamObject * base = (void *) other;
        int newVal = 0;

        assert(diffLen == 0 || diffLen == 4);

        if (diffLen)
            newVal = ntohl(*((int32_t *) diff));

        if (o->isNone == base->isNone && o->val == base->val) {
            if (!diffLen)
                o->isNone = 1;
            else {
                o->isNone = 0;
                o->val = newVal;
            }

            retVal = Py_False;
        } else if ((o->isNone && diffLen) ||
                   (!o->isNone && !diffLen) ||
                   (!o->isNone && diffLen && o->val != newVal))
            retVal = Py_True;
        else
            retVal = Py_False;
    } else if (STREAM_CHECK(self, SHORT_STREAM)) {
        ShortStreamObject * o = (void *) self;
        ShortStreamObject * base = (void *) other;
        int newVal = 0;

        assert(diffLen == 0 || diffLen == 2);

        if (diffLen)
            newVal = ntohs(*((int16_t *) diff));
        if (o->isNone == base->isNone && o->val == base->val) {
            if (!diffLen)
                o->isNone = 1;
            else {
                o->isNone = 0;
                o->val = newVal;
            }

            retVal = Py_False;
        } else if ((o->isNone && diffLen) ||
                   (!o->isNone && !diffLen) ||
                   (!o->isNone && diffLen && o->val != newVal))
            retVal = Py_True;
        else
            retVal = Py_False;
    } else if (STREAM_CHECK(self, BYTE_STREAM)) {
        ByteStreamObject * o = (void *) self;
        ByteStreamObject * base = (void *) other;
        char newVal = '\0';

        assert(diffLen == 0 || diffLen == 1);

        if (diffLen)
            newVal = *((char *) diff);

        if (o->isNone == base->isNone && o->val == base->val) {
            if (!diffLen)
                o->isNone = 1;
            else {
                o->isNone = 0;
                o->val = newVal;
            }

            retVal = Py_False;
        } else if ((o->isNone && diffLen) ||
                   (!o->isNone && !diffLen) ||
                   (!o->isNone && diffLen && o->val != newVal))
            retVal = Py_True;
        else
            retVal = Py_False;
    } else if (STREAM_CHECK(self, LONG_LONG_STREAM)) {
        LongLongStreamObject * o = (void *) self;
        LongLongStreamObject * base = (void *) other;
        uint64_t newVal = 0;

        assert(diffLen == 0 || diffLen == 8);

        if (diffLen) {
	    newVal = (uint64_t) ntohl(*((int32_t *) diff)) << 32;
	    newVal |= (uint64_t) ntohl(*((int32_t *) (diff + 4)));
	}

        if (o->isNone == base->isNone && o->val == base->val) {
            if (!diffLen)
                o->isNone = 1;
            else {
                o->isNone = 0;
                o->val = newVal;
            }

            retVal = Py_False;
        } else if ((o->isNone && diffLen) ||
                   (!o->isNone && !diffLen) ||
                   (!o->isNone && diffLen && o->val != newVal))
            retVal = Py_True;
        else
            retVal = Py_False;
    } else {
	PyErr_SetString(PyExc_TypeError, "invalid type");
	return NULL;
    }

    Py_INCREF(retVal);
    return retVal;
}

/* ------------------------------------- */
/* Type and method definition            */

static PyMethodDef NumericStreamMethods[] = {
    { "diff", (PyCFunction) NumericStream_Diff, METH_VARARGS,
      "Find the difference between two streams." },
    { "__eq__", (PyCFunction) NumericStream_Eq, METH_VARARGS | METH_KEYWORDS, 
      NULL},
    { "freeze", (PyCFunction) NumericStream_Freeze, 
      METH_VARARGS | METH_KEYWORDS,
      "Freeze a numeric stream." },
    { "set", (PyCFunction) NumericStream_Set, METH_VARARGS,
      "Set the value of the numeric stream." },
    { "thaw", (PyCFunction) NumericStream_Thaw, METH_VARARGS,
      "Thaw a numeric stream." },
    { "twm", (PyCFunction) NumericStream_Twm, METH_VARARGS,
      "Perform three way merge." },
    {NULL}  /* Sentinel */
};

PyTypeObject NumericStreamType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.NumericStream",       /*tp_name*/
    sizeof(NumericStreamObject),    /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    0,                              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    NumericStream_Cmp,              /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    0,                              /*tp_hash */
    NumericStream_Call,             /*tp_call*/
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
    NumericStreamMethods,           /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    NumericStream_Init,             /* tp_init */
};

PyTypeObject IntStreamType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.IntStream",	    /*tp_name*/
    sizeof(IntStreamObject),        /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    0,                              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    NumericStream_Cmp,              /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    IntStream_Hash,                 /*tp_hash */
    0,                              /*tp_call*/
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
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    &NumericStreamType,             /* tp_base */
};

PyTypeObject ShortStreamType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.ShortStream",	    /*tp_name*/
    sizeof(ShortStreamObject),      /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    0,                              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    NumericStream_Cmp,              /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    ShortStream_Hash,               /*tp_hash */
    0,                              /*tp_call*/
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
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    &NumericStreamType,             /* tp_base */
};

PyTypeObject ByteStreamType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.ByteStream",	    /*tp_name*/
    sizeof(ByteStreamObject),      /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    0,                              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    NumericStream_Cmp,              /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    ByteStream_Hash,               /*tp_hash */
    0,                              /*tp_call*/
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
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    &NumericStreamType,             /* tp_base */
};

PyTypeObject LongLongStreamType = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "cstreams.LongLongStream",	    /*tp_name*/
    sizeof(LongLongStreamObject),      /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    0,                              /*tp_dealloc*/
    0,                              /*tp_print*/
    0,                              /*tp_getattr*/
    0,                              /*tp_setattr*/
    NumericStream_Cmp,              /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    LongLongStream_Hash,               /*tp_hash */
    0,                              /*tp_call*/
    0,                              /*tp_str*/
    0,                              /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT,             /*tp_flags*/
    NULL,                           /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    &NumericStreamType,             /* tp_base */
};

void numericstreaminit(PyObject * m) {
    allStreams[NUMERIC_STREAM].pyType = NumericStreamType;
    allStreams[INT_STREAM].pyType     = IntStreamType;
    allStreams[SHORT_STREAM].pyType   = ShortStreamType;
    allStreams[BYTE_STREAM].pyType    = ByteStreamType;
    allStreams[LONG_LONG_STREAM].pyType = LongLongStreamType;
}
