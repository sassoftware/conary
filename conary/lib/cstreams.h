/*
 * Copyright (c) 2005 rPath, Inc.
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

void numericstreaminit(PyObject * m);
void streamsetinit(PyObject * m);
void stringstreaminit(PyObject * m);

PyObject *StreamSet_split(PyObject *self, PyObject *args);

extern PyTypeObject StringStreamType;
extern PyTypeObject NumericStreamType;
extern PyTypeObject IntStreamType;
extern PyTypeObject ShortStreamType;
extern PyTypeObject StreamSetType;
extern PyTypeObject ByteStreamType;
extern PyTypeObject LongLongStreamType;

#define STRING_STREAM	    0
#define NUMERIC_STREAM	    1
#define INT_STREAM	    2
#define SHORT_STREAM	    3
#define STREAM_SET	    4
#define BYTE_STREAM	    5
#define LONG_LONG_STREAM    6

struct singleStream {
    PyTypeObject pyType;
};

extern struct singleStream allStreams[7];

#define STREAM_CHECK(x, t) PyType_IsSubtype(x->ob_type, (PyTypeObject *) &allStreams[t])

#define SMALL 0
#define LARGE 1
#define DYNAMIC 2

#define SKIP_UNKNOWN        1
#define PRESERVE_UNKNOWN    2
