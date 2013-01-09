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

void numericstreaminit(PyObject * m);
void streamsetinit(PyObject * m);
void stringstreaminit(PyObject * m);

PyObject *StreamSet_split(PyObject *self, PyObject *args);
PyObject *StreamSet_remove(PyObject *self, PyObject *args);

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

#define STREAM_CHECK(x, t) PyObject_TypeCheck((x), (PyTypeObject *) &allStreams[t])

#define SMALL 0
#define LARGE 1
#define DYNAMIC 2

#define SKIP_UNKNOWN        1
#define PRESERVE_UNKNOWN    2

/* vim: set sts=4 sw=4 expandtab : */
