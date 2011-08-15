/*
 * Copyright (c) rPath, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
