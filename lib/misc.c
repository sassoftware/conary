/*
 *
 * Copyright (c) 2004 Specifix, Inc.
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
 */

#include <Python.h>

#include <malloc.h>

static PyObject * malloced(PyObject *self, PyObject *args);

static PyMethodDef MiscMethods[] = {
    { "malloced", malloced, METH_VARARGS, 
	"amount of memory currently allocated through malloc()" }
};

static PyObject * malloced(PyObject *self, PyObject *args) {
    struct mallinfo ma;

    ma = mallinfo();

    /* worked */
    return Py_BuildValue("i", ma.uordblks);
}

PyMODINIT_FUNC
initmisc(void)
{
    Py_InitModule3("misc", MiscMethods, 
		   "miscelaneous low-level C functions for conary");
}
