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
#include <dlfcn.h>
#include "pycompat.h"

static PyObject * doOpen(PyObject *self, PyObject *args) {
    char * fileName;

    if (!PyArg_ParseTuple(args, "s", &fileName)) {
	return NULL;
    }

    if (dlopen(fileName, RTLD_NOW | RTLD_GLOBAL) == NULL) {
        PyErr_SetString(PyExc_RuntimeError, dlerror());
	return NULL;
    }

    Py_RETURN_NONE;
}

static PyMethodDef RPMNoLockMethods[] = {
    { "open", doOpen, METH_VARARGS, 
	"load an override module" },
    { NULL }
};


PYMODULE_DECLARE(norpmlock, "norpmlock", "Override module for testing RPM",
		RPMNoLockMethods);


/* vim: set sts=4 sw=4 expandtab : */
