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
