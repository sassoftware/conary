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

/* ------------------------------------- */
/* Module initialization                 */

static PyMethodDef CStreamsMethods[] = {
    {NULL}  /* Sentinel */
};

void numstreaminit(PyObject * m);
void streamsetinit(PyObject * m);
void stringstreaminit(PyObject * m);

#ifndef PyMODINIT_FUNC  /* declarations for DLL import/export */
#define PyMODINIT_FUNC void
#endif
PyMODINIT_FUNC
initcstreams(void) 
{
    PyObject* m;

    m = Py_InitModule3("cstreams", CStreamsMethods, "");

    numstreaminit(m);
    streamsetinit(m);
    stringstreaminit(m);
}

/*
vim:ts=4:sw=4:et
*/
