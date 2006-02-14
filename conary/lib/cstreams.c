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
#include <string.h>

#include "cstreams.h"

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

/* ------------------------------------- */
/* Module initialization                 */

extern PyObject * collYank(PyObject *self, PyObject *args);

static PyMethodDef CStreamsMethods[] = {
    { "collYank", collYank, METH_VARARGS },
    {NULL}  /* Sentinel */
};

struct singleStream allStreams[];

PyMODINIT_FUNC
initcstreams(void) 
{
    PyObject* m;
    int i;

    m = Py_InitModule3("cstreams", CStreamsMethods, "");

    streamsetinit(m);
    numericstreaminit(m);
    stringstreaminit(m);

    for (i = 0; i < (sizeof(allStreams) / sizeof(*allStreams)); i++) {
        char * name;

	allStreams[i].pyType.tp_new = PyType_GenericNew;
        if (PyType_Ready(&allStreams[i].pyType) < 0)
            return;
        Py_INCREF(&allStreams[i].pyType);
        name = strrchr(allStreams[i].pyType.tp_name, '.') + 1;
        PyModule_AddObject(m, name, (PyObject *) &allStreams[i].pyType);
    }
    PyModule_AddObject(m, "SMALL", (PyObject *) PyInt_FromLong(SMALL));
    PyModule_AddObject(m, "LARGE", (PyObject *) PyInt_FromLong(LARGE));
    PyModule_AddObject(m, "DYNAMIC", (PyObject *) PyInt_FromLong(DYNAMIC));
}
