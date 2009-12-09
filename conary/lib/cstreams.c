/*
 * Copyright (c) 2005-2009 rPath, Inc.
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
#include <string.h>

#include "pycompat.h"
#include "cstreams.h"

/* debugging aid */
#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

/* ------------------------------------- */
/* Module initialization                 */

static PyMethodDef CStreamsMethods[] = {
    { "splitFrozenStreamSet", StreamSet_split, METH_VARARGS },
    { "whiteOutFrozenStreamSet", StreamSet_remove, METH_VARARGS },
    { NULL },
};

#if PY_MAJOR_VERSION >= 3
static PyModuleDef CStreamsModule = {
	PyModuleDef_HEAD_INIT,
	"cstreams",
	"",
	-1,
	CStreamsMethods
};
#endif

struct singleStream allStreams[];

PYMODULE_INIT(cstreams)
{
    PyObject* m;
    int i;

    m = PYMODULE_CREATE("cstreams", CStreamsMethods, "", &CStreamsModule);
    if (m == NULL)
        PYMODULE_RETURN(NULL);

    streamsetinit(m);
    numericstreaminit(m);
    stringstreaminit(m);

    for (i = 0; i < (sizeof(allStreams) / sizeof(*allStreams)); i++) {
        char * name;

	allStreams[i].pyType.tp_new = PyType_GenericNew;
        if (PyType_Ready(&allStreams[i].pyType) < 0)
            PYMODULE_RETURN(NULL);
        Py_INCREF(&allStreams[i].pyType);
        name = strrchr(allStreams[i].pyType.tp_name, '.') + 1;
        PyModule_AddObject(m, name, (PyObject *) &allStreams[i].pyType);
    }
    PyModule_AddObject(m, "SMALL", (PyObject *) PyLong_FromLong(SMALL));
    PyModule_AddObject(m, "LARGE", (PyObject *) PyLong_FromLong(LARGE));
    PyModule_AddObject(m, "DYNAMIC", (PyObject *) PyLong_FromLong(DYNAMIC));
    PyModule_AddObject(m, "SKIP_UNKNOWN",
                       (PyObject *) PyLong_FromLong(SKIP_UNKNOWN));
    PyModule_AddObject(m, "PRESERVE_UNKNOWN",
                       (PyObject *) PyLong_FromLong(PRESERVE_UNKNOWN));

    PYMODULE_RETURN(m);
}

/* vim: set sts=4 sw=4 expandtab : */
