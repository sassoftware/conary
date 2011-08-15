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
#include <netinet/in.h>
#include <string.h>

#include "pycompat.h"
#include "streams.h"

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

static PyModuleDef CStreamsModule = {
	PyModuleDef_HEAD_INIT,
	"streams",
	"",
	-1,
	CStreamsMethods
};

struct singleStream allStreams[];

PYMODULE_INIT(streams)
{
    PyObject* m;
    int i;

    m = PYMODULE_CREATE(&CStreamsModule);
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
