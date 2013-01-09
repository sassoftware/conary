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
