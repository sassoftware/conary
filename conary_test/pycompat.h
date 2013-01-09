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


#ifndef __PYCOMPAT_H
#define __PYCOMPAT_H

/* stupid trick to work around cpp macro handling */
#define _PASTE(x,y) x##y
#define _PASTE2(x,y) _PASTE(x,y)


#if PY_MAJOR_VERSION < 3
/* Macros for python 2.x */

# define PYBYTES                    PyString
# define PYINT                      PyInt
# define PYSTR                      PyString
# define PYSTR_RAW                  char
# define PYSTR_AS_STRING            PyString_AS_STRING

# define PYINT_CHECK_EITHER(x) \
    ( PyInt_CheckExact((x)) || PyLong_CheckExact((x)) )
# define PYLONG_AS_ULL(x) \
    ( PyInt_CheckExact((x)) ? \
        PyLong_AsUnsignedLong((x)) : \
        PyLong_AsUnsignedLongLong((x)) )

# define PYMODULE_INIT(name) \
    PyMODINIT_FUNC _PASTE2(init, name)(void)
# define PYMODULE_CREATE(name, functions, docstr, moddef) \
    Py_InitModule3(name, functions, docstr)
# define PYMODULE_RETURN(value)     { if(value) ; return; }

# define PYMODULE_DECLARE(name, fullname, doc, methods) \
    PYMODULE_INIT(name) { Py_InitModule3(fullname, methods, doc); }

#else
/* Macros for python 3.x */

# define PYBYTES                    PyBytes
# define PYINT                      PyLong
# define PYSTR                      PyUnicode
# define PYSTR_RAW                  Py_UNICODE
# define PYSTR_AS_STRING            PyUnicode_AS_UNICODE

# define PYINT_CHECK_EITHER(x)      PyLong_CheckExact((x))
# define PYLONG_AS_ULL(x)           PyLong_AsUnsignedLongLong((x))

# define PYMODULE_INIT(name)        PyMODINIT_FUNC _PASTE2(PyInit_, name)(void)
# define PYMODULE_CREATE(name, functions, docstr, moddef) \
    PyModule_Create(moddef)
# define PYMODULE_RETURN(value)     return value

# define PYMODULE_DECLARE(name, fullname, doc, methods) \
    static PyModuleDef _module_def = { PyModuleDef_HEAD_INIT, \
        fullname, doc, -1, methods }; \
    PYMODULE_INIT(name) { return PyModule_Create(_module_def); }

#endif


/* templated macros */
#define _PYBYTES(name) _PASTE2(PYBYTES, _##name)
#define _PYINT(name) _PASTE2(PYINT, _##name)
#define _PYSTR(name) _PASTE2(PYSTR, _##name)


/* Use PYBYTES for storing byte arrays. */
#define PYBYTES_AS_STRING           _PYBYTES(AS_STRING)
#define PYBYTES_AsString            _PYBYTES(AsString)
#define PYBYTES_AsStringAndSize     _PYBYTES(AsStringAndSize)
#define PYBYTES_Check               _PYBYTES(Check)
#define PYBYTES_CheckExact          _PYBYTES(CheckExact)
#define PYBYTES_FromString          _PYBYTES(FromString)
#define PYBYTES_FromStringAndSize   _PYBYTES(FromStringAndSize)
#define PYBYTES_GET_SIZE            _PYBYTES(GET_SIZE)
#define PYBYTES_Size                _PYBYTES(Size)
#define PYBYTES_Type                _PYBYTES(Type)


/* Use PYINT for small integers ( well under 2^31 ) */
#define PYINT_AS_LONG               _PYINT(AS_LONG)
#define PYINT_Check                 _PYINT(Check)
#define PYINT_CheckExact            _PYINT(CheckExact)
#define PYINT_FromLong              _PYINT(FromLong)


/* Use PYSTR for handling character data when continuing to use 'str' on 2.x is
 * desireable.
 */
#define PYSTR_FromStringAndSize     _PYSTR(FromStringAndSize)


/* Backports from 2.6 */
#if PY_VERSION_HEX < 0x02060000
# define Py_TYPE(ob)                (ob)->ob_type
# define Py_SIZE(ob)                (ob)->ob_size
# define PyVarObject_HEAD_INIT(type, size) PyObject_HEAD_INIT(type) size,
#endif


#endif
/* vim: set sts=4 sw=4 expandtab : */
