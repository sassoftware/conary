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


/*
 * How to declare a module:
 *
 *  PYMODULE_DECLARE(mymodule, MyMethods, "my docstring");
 *
 * - or -
 *
 *  static PyModuleDef MyModule = {
 *      PyModuleDef_HEAD_INIT,
 *      "mymodule",
 *      "my docstring",
 *      -1,
 *      MyMethods
 *      };
 *
 *  PYMODULE_INIT(mymodule) {
 *      PyObject *m = PYMODULE_CREATE(&MyModule);
 *      do_stuff();
 *      PYMODULE_RETURN(m);
 *  }
 */

#ifndef __PYCOMPAT_H
#define __PYCOMPAT_H

/* stupid trick to work around cpp macro handling */
#define _PASTE(x,y) x##y
#define _PASTE2(x,y) _PASTE(x,y)

# define PYMODULE_DECLARE(name, methods, doc) \
    static PyModuleDef _module_def = { PyModuleDef_HEAD_INIT, \
        #name, doc, -1, methods }; \
    PYMODULE_INIT(name) { PYMODULE_RETURN(PYMODULE_CREATE(&_module_def)); }

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

# define PYMODULE_INIT(name)        PyMODINIT_FUNC _PASTE2(init, name)(void)
# define PYMODULE_CREATE(moddef) \
    Py_InitModule3((moddef)->m_name, (moddef)->m_methods, (moddef)->m_doc)
# define PYMODULE_RETURN(value)     { if(value) ; return; }

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
# define PYMODULE_CREATE(moddef)    PyModule_Create((moddef))
# define PYMODULE_RETURN(value)     return value

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


/* Partial backport from 3.1 to make PYMODULE simpler */
#if PY_MAJOR_VERSION < 3
#define PyModuleDef_HEAD_INIT       NULL
typedef struct PyModuleDef {
    void *dummy;
    char *m_name;
    char *m_doc;
    int m_size;
    PyMethodDef *m_methods;
} PyModuleDef;
#endif


#endif
/* vim: set sts=4 sw=4 expandtab : */
