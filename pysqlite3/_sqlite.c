/* -*- Mode: C; c-basic-offset: 8 tab-width: 8 indent-tabs-mode: t -*-
**
**                                            _ _ _
**                      _ __  _   _ ___  __ _| (_) |_ ___
**                     | '_ \| | | / __|/ _` | | | __/ _ \
**                     | |_) | |_| \__ \ (_| | | | ||  __/
**                     | .__/ \__, |___/\__, |_|_|\__\___|
**                     |_|    |___/        |_|
**
**               A DB API v2.0 compatible interface to SQLite
**                       Embedded Relational Database.
**                          Copyright (c) 2001-2003
**                  Michael Owens <mike@mikesclutter.com>
**                     Gerhard Häring <gh@ghaering.de>
**
**          Portions Copyright (c) 2004-2005 rPath, Inc.
**                                           Matt Wilson <msw@specifix.com>
**              Portions Copyright (c) 2005  rpath, Inc.
**                                           Matt Wilson <msw@rpath.com>
**
** All Rights Reserved
**
** Permission to use, copy, modify, and distribute this software and its
** documentation for any purpose and without fee is hereby granted, provided
** that the above copyright notice appear in all copies and that both that
** copyright notice and this permission notice appear in supporting
** documentation,
**
** This program is distributed in the hope that it will be useful, but WITHOUT
** ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
** FITNESS FOR A PARTICULAR PURPOSE.
*/

#include "Python.h"
#include "structmember.h"

#include "sqlite3.h"

#include "port/strsep.h"

/* Compatibility macros
 *
 * From Python 2.2 to 2.3, the way to export the module init function
 * has changed. These macros keep the code compatible to both ways.
 */
#if PY_VERSION_HEX >= 0x02030000
#  define PySQLite_DECLARE_MODINIT_FUNC(name) PyMODINIT_FUNC name(void)
#  define PySQLite_MODINIT_FUNC(name)         PyMODINIT_FUNC name(void)
#else
#  define PySQLite_DECLARE_MODINIT_FUNC(name) void name(void)
#  define PySQLite_MODINIT_FUNC(name)         DL_EXPORT(void) name(void)
#endif

/*
 * These are needed because there is no "official" way to specify
 * WHERE to save the thread state.
 */
#ifdef WITH_THREAD
#  define MY_BEGIN_ALLOW_THREADS(st)    \
    { st = PyEval_SaveThread(); }
#  define MY_END_ALLOW_THREADS(st)      \
    { PyEval_RestoreThread(st); st = NULL; }
#else
#  define MY_BEGIN_ALLOW_THREADS(st)
#  define MY_END_ALLOW_THREADS(st)      { st = NULL; }
#endif

enum {
	INTEGER,
	FLOAT,
	TIMESTAMP,
	TIME,
	DATE,
	INTERVAL,
	STRING,
	UNICODESTRING,
	BINARY,
	BOOLEAN,
	NULLVALUE
} row_types;

/*----------------------------------------------------------------------------
** Object Declarations
**----------------------------------------------------------------------------
*/

/** A connection object */
typedef struct
{
	PyObject_HEAD
	const char* database_name;
	const char* sql;
	sqlite3* p_db;
	PyObject* expected_types;
	PyObject* command_logfile;
	PyThreadState *tstate;
	int timeout;
	PyObject* busy_data;
} pysqlc;

/** a statement object. */
typedef struct
{
	PyObject_HEAD
	pysqlc* con;
	sqlite3_stmt* p_stmt;
	PyObject* description;
	int num_fields;
	int reset;
} pysqlstmt;


/** Exception objects */

static PyObject* _sqlite_Warning;
static PyObject* _sqlite_Error;
static PyObject* _sqlite_DatabaseError;
static PyObject* _sqlite_InterfaceError;
static PyObject* _sqlite_DataError;
static PyObject* _sqlite_OperationalError;
static PyObject* _sqlite_IntegrityError;
static PyObject* _sqlite_InternalError;
static PyObject* _sqlite_ProgrammingError;
static PyObject* _sqlite_NotSupportedError;

static int debug_callbacks = 1;

#define PRINT_OR_CLEAR_ERROR \
	if (debug_callbacks) \
		PyErr_Print(); \
	else \
 		PyErr_Clear();

/*** Type codes */

static PyObject* tc_INTEGER;
static PyObject* tc_FLOAT;
static PyObject* tc_TIMESTAMP;
static PyObject* tc_DATE;
static PyObject* tc_TIME;
static PyObject* tc_INTERVAL;
static PyObject* tc_STRING;
static PyObject* tc_UNICODESTRING;
static PyObject* tc_BINARY;
static PyObject* tc_BOOLEAN;
static PyObject* tc_NULL;

/*----------------------------------------------------------------------------
** Function Prototypes
**----------------------------------------------------------------------------
*/

PySQLite_DECLARE_MODINIT_FUNC(init_sqlite3);

/* Defined in encode.c */
int sqlite_encode_binary(const unsigned char *in, int n, unsigned char *out);
int sqlite_decode_binary(const unsigned char *in, unsigned char *out);

/** Connection Object Methods */
static PyObject* _con_get_attr(pysqlc *self, char *attr);
static void _con_dealloc(pysqlc *self);

/** Statement Object Methods */
static void _stmt_dealloc(pysqlstmt* self);
static PyObject* _stmt_get_attr(pysqlstmt* self, char *attr);
static PyObject* _stmt_get_description(pysqlstmt* self);

#ifdef _MSC_VER
#define staticforward extern
#endif

staticforward PyMethodDef _con_methods[];
staticforward struct memberlist _con_memberlist[];

PyTypeObject pysqlc_Type = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "Connection",                   /*tp_name*/
    sizeof(pysqlc),                 /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    (destructor) _con_dealloc,      /*tp_dealloc*/
    0,                              /*tp_print*/
    (getattrfunc) _con_get_attr,    /*tp_getattr*/
    (setattrfunc) NULL,             /*tp_setattr*/
    0,				    /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    0,                              /*tp_hash */
    0,				    /*tp_call*/
    0,                              /*tp_str*/
    0,                              /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,             /*tp_flags*/
    NULL,                           /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
};

PyTypeObject pysqlstmt_Type = {
    PyObject_HEAD_INIT(&PyType_Type)
    0,                              /*ob_size*/
    "Statement",                    /*tp_name*/
    sizeof(pysqlstmt),              /*tp_basicsize*/
    0,                              /*tp_itemsize*/
    (destructor) _stmt_dealloc,     /*tp_dealloc*/
    0,                              /*tp_print*/
    (getattrfunc) _stmt_get_attr,   /*tp_getattr*/
    (setattrfunc) NULL,             /*tp_setattr*/
    0,				    /*tp_compare*/
    0,                              /*tp_repr*/
    0,                              /*tp_as_number*/
    0,                              /*tp_as_sequence*/
    0,                              /*tp_as_mapping*/
    0,                              /*tp_hash */
    0,				    /*tp_call*/
    0,                              /*tp_str*/
    0,                              /*tp_getattro*/
    0,                              /*tp_setattro*/
    0,                              /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,/*tp_flags*/
    NULL,                           /* tp_doc */
    0,                              /* tp_traverse */
    0,                              /* tp_clear */
    0,                              /* tp_richcompare */
    0,                              /* tp_weaklistoffset */
    0,                              /* tp_iter */
    0,                              /* tp_iternext */
    0,                              /* tp_methods */
    0,                              /* tp_members */
    0,                              /* tp_getset */
    0,                              /* tp_base */
    0,                              /* tp_dict */
    0,                              /* tp_descr_get */
    0,                              /* tp_descr_set */
    0,                              /* tp_dictoffset */
    0,                              /* tp_init */
};

static void
_con_dealloc(pysqlc* self)
{
	if(self) {
		if(self->p_db != 0) {
			/* Close the database */
			sqlite3_close(self->p_db);
			self->p_db = 0;
		}

		if(self->sql != NULL) {
			/* Free last SQL statement string */
			free((void*)self->sql);
			self->sql = NULL;
		}

		if(self->database_name != NULL) {
			/* Free database name string */
			free((void*)self->database_name);
			self->database_name = NULL;
		}

		Py_XDECREF(self->busy_data);

		Py_DECREF(self->command_logfile);

		PyObject_Del(self);
	}
}

static const char *
ctype_to_str(int ctype)
{
	switch(ctype) {
	case SQLITE_INTEGER:
		return "INTEGER";
		break;
	case SQLITE_FLOAT:
		return "FLOAT";
		break;
	case SQLITE_TEXT:
		return "TEXT";
		break;
	case SQLITE_BLOB:
		return "BLOB";
		break;
	case SQLITE_NULL:
		return NULL;
		break;
	default:
		return "TEXT";
		break;
	}

}

static char pysqlite_connect_doc[] =
"connect(path) -> Connection.\n\
Opens a new database connection.";

/* return a new instance of sqlite_connection */
static PyObject*
pysqlite_connect(PyObject *self, PyObject *args, PyObject *kwargs)
{
	const char* db_name = 0;
	int ret;
	pysqlc* obj;
	static char *kwlist[] = { "filename", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s:pysqlite_connect",
					 kwlist, &db_name)) {
		return NULL;
	}

	if ((obj = PyObject_New(pysqlc, &pysqlc_Type)) == NULL) {
		return NULL;
	}

	/* Open the database */
	ret = sqlite3_open(db_name, &obj->p_db);

	if (ret != SQLITE_OK) {
		PyObject_Del(obj);
		PyErr_SetString(_sqlite_DatabaseError,
				sqlite3_errmsg(obj->p_db));
		return NULL;
	}

	/* Assign the database name */
	if ((obj->database_name = strdup(db_name)) == NULL) {
		PyErr_SetString(PyExc_MemoryError,
				"Cannot allocate memory for database name.");
		return NULL;
	}

	/* Init sql string to NULL */
	obj->sql = NULL;

	/* Set the thread state to NULL */
	obj->tstate = NULL;
	obj->timeout = 0;
	obj->busy_data = NULL;

	Py_INCREF(Py_None);
	obj->command_logfile = Py_None;

	return (PyObject *) obj;
}

static PyObject*
_con_get_attr(pysqlc *self, char *attr)
{
	PyObject *res;

	res = Py_FindMethod(_con_methods, (PyObject *) self,attr);

	if(NULL != res) {
		return res;
	}
	else {
		PyErr_Clear();
		return PyMember_Get((char *) self, _con_memberlist, attr);
	}
}

static char _con_close_doc [] =
"close()\n\
Close the database connection.";

static PyObject*
_con_close(pysqlc *self)
{
	if(self->p_db != 0) {
		/* Close the database */
		sqlite3_close(self->p_db);
		self->p_db = 0;
	}
	else {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Database is not open.");
		return NULL;
	}

	Py_INCREF(Py_None);

	return Py_None;
}

static void
set_result(sqlite3_context* context, PyObject *obj)
{
	/* bools are derived from int, no need to check for it explicitly */
	if (PyInt_Check(obj)) {
		sqlite3_result_int(context, PyInt_AsLong(obj));
	}
	else if (PyLong_Check(obj)) {
		sqlite3_result_int64(context, PyLong_AsLongLong(obj));
	}
	else if (PyString_Check(obj)) {
		char *buf;
		int len;

		PyString_AsStringAndSize(obj, &buf, &len);
		if (memchr(buf, '\0', len))
			sqlite3_result_blob(context, buf, len,
					    SQLITE_TRANSIENT);
		else
			sqlite3_result_text(context, buf, len,
					    SQLITE_TRANSIENT);
	}
	else if (PyUnicode_Check(obj)) {
		int len;
		Py_UNICODE *buf = PyUnicode_AsUnicode(obj);
		len = PyUnicode_GetSize(obj);
		sqlite3_result_text16(context, buf, len, SQLITE_TRANSIENT);
	}
	else if (PyFloat_Check(obj)) {
		sqlite3_result_double(context, PyFloat_AsDouble(obj));
	}
	else if (obj == Py_None) {
		sqlite3_result_null(context);
	}
	else {
		PyObject *o = NULL;
		if (PyObject_HasAttrString(obj, "__quote__"))
			o = PyObject_GetAttrString(obj, "__quote__");
		else if (PyObject_HasAttrString(obj, "_quote"))
			o = PyObject_GetAttrString(obj, "_quote");

		if (o != NULL && PyCallable_Check(o)) {
			char *buf;
			int len;
			PyObject *str = PyObject_CallObject(o, NULL);

			PyString_AsStringAndSize(str, &buf, &len);
			sqlite3_result_text(context, buf, len,
					    SQLITE_TRANSIENT);
			Py_DECREF(str);
		}
		else {
			PyObject *s = PyObject_Str(obj);
			sqlite3_result_text(context, PyString_AsString(s), -1,
					    SQLITE_TRANSIENT);
			Py_DECREF(s);
		}
	}
}

static void
function_callback(sqlite3_context *context, int argc, sqlite3_value **argv)
{
	int i;
	PyObject* function_result;
	PyObject* args;
	PyObject* userdata;
	PyObject* func;
	pysqlc* con;

	userdata = (PyObject*)sqlite3_user_data(context);
	func = PyTuple_GetItem(userdata, 0);
	con = (pysqlc*)PyTuple_GetItem(userdata, 1);
	MY_END_ALLOW_THREADS(con->tstate);

	args = PyTuple_New(argc);
	for (i = 0; i < argc; i++) {
		const char *s = sqlite3_value_text(argv[i]);
		if (s == NULL) {
			Py_INCREF(Py_None);
			PyTuple_SetItem(args, i, Py_None);
		}
		else {
			PyTuple_SetItem(args, i, PyString_FromString(s));
		}
	}

	function_result = PyObject_CallObject(func, args);
	Py_DECREF(args);

	if (PyErr_Occurred()) {
		PRINT_OR_CLEAR_ERROR
			sqlite3_result_error(context, NULL, -1);
	}
	else {
		set_result(context, function_result);
	}
	Py_XDECREF(function_result);
	MY_BEGIN_ALLOW_THREADS(con->tstate);
}

static void
aggregate_step(sqlite3_context *context, int argc, sqlite3_value **argv)
{
	int i;
	PyObject* args;
	PyObject* function_result;
	PyObject* userdata;
	PyObject* aggregate_class;
	pysqlc* con;
	PyObject** aggregate_instance;
	PyObject* stepmethod;

	userdata = (PyObject*)sqlite3_user_data(context);
	aggregate_class = PyTuple_GetItem(userdata, 0);

	con = (pysqlc*)PyTuple_GetItem(userdata, 1);
	MY_END_ALLOW_THREADS(con->tstate);
	aggregate_instance =
		(PyObject**)sqlite3_aggregate_context(context,
						      sizeof(PyObject*));

	if (*aggregate_instance == 0) {
		args = PyTuple_New(0);
		*aggregate_instance = PyObject_CallObject(aggregate_class, args);
		Py_DECREF(args);

		if (PyErr_Occurred()) {
			PRINT_OR_CLEAR_ERROR;
			MY_BEGIN_ALLOW_THREADS(con->tstate);
			return;
		}
	}

	stepmethod = PyObject_GetAttrString(*aggregate_instance, "step");
	if (!stepmethod) {
		/* PRINT_OR_CLEAR_ERROR */
		MY_BEGIN_ALLOW_THREADS(con->tstate);
		return;
	}

	args = PyTuple_New(argc);
	for (i = 0; i < argc; i++) {
		const char *s = sqlite3_value_text(argv[i]);
		if (s == NULL) {
			Py_INCREF(Py_None);
			PyTuple_SetItem(args, i, Py_None);
		}
		else {
			PyTuple_SetItem(args, i, PyString_FromString(s));
		}
	}

	if (PyErr_Occurred()) {
		PRINT_OR_CLEAR_ERROR;
	}

	function_result = PyObject_CallObject(stepmethod, args);
	Py_DECREF(args);
	Py_DECREF(stepmethod);

	if (function_result == NULL) {
		PRINT_OR_CLEAR_ERROR;
		/* Don't use sqlite_set_result_error here. Else an assertion in
		 * the SQLite code will trigger and create a core dump.
		 */
	}
	else {
		Py_DECREF(function_result);
	}

	MY_BEGIN_ALLOW_THREADS(con->tstate);
}

static void
aggregate_finalize(sqlite3_context *context)
{
	PyObject* args;
	PyObject* function_result;
	PyObject** aggregate_instance;
	PyObject* userdata;
	pysqlc* con;
	PyObject* aggregate_class;
	PyObject* finalizemethod;

	userdata = (PyObject*)sqlite3_user_data(context);
	aggregate_class = PyTuple_GetItem(userdata, 0);
	con = (pysqlc*)PyTuple_GetItem(userdata, 1);
	MY_END_ALLOW_THREADS(con->tstate);

	aggregate_instance =
		(PyObject**)sqlite3_aggregate_context(context,
						      sizeof(PyObject*));

	finalizemethod = PyObject_GetAttrString(*aggregate_instance,
						"finalize");

	if (!finalizemethod) {
		PyErr_SetString(PyExc_ValueError, "finalize method missing");
		goto error;
	}

	args = PyTuple_New(0);
	function_result = PyObject_CallObject(finalizemethod, args);
	Py_DECREF(args);
	Py_DECREF(finalizemethod);

	if (PyErr_Occurred()) {
		PRINT_OR_CLEAR_ERROR;
		sqlite3_result_error(context, NULL, -1);
	}
	else {
		set_result(context, function_result);
	}
	Py_XDECREF(function_result);
 error:
	Py_XDECREF(*aggregate_instance);
	MY_BEGIN_ALLOW_THREADS(con->tstate);
}

static int
sqlite_busy_handler_callback(void* void_data, int num_busy)
{
	PyObject* data;
	PyObject* func;
	PyObject* userdata;
	PyObject* args;
	PyObject* function_result;
	pysqlc* con;
	int result_int;

	data = (PyObject*)void_data;

	func = PyTuple_GetItem(data, 0);
	userdata = PyTuple_GetItem(data, 1);
	con = (pysqlc*)PyTuple_GetItem(data, 2);

	MY_END_ALLOW_THREADS(con->tstate);

	args = PyTuple_New(2);
	Py_INCREF(userdata);
	PyTuple_SetItem(args, 0, userdata);
	PyTuple_SetItem(args, 1, PyInt_FromLong((long)num_busy));

	function_result = PyObject_CallObject(func, args);
	Py_DECREF(args);

	if (PyErr_Occurred()) {
		PRINT_OR_CLEAR_ERROR;
		MY_BEGIN_ALLOW_THREADS(con->tstate);
		return 0;
	}

	result_int = PyObject_IsTrue(function_result);
	Py_DECREF(function_result);
	MY_BEGIN_ALLOW_THREADS(con->tstate);
	return result_int;
}

static char _con_sqlite_busy_handler_doc[] =
"sqlite_busy_handler(func, data)\n\
Register a busy handler.\n\
\n\
    The sqlite_busy_handler() procedure can be used to register a busy\n\
    callback with an open SQLite database. The busy callback will be invoked\n\
    whenever SQLite tries to access a database that is locked. The callback\n\
    will typically do some other useful work, or perhaps sleep, in order to\n\
    give the lock a chance to clear. If the callback returns non-zero, then\n\
    SQLite tries again to access the database and the cycle repeats. If the\n\
    callback returns zero, then SQLite aborts the current operation and \n\
    returns SQLITE_BUSY, which PySQLite will make throw an OperationalError.\n\
    \n\
    The arguments to sqlite_busy_handler() are the callback function (func)\n\
    and an additional argument (data) that will be passed to the busy\n\
    callback function.\n\
    \n\
    When the busy callback is invoked, it is sent two arguments. The first\n\
    argument will be the 'data' that was set as the third argument to\n\
    sqlite_busy_handler. The second will be the\n\
    number of times that the library has attempted to access the database\n\
    table or index.";

static PyObject*
_con_sqlite_busy_handler(pysqlc* self, PyObject *args, PyObject* kwargs)
{
	static char *kwlist[] = {"func", "data", NULL};
	PyObject* func;
	PyObject* data = Py_None;
	PyObject* userdata;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs,
					 "O|O:sqlite_busy_handler", kwlist,
					 &func, &data)) {
		return NULL;
	}

	/* dereference old callback (if any) */
	if (self->busy_data != NULL) {
		Py_DECREF(self->busy_data);
	}

	if ((userdata = PyTuple_New(3)) == NULL)
		return NULL;

	Py_INCREF(func);
	PyTuple_SetItem(userdata, 0, func);
	Py_INCREF(data);
	PyTuple_SetItem(userdata, 1, data);
	Py_INCREF(self);
	PyTuple_SetItem(userdata, 2, (PyObject*)self);

	sqlite3_busy_handler(self->p_db, &sqlite_busy_handler_callback,
			     userdata);

	self->busy_data = userdata;

	Py_INCREF(Py_None);
	return Py_None;
}

static char _con_sqlite_busy_timeout_doc[] =
"sqlite_busy_timeout(milliseconds)\n\
Register a busy handler that will wait for a specific time before giving up.\n\
\n\
    This is a convenience routine that will install a busy handler (see\n\
    sqlite_busy_handler) that will sleep for n milliseconds before\n\
    giving up (i. e. return SQLITE_BUSY/throw OperationalError).";

static PyObject*
_con_sqlite_busy_timeout(pysqlc* self, PyObject *args, PyObject* kwargs)
{
	int timeout;
	static char *kwlist[] = {"timeout", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i:sqlite_busy_timeout",
					 kwlist, &timeout)) {
		return NULL;
	}

	self->timeout = timeout;
	sqlite3_busy_timeout(self->p_db, timeout);

	Py_INCREF(Py_None);
	return Py_None;
}

static char _con_create_function_doc[] =
"create_function(name, n_args, func)\n\
Create a new SQL function.\n\
\n\
    A new function under the name 'name', with 'n_args' arguments is \
created.\n\
    The callback 'func' will be called for this function.";

static PyObject*
_con_create_function(pysqlc* self, PyObject *args, PyObject* kwargs)
{
	int n_args;
	char* name;
	PyObject* func;
	PyObject* userdata;
	static char *kwlist[] = {"name", "n_args", "func", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "siO:create_function",
					 kwlist, &name, &n_args,
					 &func)) {
		return NULL;
	}

	if (!(userdata = PyTuple_New(2)))
		return NULL;
	Py_INCREF(func);
	PyTuple_SetItem(userdata, 0, func);
	Py_INCREF(self);
	PyTuple_SetItem(userdata, 1, (PyObject*)self);

	if (!PyCallable_Check(func)) {
		PyErr_SetString(PyExc_ValueError, "func must be a callable!");
		return NULL;
	}

	Py_INCREF(func);
	if (0 != sqlite3_create_function(self->p_db, name, n_args, SQLITE_UTF8,
					 (void *) userdata,
					 &function_callback, NULL, NULL)) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Cannot create function.");
		return NULL;
	}
	else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

static char _con_create_aggregate_doc[] =
"create_aggregate(name, n_args, step_func, finalize_func)\n\
Create a new SQL function.\n\
\n\
    A new aggregate function under the name 'name', with 'n_args' arguments\n\
    to the 'step_func' function will be created. 'finalize_func' will be\n\
    called without arguments for finishing the aggregate.";

static PyObject*
_con_create_aggregate(pysqlc* self, PyObject *args, PyObject* kwargs)
{
	PyObject* aggregate_class;

	int n_args;
	char* name;
	static char *kwlist[] = { "name", "n_args", "aggregate_class", NULL };
	PyObject* userdata;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "siO:create_aggregate",
					 kwlist, &name, &n_args,
					 &aggregate_class)) {
		return NULL;
	}

	if (!(userdata = PyTuple_New(2)))
		return NULL;

	Py_INCREF(aggregate_class);
	PyTuple_SetItem(userdata, 0, aggregate_class);
	Py_INCREF(self);
	PyTuple_SetItem(userdata, 1, (PyObject*)self);

	if (0 != sqlite3_create_function(self->p_db, name, n_args, SQLITE_UTF8,
					 (void*)userdata, NULL,
					 &aggregate_step,
					 &aggregate_finalize)) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Cannot create aggregate.");
		return NULL;
	}
	else {
		Py_INCREF(Py_None);
		return Py_None;
	}
}

static char _con_set_command_logfile_doc[] =
"set_command_logfile(logfile)\n\
Registers a writeable file-like object as logfile where all SQL commands\n\
that get executed are written to.";

static PyObject*
_con_set_command_logfile(pysqlc* self, PyObject *args,
			 PyObject* kwargs)
{
	PyObject* logfile;
	PyObject* o;

	static char *kwlist[] = { "logfile", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O:set_command_logfile",
					 kwlist, &logfile))
		return NULL;

	/* if it's None, we can skip the .write() method checks */
	if (logfile != Py_None) {
		o = PyObject_GetAttrString(logfile, "write");
		if (!o) {
			PyErr_SetString(PyExc_ValueError,
				  "logfile must have a 'write' attribute!");
			return NULL;
		}

		if (!PyCallable_Check(o)) {
			PyErr_SetString(PyExc_ValueError,
			    "logfile must have a callable 'write' attribute!");
			Py_DECREF(o);
			return NULL;
		}
		Py_DECREF(o);
	}

	/* looks good, use it for the logfile */
	Py_INCREF(logfile);
	self->command_logfile = logfile;

	Py_INCREF(Py_None);
	return Py_None;
}

int sqlite_exec_callback(void* pArg, int argc, char **argv, char **columnNames)
{
	PyObject* parg;
	PyObject* callback;
	PyObject* arg1;
	pysqlc* con;
	PyObject* values;
	PyObject* colnames;
	PyObject* calling_args;
	PyObject* function_result;
	int i;

	parg = (PyObject*)pArg;

	callback = PyTuple_GetItem(parg, 0);
	arg1 = PyTuple_GetItem(parg, 1);
	con = (pysqlc*)PyTuple_GetItem(parg, 2);

	MY_END_ALLOW_THREADS(con->tstate)

		colnames = PyTuple_New(argc);
	for (i = 0; i < argc; i++) {
		PyTuple_SetItem(colnames, i,
				PyString_FromString(columnNames[i]));
	}

	values = PyTuple_New(argc);
	for (i = 0; i < argc; i++) {
		if (argv[i] == NULL) {
			Py_INCREF(Py_None);
			PyTuple_SetItem(values, i, Py_None);
		}
		else
			PyTuple_SetItem(values, i,
					PyString_FromString(argv[i]));
	}

	calling_args = PyTuple_New(3);
	Py_INCREF(arg1);
	PyTuple_SetItem(calling_args, 0, arg1);
	PyTuple_SetItem(calling_args, 1, values);
	PyTuple_SetItem(calling_args, 2, colnames);

	function_result = PyObject_CallObject(callback, calling_args);
	if (PyErr_Occurred()) {
		PRINT_OR_CLEAR_ERROR
			MY_BEGIN_ALLOW_THREADS(con->tstate)
			return 1;
	}

	Py_DECREF(function_result);
	Py_DECREF(calling_args);

	MY_BEGIN_ALLOW_THREADS(con->tstate)
		return 0;
}

static PyObject*
_con_sqlite_last_insert_rowid(pysqlc *self)
{
	return PyInt_FromLong((long)sqlite3_last_insert_rowid(self->p_db));
}

static PyObject*
_con_sqlite_changes(pysqlc *self)
{
	return PyInt_FromLong((long)sqlite3_changes(self->p_db));
}

static PyObject*
sqlite_library_version(PyObject *self)
{
	return Py_BuildValue("s", sqlite3_libversion());
}

static PyObject*
sqlite_enable_callback_debugging(PyObject *self, PyObject *args)
{
	if (!PyArg_ParseTuple(args, "i", &debug_callbacks)) {
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

static char pysqlite_encode_doc[] =
"encode(s) -> encoded binary string.\n\
Encode binary string 's' for storage in SQLite.";

static PyObject*
pysqlite_encode(PyObject *self, PyObject *args)
{
	char *in, *out;
	int n;
	PyObject *res;

	if (!PyArg_ParseTuple(args, "s#", &in, &n)) {
		return NULL;
	}

	/* See comments in encode.c for details on maximum size of encoded */
	/* data. */
	out = malloc(2 + (257*n)/254);
	if (out == NULL) {
		return PyErr_NoMemory();
	}
	sqlite_encode_binary(in, n, out);
	res = Py_BuildValue("s", out);
	free(out);
	return res;
}

static char pysqlite_decode_doc[] =
"decode(s) -> decoded binary string.\n\
Decode encoded binary string retrieved from SQLite.";

static PyObject*
pysqlite_decode(PyObject *self, PyObject *args)
{
	char *in, *out;
	int n;
	PyObject *res;

	if (!PyArg_ParseTuple(args, "s", &in)) {
		return NULL;
	}

	/* Decoded string is always shorter than encoded string. */
	out = malloc(strlen(in));
	if (out == NULL) {
		return PyErr_NoMemory();
	}
	n = sqlite_decode_binary(in, out);
	res = Py_BuildValue("s#", out, n);
	free(out);
	return res;
}

static PyObject*
_con_execute(pysqlc* self, PyObject *args)
{
	int ret;
	char *sql;
	char *errmsg = NULL;

	if(!PyArg_ParseTuple(args,"s:execute", &sql)) {
		return NULL;
	}

	ret = sqlite3_exec(self->p_db, sql, NULL, NULL, &errmsg);

	if (ret != SQLITE_OK) {
		if (errmsg != NULL) {
			PyErr_SetString(_sqlite_DatabaseError, errmsg);
			free(errmsg);
		}
		else {
			PyErr_SetString(_sqlite_DatabaseError,
					sqlite3_errmsg(self->p_db));
		}
		return NULL;
	}
	Py_INCREF(Py_None);
	return Py_None;
}


static PyObject*
_con_prepare(pysqlc* self, PyObject *args)
{
	int ret;
	int record_number;
	int sql_len;
	char* sql;
	pysqlstmt* pystmt;
	sqlite3_stmt *stmt;
	const char* query_tail;
	PyObject* logfile_writemethod;
	PyObject* logfile_writeargs;

	record_number = 0;

	if(!PyArg_ParseTuple(args,"s#:prepare", &sql, &sql_len)) {
		return NULL;
	}

	if(self->p_db == 0) {
		/* There is no open database. */
		PyErr_SetString(_sqlite_ProgrammingError,
				"There is no open database.");
		return NULL;
	}

	/* Log SQL statement */
	if (self->command_logfile != Py_None) {
		logfile_writemethod =
			PyObject_GetAttrString(self->command_logfile, "write");

		logfile_writeargs = PyTuple_New(1);
		PyTuple_SetItem(logfile_writeargs, 0,
				PyString_FromString(sql));
		PyObject_CallObject(logfile_writemethod, logfile_writeargs);
		PyTuple_SetItem(logfile_writeargs, 0,
				PyString_FromString(";\n"));
		PyObject_CallObject(logfile_writemethod, logfile_writeargs);

		Py_DECREF(logfile_writeargs);
		Py_DECREF(logfile_writemethod);

		if (PyErr_Occurred()) {
			return NULL;
		}
	}

	ret = sqlite3_prepare(self->p_db, sql, sql_len, &stmt, &query_tail);

	if (ret != SQLITE_OK) {
		PyErr_SetString(_sqlite_DatabaseError,
				sqlite3_errmsg(self->p_db));
		return NULL;
	}

	if (stmt == NULL) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"SQL contained no statement.");
		return NULL;
	}

	if (*query_tail != '\0') {
		sqlite3_finalize(stmt);
		PyErr_SetString(_sqlite_ProgrammingError,
				"SQL must only contain one statement.");
		return NULL;
	}

	pystmt = PyObject_New(pysqlstmt, &pysqlstmt_Type);
	if (pystmt == NULL) {
		sqlite3_finalize(stmt);
		return NULL;
	}

	Py_INCREF(self);
	pystmt->con = self;
	pystmt->p_stmt = stmt;
	pystmt->description = NULL;
	pystmt->num_fields = sqlite3_column_count(stmt);
	pystmt->reset = 1;

	return (PyObject*)pystmt;
}

static PyObject*
sqlite_version_info(PyObject* self)
{
	PyObject* vi_list;
	PyObject* vi_tuple;
	char* buf;
	char* iterator;
	char* token;

	buf = strdup(sqlite3_libversion());
	iterator = buf;

	vi_list = PyList_New(0);

	while ((token = pysqlite_strsep(&iterator, ".")) != NULL) {
		PyList_Append(vi_list, PyInt_FromLong((long)atoi(token)));
	}

	vi_tuple = PyList_AsTuple(vi_list);
	Py_DECREF(vi_list);
	free(buf);

	return vi_tuple;
}

/*----------------------------------------------------------------------------
** Statement Object Implementation
**----------------------------------------------------------------------------
*/

#if defined(__i386__) || defined(__x86_64__)
# define breakpoint do {__asm__ __volatile__ ("int $03");} while (0)
#endif

static char _stmt_step_doc [] =
"step()\n\
Fetch the next row from a the statement.";

static PyObject*
_stmt_step(pysqlstmt *self, PyObject *args)
{
	int result;
	int i;
	PyObject *row;

	if(self->p_stmt == NULL) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Statement has already been finalized.");
		return NULL;
	}

	MY_BEGIN_ALLOW_THREADS(self->con->tstate);
	result = sqlite3_step(self->p_stmt);
	MY_END_ALLOW_THREADS(self->con->tstate);

	if (self->description == NULL)
		self->description = _stmt_get_description(self);

	if (result == SQLITE_ROW) {
		long long int lval;
		double dval;
		int len;
		const void *blob;
		const char *text;

		if (self->reset) {
			self->reset = 0;
		}
		row = PyTuple_New(self->num_fields);
		for(i=0; i < self->num_fields; i++) {
			PyObject *type_code, *row_descr;
			int row_type;

			/* handle the NULL case early */
			text = sqlite3_column_text(self->p_stmt, i);
			if (text == NULL) {
				Py_INCREF(Py_None);
				PyTuple_SetItem(row, i, Py_None);
				continue;
			}

			/* look up the type code */
			row_descr = PyTuple_GetItem(self->description, i);
			type_code = PyTuple_GetItem(row_descr, 1);

			row_type = PyInt_AsLong(type_code);
			switch(row_type) {
			case INTEGER:
				lval = sqlite3_column_int64(self->p_stmt, i);
				if (lval > INT_MAX)
					PyTuple_SetItem(row, i,
						PyLong_FromLongLong(lval));
				else
					PyTuple_SetItem(row, i,
							PyInt_FromLong(lval));
				break;
			case BOOLEAN:
				len = sqlite3_column_int(self->p_stmt, i);
				PyTuple_SetItem(row, i,
						PyBool_FromLong(len));
				break;
			case FLOAT:
				dval = sqlite3_column_double(self->p_stmt, i);
				PyTuple_SetItem(row, i,
						PyFloat_FromDouble(dval));
				break;
			case BINARY:
				len = sqlite3_column_bytes(self->p_stmt, i);
				blob = sqlite3_column_blob(self->p_stmt, i);
				PyTuple_SetItem(row, i,
					PyString_FromStringAndSize(blob, len));
				break;
			case UNICODESTRING:
				blob = sqlite3_column_text16(self->p_stmt, i);
				len = sqlite3_column_bytes16(self->p_stmt, i) / 4;
				PyTuple_SetItem(row, i,
					PyUnicode_FromUnicode(blob, len));
				break;
			case NULLVALUE:
				Py_INCREF(Py_None);
				PyTuple_SetItem(row, i, Py_None);
				break;
			default:
				/* handle everything else as a string */
				len = sqlite3_column_bytes(self->p_stmt, i);
				PyTuple_SetItem(row, i,
					PyString_FromStringAndSize(text, len));
				break;
			}
		}
		return row;
	}
	else if (result == SQLITE_DONE) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	else if (result == SQLITE_ERROR) {
		/* a run-time error has occurred.  We need to
		   reset the statement in order to get a useful
		   error message */
		result = sqlite3_reset(self->p_stmt);
		PyErr_SetString(_sqlite_ProgrammingError,
				sqlite3_errmsg(self->con->p_db));
		return NULL;
	}
	else {
		/* this statement is bad, better not touch it anymore,
		 just return what we have */
		PyErr_SetString(_sqlite_InternalError,
				sqlite3_errmsg(self->con->p_db));
		return NULL;
	}
}

static char _stmt_reset_doc [] =
"reset()\n\
Reset the virtual machine associated with a stmtiled SQL statement.";

static PyObject*
_stmt_reset(pysqlstmt *self)
{
	int result, i;

	if(self->p_stmt == NULL) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Statement has already been finalized.");
		return NULL;
	}

	for (i=0; i<2; i++) {
		result = sqlite3_reset(self->p_stmt);
		if (result != SQLITE_OK) {
			const char *msg = sqlite3_errmsg(self->con->p_db);
			/* if the message is "use function error", */
			/* retry the reset */
			if (!strcmp(msg, "user function error")) {
				continue;
			}
			PyErr_SetString(_sqlite_DatabaseError, msg);
			return NULL;
		}
	}

	Py_INCREF(Py_None);
	return Py_None;
}

static char _stmt_finalize_doc [] =
"finalize()\n\
Frees the virtual machine associated with a stmtiled SQL statement.";

static PyObject*
_stmt_finalize(pysqlstmt *self)
{
	int result;

	if(self->p_stmt == NULL) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Statement has already been finalized.");
		return NULL;
	}

	if (self->p_stmt != NULL) {
		result = sqlite3_finalize(self->p_stmt);
		if (result != SQLITE_OK) {
			PyErr_SetString(_sqlite_DatabaseError,
					sqlite3_errmsg(self->con->p_db));
			return NULL;
		}
		self->p_stmt = NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

static char _stmt_bind_doc [] =
"bind()\n\
Bind arguments to a SQL statement.";

static PyObject*
_stmt_bind(pysqlstmt *self, PyObject *args)
{
	PyObject *obj;
	PyObject *idobj;
	int idx, rc;
	char *bName = NULL;
	int bLen = 0;
	
	if (!PyArg_ParseTuple(args, "OO", &idobj, &obj)) {
		return NULL;
	}

	if(self->p_stmt == NULL) {
		PyErr_SetString(_sqlite_ProgrammingError,
				"Statement has already been finalized.");
		return NULL;
	}
	/* if we're using named bind arguments, extract the idx */
	if (PyString_Check(idobj)) {
		PyString_AsStringAndSize(idobj, &bName, &bLen);
		/* XXX: some implementations are more permissive when asked to
		   bind parameters that are not present in the query */
		if (sqlite3_bind_parameter_count(self->p_stmt) == 0) {
			PyErr_SetString(_sqlite_ProgrammingError,
					"Statement does not use named bind parameters.");
			return NULL;
		}
		idx = sqlite3_bind_parameter_index(self->p_stmt, bName);
		/* FIXME: this detects errors faster - but normally we
		   should be ok with looking at a hash and only
		   picking the stuff we need in the query */
		if (idx == 0) {
			PyErr_SetString(_sqlite_ProgrammingError,
					"Bind parameter name unknown to the query");
			return NULL;
		}
	}
	else if (PyInt_Check(idobj))
		idx = (int) PyInt_AsLong(idobj);
	else {
		PyErr_SetString(PyExc_TypeError,
				"First bind argument must be int or string");
		return NULL;
	}		
	
	rc = -1;
	/* bools are derived from int, no need to handle it explicitly */
	if (PyInt_Check(obj)) {
		rc = sqlite3_bind_int(self->p_stmt, idx, PyInt_AsLong(obj));
	}
	else if (PyLong_Check(obj)) {
		rc = sqlite3_bind_int64(self->p_stmt, idx,
					PyLong_AsLongLong(obj));
	}
	else if (PyString_Check(obj)) {
		char *buf;
		int len;
		PyString_AsStringAndSize(obj, &buf, &len);
		if (memchr(buf, '\0', len))
			rc = sqlite3_bind_blob(self->p_stmt, idx, buf, len,
					       SQLITE_TRANSIENT);
		else
			rc = sqlite3_bind_text(self->p_stmt, idx, buf, len,
					       SQLITE_TRANSIENT);
	}
	else if (PyUnicode_Check(obj)) {
		int len;
		Py_UNICODE *buf = PyUnicode_AS_UNICODE(obj);
		len = PyUnicode_GET_DATA_SIZE(obj);
		rc = sqlite3_bind_text16(self->p_stmt, idx, (void *) buf, len,
					 SQLITE_TRANSIENT);
	}
	else if (PyFloat_Check(obj)) {
		rc = sqlite3_bind_double(self->p_stmt, idx,
					 PyFloat_AsDouble(obj));
	}
	else if (obj == Py_None) {
		rc = sqlite3_bind_null(self->p_stmt, idx);
	}
	else {
		PyObject *o = NULL;
		if (PyObject_HasAttrString(obj, "__quote__"))
			o = PyObject_GetAttrString(obj, "__quote__");
		else if (PyObject_HasAttrString(obj, "_quote"))
			o = PyObject_GetAttrString(obj, "_quote");

		if (o != NULL && PyCallable_Check(o)) {
			char *buf;
			int len;
			PyObject *str = PyObject_CallObject(o, NULL);

			PyString_AsStringAndSize(str, &buf, &len);
			rc = sqlite3_bind_text(self->p_stmt, idx, buf, len,
					       SQLITE_TRANSIENT);
			Py_DECREF(str);
		}
	}

	if (rc != SQLITE_OK) {
		if (rc == -1)
			PyErr_SetString(_sqlite_ProgrammingError,
					"unknown type, unable to bind");
		else
			PyErr_SetString(_sqlite_DatabaseError,
					sqlite3_errmsg(self->con->p_db));
		return NULL;
	}

	Py_INCREF(Py_None);
	return Py_None;
}

static char _stmt_get_description_doc [] =
"get_description()\n\
Returns the current definition.";
static PyObject*
_stmt_get_description(pysqlstmt* self)
{
	int num_fields, i, l, j;
	PyObject *obj, *col, *type_code;
	char type_name[255];

	num_fields = sqlite3_column_count(self->p_stmt);
	if ((obj = PyTuple_New(num_fields)) == NULL) {
		return NULL;
	}

	for(i=0; i < num_fields; i++){
		const char *name = sqlite3_column_name(self->p_stmt, i);
		const char *ctype = sqlite3_column_decltype(self->p_stmt, i);
		if (ctype == NULL)
			ctype = ctype_to_str(sqlite3_column_type(self->p_stmt,
								 i));
		if (ctype == NULL)
			ctype = "NULL";

		col = PyTuple_New(7);

		/* 1. Column Name */
		PyTuple_SetItem(col, 0, Py_BuildValue("s", name));

		/* 2. Type code */
		/* Make a copy of column type. */
		if (ctype == NULL) {
			strcpy(type_name, "TEXT");
		}
		else {
			strncpy(type_name, ctype, sizeof(type_name) - 1);
		}

		/* Get its length. */
		l = strlen(type_name);

		/* Convert to uppercase. */
		for(j=0; j < l; j++) {
			type_name[j] = toupper(type_name[j]);
		}

		/* Init/unset value */
		type_code = NULL;
		/* Try to determine column type. */
		if (strstr(type_name, "INTERVAL"))
			type_code = tc_INTERVAL;
		else if (strstr(type_name, "INT"))
			type_code = tc_INTEGER;
		else if (strstr(type_name, "BOOL"))
			type_code = tc_BOOLEAN;
		else if (strstr(type_name, "CHAR") ||
			 strstr(type_name, "TEXT") ||
			 strstr(type_name, "STR"))
			type_code = tc_STRING;
		else if (strstr(type_name, "UNICODE"))
			type_code = tc_UNICODESTRING;
		else if (strstr(type_name, "BIN") ||
			 strstr(type_name, "BLOB"))
			type_code = tc_BINARY;
		else if (strstr(type_name, "FLOAT") ||
			 strstr(type_name, "NUMERIC") ||
			 strstr(type_name, "NUMBER") ||
			 strstr(type_name, "DECIMAL") ||
			 strstr(type_name, "REAL") ||
			 strstr(type_name, "DOUBLE"))
			type_code = tc_FLOAT;
		else if (strstr(type_name, "TIMESTAMP"))
			type_code = tc_TIMESTAMP;
		else if (strstr(type_name, "DATE"))
			type_code = tc_DATE;
		else if (strstr(type_name, "TIME"))
			type_code = tc_TIME;
		else if (type_code == NULL)
			type_code = tc_NULL;

		/* Assign type. */
		Py_INCREF(type_code);
		PyTuple_SetItem(col, 1, type_code);

		/* 3. Display Size */
		Py_INCREF(Py_None);
		PyTuple_SetItem(col, 2, Py_None);

		/* 4. Internal Size */
		Py_INCREF(Py_None);
		PyTuple_SetItem(col, 3, Py_None);

		/* 5. Precision */
		Py_INCREF(Py_None);
		PyTuple_SetItem(col, 4, Py_None);

		/* 6. Scale */
		Py_INCREF(Py_None);
		PyTuple_SetItem(col, 5, Py_None);

		/* 7. NULL Okay */
		Py_INCREF(Py_None);
		PyTuple_SetItem(col, 6, Py_None);

		PyTuple_SetItem(obj, i, col);
	}

	return obj;
}

static PyMethodDef _stmt_methods[] = {
	{ "bind",
	 (PyCFunction) _stmt_bind, METH_VARARGS, _stmt_bind_doc },
	{ "finalize",
	 (PyCFunction) _stmt_finalize, METH_NOARGS, _stmt_finalize_doc },
	{ "reset",
	 (PyCFunction) _stmt_reset, METH_NOARGS, _stmt_reset_doc },
	{ "step",
	 (PyCFunction) _stmt_step, METH_NOARGS, _stmt_step_doc },
	{ "get_description",
	 (PyCFunction) _stmt_get_description, METH_NOARGS,
	 _stmt_get_description_doc },
	{ NULL, NULL }
};

static void
_stmt_dealloc(pysqlstmt* self)
{
	int result;

	if(self->description != 0) {
		Py_DECREF(self->description);
		self->description = 0;
	}

	if (self->p_stmt != NULL) {
		result = sqlite3_finalize(self->p_stmt);
		if (result == SQLITE_MISUSE) {
			PyObject *err_type, *err_value, *err_traceback;
			int have_error = PyErr_Occurred() ? 1 : 0;

			if (have_error)
				PyErr_Fetch(&err_type, &err_value,
					    &err_traceback);
			PyErr_SetString(_sqlite_DatabaseError,
					sqlite3_errmsg(self->con->p_db));
			PyErr_WriteUnraisable(
			     PyString_FromString("_sqlite.Statement.__del__"));
			if (have_error)
				PyErr_Restore(err_type, err_value,
					      err_traceback);
		}
	}
	Py_DECREF(self->con);
	PyObject_Del(self);
}

static PyObject*
_stmt_get_attr(pysqlstmt *self, char *attr)
{
	return Py_FindMethod(_stmt_methods, (PyObject *) self, attr);
}

/*----------------------------------------------------------------------------
** Module Definitions / Initialization
**----------------------------------------------------------------------------
*/
static PyMethodDef pysqlite_functions[] = {
	{ "connect",
	  (PyCFunction)pysqlite_connect,
	  METH_VARARGS | METH_KEYWORDS, pysqlite_connect_doc},
	{ "sqlite_version",
	  (PyCFunction)sqlite_library_version, METH_NOARGS},
	{ "sqlite_version_info",
	  (PyCFunction)sqlite_version_info, METH_NOARGS},
	{ "enable_callback_debugging",
	  (PyCFunction)sqlite_enable_callback_debugging, METH_VARARGS},
	{ "encode",
	  (PyCFunction)pysqlite_encode, METH_VARARGS, pysqlite_encode_doc},
	{ "decode",
	  (PyCFunction)pysqlite_decode, METH_VARARGS, pysqlite_decode_doc},
	{ NULL, NULL }
};

/*----------------------------------------------------------------------------
** Connection Object Implementation
**----------------------------------------------------------------------------
*/

static struct memberlist _con_memberlist[] = {
	{ "sql",             T_STRING, offsetof(pysqlc, sql), RO },
	{ "filename",        T_STRING, offsetof(pysqlc, database_name), RO },
	{ NULL }
};

static PyMethodDef _con_methods[] = {
	{ "close",
	  (PyCFunction) _con_close, METH_NOARGS, _con_close_doc },
	{ "prepare",
	  (PyCFunction)_con_prepare, METH_VARARGS },
	{ "execute",
	  (PyCFunction)_con_execute, METH_VARARGS },
	{ "set_command_logfile",
	  (PyCFunction)_con_set_command_logfile, METH_VARARGS | METH_KEYWORDS,
	  _con_set_command_logfile_doc },
	{ "create_function",
	  (PyCFunction)_con_create_function, METH_VARARGS | METH_KEYWORDS,
	  _con_create_function_doc },
	{ "create_aggregate",
	  (PyCFunction)_con_create_aggregate, METH_VARARGS | METH_KEYWORDS,
	  _con_create_aggregate_doc },
	{ "sqlite_last_insert_rowid",
	  (PyCFunction)_con_sqlite_last_insert_rowid, METH_NOARGS },
	{ "sqlite_changes",
	  (PyCFunction)_con_sqlite_changes, METH_NOARGS },
	{ "sqlite_busy_handler",
	  (PyCFunction)_con_sqlite_busy_handler, METH_VARARGS | METH_KEYWORDS,
	  _con_sqlite_busy_handler_doc },
	{ "sqlite_busy_timeout",
	  (PyCFunction)_con_sqlite_busy_timeout, METH_VARARGS | METH_KEYWORDS,
	  _con_sqlite_busy_timeout_doc },
	{ NULL, NULL }
};

#define REGISTER_TYPE(name) \
    if (PyType_Ready(&name ## _Type) < 0) \
        return; \
    Py_INCREF(&name ## _Type); \
    PyModule_AddObject(module, #name, (PyObject *) &name ## _Type);

PySQLite_MODINIT_FUNC(init_sqlite3)
{
	PyObject *module, *dict;

	module = Py_InitModule("_sqlite3", pysqlite_functions);

	if (!(dict = PyModule_GetDict(module))) {
		goto error;
	}

	REGISTER_TYPE(pysqlc);
	REGISTER_TYPE(pysqlstmt);

	/*** Initialize type codes */
	tc_INTEGER = PyInt_FromLong(INTEGER);
	tc_FLOAT = PyInt_FromLong(FLOAT);
	tc_TIMESTAMP = PyInt_FromLong(TIMESTAMP);
	tc_DATE = PyInt_FromLong(DATE);
	tc_TIME = PyInt_FromLong(TIME);
	tc_INTERVAL = PyInt_FromLong(INTERVAL);
	tc_STRING = PyInt_FromLong(STRING);
	tc_UNICODESTRING = PyInt_FromLong(UNICODESTRING);
	tc_BINARY = PyInt_FromLong(BINARY);
	tc_BOOLEAN = PyInt_FromLong(BOOLEAN);
	tc_NULL = PyInt_FromLong(NULLVALUE);

	PyDict_SetItemString(dict, "INTEGER", tc_INTEGER);
	PyDict_SetItemString(dict, "FLOAT", tc_FLOAT);
	PyDict_SetItemString(dict, "TIMESTAMP", tc_TIMESTAMP);
	PyDict_SetItemString(dict, "DATE", tc_DATE);
	PyDict_SetItemString(dict, "TIME", tc_TIME);
	PyDict_SetItemString(dict, "INTERVAL", tc_INTERVAL);
	PyDict_SetItemString(dict, "STRING", tc_STRING);
	PyDict_SetItemString(dict, "UNICODESTRING", tc_UNICODESTRING);
	PyDict_SetItemString(dict, "BINARY", tc_BINARY);
	PyDict_SetItemString(dict, "BOOLEAN", tc_BOOLEAN);
	PyDict_SetItemString(dict, "NULL", tc_NULL);

	/*** Create DB-API Exception hierarchy */
	_sqlite_Error = PyErr_NewException("_sqlite.Error",
					   PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Error", _sqlite_Error);

	_sqlite_Warning = PyErr_NewException("_sqlite.Warning",
					     PyExc_StandardError, NULL);
	PyDict_SetItemString(dict, "Warning", _sqlite_Warning);

	/* Error subclasses */
	_sqlite_InterfaceError = PyErr_NewException("_sqlite.InterfaceError",
						    _sqlite_Error, NULL);
	PyDict_SetItemString(dict, "InterfaceError", _sqlite_InterfaceError);

	_sqlite_DatabaseError = PyErr_NewException("_sqlite.DatabaseError",
						   _sqlite_Error, NULL);
	PyDict_SetItemString(dict, "DatabaseError", _sqlite_DatabaseError);

	/* DatabaseError subclasses */
	_sqlite_InternalError =
		PyErr_NewException("_sqlite.InternalError",
				   _sqlite_DatabaseError, NULL);
	PyDict_SetItemString(dict, "InternalError", _sqlite_InternalError);

	_sqlite_OperationalError =
		PyErr_NewException("_sqlite.OperationalError",
				   _sqlite_DatabaseError, NULL);
	PyDict_SetItemString(dict, "OperationalError",
			     _sqlite_OperationalError);

	_sqlite_ProgrammingError =
		PyErr_NewException("_sqlite.ProgrammingError",
				   _sqlite_DatabaseError, NULL);
	PyDict_SetItemString(dict, "ProgrammingError",
			     _sqlite_ProgrammingError);

	_sqlite_IntegrityError =
		PyErr_NewException("_sqlite.IntegrityError",
				   _sqlite_DatabaseError,NULL);
	PyDict_SetItemString(dict, "IntegrityError", _sqlite_IntegrityError);

	_sqlite_DataError = PyErr_NewException("_sqlite.DataError",
					       _sqlite_DatabaseError, NULL);
	PyDict_SetItemString(dict, "DataError", _sqlite_DataError);

	_sqlite_NotSupportedError =
		PyErr_NewException("_sqlite.NotSupportedError",
				   _sqlite_DatabaseError, NULL);
	PyDict_SetItemString(dict, "NotSupportedError",
			     _sqlite_NotSupportedError);

 error:
	if (PyErr_Occurred()) {
		PyErr_SetString(PyExc_ImportError, "sqlite: init failed");
	}
}
