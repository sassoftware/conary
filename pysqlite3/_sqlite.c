/*                                            _ _ _
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
** All Rights Reserved
**
** Permission to use, copy, modify, and distribute this software and its
** documentation for any purpose and without fee is hereby granted, provided
** that the above copyright notice appear in all copies and that both that
** copyright notice and this permission notice appear in supporting
** documentation,
**
** This program is distributed in the hope that it will be useful, but WITHOUT
** ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
** FOR A PARTICULAR PURPOSE.
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

/*------------------------------------------------------------------------------
** Object Declarations
**------------------------------------------------------------------------------
*/

/** A connection object */
typedef struct
{
    PyObject_HEAD
    const char* database_name;
    const char* sql;
    sqlite3* p_db;
    PyObject* converters;
    PyObject* expected_types;
    PyObject* command_logfile;
    PyThreadState *tstate;
} pysqlc;

/** A result set object. */
typedef struct
{
    PyObject_HEAD
    pysqlc* con;
    PyObject* p_row_list;
    PyObject* p_col_def_list;
    int row_count;
} pysqlrs;

/** a statement object. */
typedef struct
{
    PyObject_HEAD
    pysqlc* con;
    sqlite3_stmt* p_stmt;
    PyObject* p_col_def_list;
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

static int debug_callbacks = 0;

#define PRINT_OR_CLEAR_ERROR if (debug_callbacks) PyErr_Print(); else PyErr_Clear();

/* A tuple describing the minimum required SQLite version */
static PyObject* required_sqlite_version;

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

/*------------------------------------------------------------------------------
** Function Prototypes
**------------------------------------------------------------------------------
*/

static int process_record(void* p_data, int num_fields, char** p_fields, char** p_col_names);

PySQLite_DECLARE_MODINIT_FUNC(init_sqlite3);
static void _con_dealloc(pysqlc *self);
static PyObject* sqlite_version_info(PyObject* self, PyObject* args);
static PyObject* pysqlite_connect(PyObject *self, PyObject *args, PyObject *kwargs);
static PyObject* sqlite_library_version(PyObject *self, PyObject *args);
static PyObject* sqlite_enable_callback_debugging(PyObject *self, PyObject *args);
static PyObject* pysqlite_encode(PyObject *self, PyObject *args);
static PyObject* pysqlite_decode(PyObject *self, PyObject *args);

/* Defined in encode.c */
int sqlite_encode_binary(const unsigned char *in, int n, unsigned char *out);
int sqlite_decode_binary(const unsigned char *in, unsigned char *out);

/** Connection Object Methods */
static PyObject* _con_get_attr(pysqlc *self, char *attr);
static PyObject* _con_close(pysqlc *self, PyObject *args);
static PyObject* _con_execute(pysqlc *self, PyObject *args);
static PyObject* _con_prepare(pysqlc *self, PyObject *args);
static PyObject* _con_register_converter(pysqlc* self, PyObject *args, PyObject* kwargs);
static PyObject* _con_set_expected_types(pysqlc* self, PyObject *args, PyObject* kwargs);
static PyObject* _con_create_function(pysqlc *self, PyObject *args, PyObject *kwargs);
static PyObject* _con_create_aggregate(pysqlc *self, PyObject *args, PyObject *kwargs);
static PyObject* _con_sqlite_exec(pysqlc *self, PyObject *args, PyObject *kwargs);
static PyObject* _con_sqlite_last_insert_rowid(pysqlc *self, PyObject *args);
static PyObject* _con_sqlite_changes(pysqlc *self, PyObject *args);
static PyObject* _con_sqlite_busy_handler(pysqlc* self, PyObject *args, PyObject* kwargs);
static PyObject* _con_sqlite_busy_timeout(pysqlc* self, PyObject *args, PyObject* kwargs);
static PyObject* _con_set_command_logfile(pysqlc* self, PyObject *args, PyObject* kwargs);

/** Result set Object Methods */
static void _rs_dealloc(pysqlrs* self);
static PyObject* _rs_get_attr(pysqlrs* self, char *attr);

static void _stmt_dealloc(pysqlstmt* self);
static PyObject* _stmt_get_attr(pysqlstmt* self, char *attr);

#ifdef _MSC_VER
#define staticforward extern
#endif

staticforward PyMethodDef _con_methods[];
staticforward struct memberlist _con_memberlist[];

PyTypeObject pysqlc_Type =
{
    PyObject_HEAD_INIT(NULL)
    0,
    "Connection",
    sizeof(pysqlc),
    0,
    (destructor) _con_dealloc,
    0,
    (getattrfunc) _con_get_attr,
    (setattrfunc) NULL,
};

PyTypeObject pysqlrs_Type =
{
    PyObject_HEAD_INIT(NULL)
    0,
    "ResultSet",
    sizeof(pysqlrs),
    0,
    (destructor) _rs_dealloc,
    0,
    (getattrfunc) _rs_get_attr,
    (setattrfunc) NULL,
};

PyTypeObject pysqlstmt_Type =
{
    PyObject_HEAD_INIT(NULL)
    0,
    "Statement",
    sizeof(pysqlstmt),
    0,
    (destructor) _stmt_dealloc,
    0,
    (getattrfunc) _stmt_get_attr,
    (setattrfunc) NULL,
};

static void
_con_dealloc(pysqlc* self)
{
    if(self)
    {
        if(self->p_db != 0)
        {
            /* Close the database */
            sqlite3_close(self->p_db);
            self->p_db = 0;
        }

        if(self->sql != NULL)
        {
            /* Free last SQL statement string */
            free((void*)self->sql);
            self->sql = NULL;
        }

        if(self->database_name != NULL)
        {
            /* Free database name string */
            free((void*)self->database_name);
            self->database_name = NULL;
        }

        Py_DECREF(self->converters);
        Py_DECREF(self->expected_types);
        Py_DECREF(self->command_logfile);

        PyObject_Del(self);
    }
}

static const char * ctype_to_str(int ctype)
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
    }
}


static int pysqlite3_execute_statement(sqlite3 *db, sqlite3_stmt *pStmt,
				       sqlite_callback xCallback,
				       void *pArg, char **pzErrMsg)
{
  int nCallback; 
  nCallback = 0;
  int nCol;
  char **azCols = 0;
  int rc;
  int i;

  nCol = sqlite3_column_count(pStmt);
  azCols = malloc(3*nCol*sizeof(const char *));
  if( nCol && !azCols ){
    rc = SQLITE_NOMEM;
    goto exec_out;
  }

  while( 1 ){
    char **azVals = 0;
    rc = sqlite3_step(pStmt);
    /* Invoke the callback function if required */
    if( xCallback && SQLITE_ROW==rc) {
      if( 0==nCallback ){
        for(i=0; i<nCol; i++){
	  char *name = (char *) sqlite3_column_name(pStmt, i);
	  char *ctype = (char *) sqlite3_column_decltype(pStmt, i);
	  if (ctype == NULL)
	      ctype = (char *) ctype_to_str(sqlite3_column_type(pStmt, i));
	  azCols[i] = name;
	  azCols[nCol + i] = ctype;
	}
	nCallback++;
      }
      if( rc==SQLITE_ROW ){
	azVals = &azCols[nCol * 2];
	for(i=0; i<nCol; i++){
	  azVals[i] = (char *)sqlite3_column_text(pStmt, i);
        }
      }
      if( xCallback(pArg, nCol, azVals, azCols)){
        rc = SQLITE_ABORT;
        goto exec_out;
      }
    }

    if( rc!=SQLITE_ROW ){
      rc = sqlite3_finalize(pStmt);
      pStmt = 0;
      break;
    }
  }
  free(azCols);
  azCols = 0;

exec_out:
  if( pStmt ) sqlite3_finalize(pStmt);
  if( azCols ) free(azCols);

  if( rc!=SQLITE_OK && rc==sqlite3_errcode(db) && pzErrMsg ){
    *pzErrMsg = malloc(1+strlen(sqlite3_errmsg(db)));
    if( *pzErrMsg ){
      strcpy(*pzErrMsg, sqlite3_errmsg(db));
    }
  }else if( pzErrMsg ){
    *pzErrMsg = 0;
  }

  return rc;
}


/* this is a function that replaces sqlite3_exec, and always
   provides type information to the callback function */
static int pysqlite3_exec(sqlite3 *db, const char *zSql,
			  sqlite_callback xCallback,
			  void *pArg, char **pzErrMsg)
{
  int rc = SQLITE_OK;
  const char *zLeftover;
  sqlite3_stmt *pStmt = 0;

  int nRetry = 0;

  if( zSql==0 ) return SQLITE_OK;
  while( (rc==SQLITE_OK || (rc==SQLITE_SCHEMA && (++nRetry)<2)) && zSql[0] ){

    pStmt = 0;
    rc = sqlite3_prepare(db, zSql, -1, &pStmt, &zLeftover);
    if( rc!=SQLITE_OK ){
      if( pStmt ) sqlite3_finalize(pStmt);
      continue;
    }
    if( !pStmt ){
      /* this happens for a comment or white-space */
      zSql = zLeftover;
      continue;
    }
    rc = pysqlite3_execute_statement(db, pStmt, xCallback,
				     pArg, pzErrMsg);
    if (rc!=SQLITE_SCHEMA) {
        nRetry = 0;
        zSql = zLeftover;
        while( isspace(zSql[0]) ) zSql++;
    }
  }
  return rc;
}

static char pysqlite_connect_doc[] =
"connect(path) -> Connection.\n\
Opens a new database connection.";

/* return a new instance of sqlite_connection */
PyObject* pysqlite_connect(PyObject *self, PyObject *args, PyObject *kwargs)
{
    const char* db_name = 0;
    int ret;

    pysqlc* obj;

    static char *kwlist[] = { "filename", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s:pysqlite_connect",
                                      kwlist, &db_name))
    {
        return NULL;
    }

    if ((obj = PyObject_New(pysqlc, &pysqlc_Type)) == NULL)
    {
        return NULL;
    }

    /* Open the database */
    ret = sqlite3_open(db_name, &obj->p_db);

    if (ret != SQLITE_OK)
    {
        PyObject_Del(obj);
	PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(obj->p_db));
        return NULL;
    }

    /* Assign the database name */
    if ((obj->database_name = strdup(db_name)) == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for database name.");
        return NULL;
    }

    /* Init sql string to NULL */
    obj->sql = NULL;

    /* Set the thread state to NULL */
    obj->tstate = NULL;

    if ((obj->converters = PyDict_New()) == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for converters.");
        return NULL;
    }

    Py_INCREF(Py_None);
    obj->expected_types = Py_None;

    Py_INCREF(Py_None);
    obj->command_logfile = Py_None;

    return (PyObject *) obj;
}

static PyObject* _con_get_attr(pysqlc *self, char *attr)
{
    PyObject *res;

    res = Py_FindMethod(_con_methods, (PyObject *) self,attr);

    if(NULL != res)
    {
        return res;
    }
    else
    {
        PyErr_Clear();
        return PyMember_Get((char *) self, _con_memberlist, attr);
    }
}

static char _con_close_doc [] =
"close()\n\
Close the database connection.";

static PyObject* _con_close(pysqlc *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args,""))
    {
        return NULL;
    }

    if(self->p_db != 0)
    {
        /* Close the database */
        sqlite3_close(self->p_db);
        self->p_db = 0;
    }
    else
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Database is not open.");
        return NULL;
    }

    Py_INCREF(Py_None);

    return Py_None;
}

static void function_callback(sqlite3_context *context,
			      int argc, sqlite3_value **argv)
{
    int i;
    PyObject* function_result;
    PyObject* args;
    PyObject* userdata;
    PyObject* func;
    PyObject* s;
    pysqlc* con;

    userdata = (PyObject*)sqlite3_user_data(context);
    func = PyTuple_GetItem(userdata, 0);
    con = (pysqlc*)PyTuple_GetItem(userdata, 1);
    MY_END_ALLOW_THREADS(con->tstate)

    args = PyTuple_New(argc);
    for (i = 0; i < argc; i++)
    {
	const char *s = sqlite3_value_text(argv[i]);
        if (s == NULL)
        {
            Py_INCREF(Py_None);
            PyTuple_SetItem(args, i, Py_None);
        }
        else
        {
            PyTuple_SetItem(args, i, PyString_FromString(s));
        }
    }

    function_result = PyObject_CallObject(func, args);
    Py_DECREF(args);

    if (PyErr_Occurred())
    {
        PRINT_OR_CLEAR_ERROR
        sqlite3_result_error(context, NULL, -1);
        MY_BEGIN_ALLOW_THREADS(con->tstate)
        return;
    }

    if (function_result == Py_None)
    {
        sqlite3_result_text(context, NULL, -1, SQLITE_TRANSIENT);
    }
    else
    {
        s = PyObject_Str(function_result);
        sqlite3_result_text(context, PyString_AsString(s), -1, SQLITE_TRANSIENT);
        Py_DECREF(s);
    }

    Py_DECREF(function_result);
    MY_BEGIN_ALLOW_THREADS(con->tstate)
}

static void aggregate_step(sqlite3_context *context, int argc,
			   sqlite3_value **argv)
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
    MY_END_ALLOW_THREADS(con->tstate)

    aggregate_instance = (PyObject**)sqlite3_aggregate_context(context, sizeof(PyObject*));

    if (*aggregate_instance == 0) {
        args = PyTuple_New(0);
        *aggregate_instance = PyObject_CallObject(aggregate_class, args);
        Py_DECREF(args);

        if (PyErr_Occurred())
        {
            PRINT_OR_CLEAR_ERROR
            MY_BEGIN_ALLOW_THREADS(con->tstate)
            return;
        }
    }

    stepmethod = PyObject_GetAttrString(*aggregate_instance, "step");
    if (!stepmethod)
    {
        /* PRINT_OR_CLEAR_ERROR */
        MY_BEGIN_ALLOW_THREADS(con->tstate)
        return;
    }

    args = PyTuple_New(argc);
    for (i = 0; i < argc; i++) {
	const char *s = sqlite3_value_text(argv[i]);
        if (s == NULL) {
            Py_INCREF(Py_None);
            PyTuple_SetItem(args, i, Py_None);
        } else {
            PyTuple_SetItem(args, i, PyString_FromString(s));
        }
    }

    if (PyErr_Occurred())
    {
        PRINT_OR_CLEAR_ERROR
    }

    function_result = PyObject_CallObject(stepmethod, args);
    Py_DECREF(args);
    Py_DECREF(stepmethod);

    if (function_result == NULL)
    {
        PRINT_OR_CLEAR_ERROR
        /* Don't use sqlite_set_result_error here. Else an assertion in
         * the SQLite code will trigger and create a core dump.
         */
    }
    else
    {
        Py_DECREF(function_result);
    }

    MY_BEGIN_ALLOW_THREADS(con->tstate)
}

static void aggregate_finalize(sqlite3_context *context)
{
    PyObject* args;
    PyObject* function_result;
    PyObject* s;
    PyObject** aggregate_instance;
    PyObject* userdata;
    pysqlc* con;
    PyObject* aggregate_class;
    PyObject* finalizemethod;

    userdata = (PyObject*)sqlite3_user_data(context);
    aggregate_class = PyTuple_GetItem(userdata, 0);
    con = (pysqlc*)PyTuple_GetItem(userdata, 1);
    MY_END_ALLOW_THREADS(con->tstate)

    aggregate_instance = (PyObject**)sqlite3_aggregate_context(context, sizeof(PyObject*));

    finalizemethod = PyObject_GetAttrString(*aggregate_instance, "finalize");

    if (!finalizemethod)
    {
        PyErr_SetString(PyExc_ValueError, "finalize method missing");
        goto error;
    }

    args = PyTuple_New(0);
    function_result = PyObject_CallObject(finalizemethod, args);
    Py_DECREF(args);
    Py_DECREF(finalizemethod);

    if (PyErr_Occurred())
    {
        PRINT_OR_CLEAR_ERROR
        sqlite3_result_error(context, NULL, -1);
    }
    else if (function_result == Py_None)
    {
        Py_DECREF(function_result);
        sqlite3_result_text(context, NULL, -1, SQLITE_TRANSIENT);
    }
    else
    {
        Py_DECREF(function_result);
        s = PyObject_Str(function_result);
        sqlite3_result_text(context, PyString_AsString(s), -1, SQLITE_TRANSIENT);
        Py_DECREF(s);
    }

error:
    Py_XDECREF(*aggregate_instance);

    MY_BEGIN_ALLOW_THREADS(con->tstate)
}

static int sqlite_busy_handler_callback(void* void_data, int num_busy)
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

    MY_END_ALLOW_THREADS(con->tstate)

    args = PyTuple_New(3);
    PyTuple_SetItem(args, 0, userdata);
    PyTuple_SetItem(args, 1, PyInt_FromLong((long)num_busy));

    function_result = PyObject_CallObject(func, args);
    Py_DECREF(args);

    if (PyErr_Occurred())
    {
        PRINT_OR_CLEAR_ERROR
        MY_BEGIN_ALLOW_THREADS(con->tstate)
        return 0;
    }

    result_int = PyObject_IsTrue(function_result);

    Py_DECREF(function_result);

    MY_BEGIN_ALLOW_THREADS(con->tstate)

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
    callback returns zero, then SQLite aborts the current operation and returns\n\
    SQLITE_BUSY, which PySQLite will make throw an OperationalError.\n\
    \n\
    The arguments to sqlite_busy_handler() are the callback function (func) and\n\
    an additional argument (data) that will be passed to the busy callback\n\
    function.\n\
    \n\
    When the busy callback is invoked, it is sent three arguments. The first\n\
    argument will be the 'data' that was set as the third argument to\n\
    sqlite_busy_handler. The second will be the name of the database table or\n\
    index that SQLite was trying to access and the third one will be the number\n\
    of times that the library has attempted to access the database table or\n\
    index.";

static PyObject* _con_sqlite_busy_handler(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    static char *kwlist[] = {"func", "data", NULL};
    PyObject* func;
    PyObject* data = Py_None;
    PyObject* userdata;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O:sqlite_busy_handler",
                                      kwlist, &func, &data))
    {
        return NULL;
    }

    if ((userdata = PyTuple_New(3)) == NULL)
    {
        return NULL;
    }
    Py_INCREF(func); PyTuple_SetItem(userdata, 0, func);
    Py_INCREF(data); PyTuple_SetItem(userdata, 1, data);
    Py_INCREF(self); PyTuple_SetItem(userdata, 2, (PyObject*)self);

    sqlite3_busy_handler(self->p_db, &sqlite_busy_handler_callback, userdata);

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

static PyObject* _con_sqlite_busy_timeout(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    int timeout;
    static char *kwlist[] = {"timeout", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i:sqlite_busy_timeout",
                                      kwlist, &timeout))
    {
        return NULL;
    }

    sqlite3_busy_timeout(self->p_db, timeout);

    Py_INCREF(Py_None);
    return Py_None;
}

static char _con_create_function_doc[] =
"create_function(name, n_args, func)\n\
Create a new SQL function.\n\
\n\
    A new function under the name 'name', with 'n_args' arguments is created.\n\
    The callback 'func' will be called for this function.";

static PyObject* _con_create_function(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    int n_args;
    char* name;
    PyObject* func;
    PyObject* userdata;
    static char *kwlist[] = {"name", "n_args", "func", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "siO:create_function",
                                     kwlist, &name, &n_args,
                                     &func))
    {
        return NULL;
    }

    if (!(userdata = PyTuple_New(2))) return NULL;
    Py_INCREF(func);
    PyTuple_SetItem(userdata, 0, func);
    Py_INCREF(self);
    PyTuple_SetItem(userdata, 1, (PyObject*)self);

    if (!PyCallable_Check(func))
    {
        PyErr_SetString(PyExc_ValueError, "func must be a callable!");
        return NULL;
    }

    Py_INCREF(func);
    if (0 != sqlite3_create_function(self->p_db, name, n_args, SQLITE_UTF8,
				     (void *) userdata,
				     &function_callback, NULL, NULL))
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Cannot create function.");
        return NULL;
    }
    else
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

static char _con_create_aggregate_doc[] =
"create_aggregate(name, n_args, step_func, finalize_func)\n\
Create a new SQL function.\n\
\n\
    A new aggregate function under the name 'name', with 'n_args' arguments to\n\
    the 'step_func' function will be created. 'finalize_func' will be called\n\
    without arguments for finishing the aggregate.";

static PyObject* _con_create_aggregate(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    PyObject* aggregate_class;

    int n_args;
    char* name;
    static char *kwlist[] = { "name", "n_args", "aggregate_class", NULL };
    PyObject* userdata;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "siO:create_aggregate",
                                      kwlist, &name, &n_args, &aggregate_class))
    {
        return NULL;
    }

    if (!(userdata = PyTuple_New(2))) return NULL;
    Py_INCREF(aggregate_class);
    PyTuple_SetItem(userdata, 0, aggregate_class);
    Py_INCREF(self);
    PyTuple_SetItem(userdata, 1, (PyObject*)self);

    if (0 != sqlite3_create_function(self->p_db, name, n_args, SQLITE_UTF8,
				     (void*)userdata, NULL,
				     &aggregate_step, &aggregate_finalize))
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Cannot create aggregate.");
        return NULL;
    }
    else
    {
        Py_INCREF(Py_None);
        return Py_None;
    }
}

static char _con_set_command_logfile_doc[] =
"set_command_logfile(logfile)\n\
Registers a writeable file-like object as logfile where all SQL commands\n\
that get executed are written to.";

static PyObject* _con_set_command_logfile(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    PyObject* logfile;
    PyObject* o;

    static char *kwlist[] = { "logfile", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O:set_command_logfile",
                                      kwlist, &logfile))
    {
        return NULL;
    }

    if (logfile == Py_None)
    {
        Py_DECREF(logfile);

        Py_INCREF(Py_None);
        return Py_None;
    }

    o = PyObject_GetAttrString(logfile, "write");
    if (!o)
    {
        PyErr_SetString(PyExc_ValueError, "logfile must have a 'write' attribute!");
        return NULL;
    }

    if (!PyCallable_Check(o))
    {
        PyErr_SetString(PyExc_ValueError, "logfile must have a callable 'write' attribute!");
        Py_DECREF(o);
        return NULL;
    }

    Py_DECREF(o);
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
    for (i = 0; i < argc; i++)
    {
        PyTuple_SetItem(colnames, i, PyString_FromString(columnNames[i]));
    }

    values = PyTuple_New(argc);
    for (i = 0; i < argc; i++)
    {
        if (argv[i] == NULL)
        {
            Py_INCREF(Py_None);
            PyTuple_SetItem(values, i, Py_None);
        }
        else
        {
            PyTuple_SetItem(values, i, PyString_FromString(argv[i]));
        }
    }

    calling_args = PyTuple_New(3);
    Py_INCREF(arg1);
    PyTuple_SetItem(calling_args, 0, arg1);
    PyTuple_SetItem(calling_args, 1, values);
    PyTuple_SetItem(calling_args, 2, colnames);

    function_result = PyObject_CallObject(callback, calling_args);
    if (PyErr_Occurred())
    {
        PRINT_OR_CLEAR_ERROR
        MY_BEGIN_ALLOW_THREADS(con->tstate)
        return 1;
    }

    Py_DECREF(function_result);
    Py_DECREF(calling_args);

    MY_BEGIN_ALLOW_THREADS(con->tstate)
    return 0;
}

static char _con_sqlite_exec_doc[] =
"sqlite_exec(sql, func, arg, use_types=0)\n\
Execute SQL.\n\
\n\
    Executes the SQL string 'sql' and uses the callback function 'func' for\n\
    each returned row. The argument 'arg' will be passed to the callback\n\
    function.\n\
    \n\
    The signature of the callback function is (arg, values, colnames{, types}).\n\
    types is ommitted unless use_types is true.";

static PyObject* _con_sqlite_exec(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    static char *kwlist[] = {"sql", "func", "arg", "use_types", NULL};
    char* sql;
    PyObject* callback;
    PyObject* arg1;
    int use_types = 0;

    PyObject* cb_args;

    /* TODO add errmsg handling */
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sOO|i:sqlite_exec",
                                      kwlist, &sql, &callback, &arg1, &use_types))
    {
        return NULL;
    }

    cb_args = PyTuple_New(3);
    Py_INCREF(callback);
    Py_INCREF(arg1);
    Py_INCREF(self);
    PyTuple_SetItem(cb_args, 0, callback);
    PyTuple_SetItem(cb_args, 1, arg1);
    PyTuple_SetItem(cb_args, 2, (PyObject*)self);

    MY_BEGIN_ALLOW_THREADS(self->tstate)
    pysqlite3_exec(self->p_db, sql, &sqlite_exec_callback, cb_args, NULL);
    MY_END_ALLOW_THREADS(self->tstate)

    Py_DECREF(cb_args);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* _con_sqlite_last_insert_rowid(pysqlc *self, PyObject *args)
{
    PyObject* value;

    if (!PyArg_ParseTuple(args,""))
    {
        return NULL;
    }

    value = PyInt_FromLong((long)sqlite3_last_insert_rowid(self->p_db));

    return value;
}

static PyObject* _con_sqlite_changes(pysqlc *self, PyObject *args)
{
    PyObject* value;

    if (!PyArg_ParseTuple(args,""))
    {
        return NULL;
    }

    value = PyInt_FromLong((long)sqlite3_changes(self->p_db));

    return value;
}

static PyObject * sqlite_library_version(PyObject *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args, ""))
    {
        return NULL;
    }

    return Py_BuildValue("s", SQLITE_VERSION);
}

static PyObject* sqlite_enable_callback_debugging(PyObject *self, PyObject *args)
{
    if (!PyArg_ParseTuple(args, "i", &debug_callbacks))
    {
        return NULL;
    }

    Py_INCREF(Py_None);
    return Py_None;
}

static char pysqlite_encode_doc[] =
"encode(s) -> encoded binary string.\n\
Encode binary string 's' for storage in SQLite.";

static PyObject* pysqlite_encode(PyObject *self, PyObject *args)
{
    char *in, *out;
    int n;
    PyObject *res;

    if (!PyArg_ParseTuple(args, "s#", &in, &n))
    {
        return NULL;
    }

    /* See comments in encode.c for details on maximum size of encoded data. */
    out = malloc(2 + (257*n)/254);
    if (out == NULL)
    {
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

static PyObject* pysqlite_decode(PyObject *self, PyObject *args)
{
    char *in, *out;
    int n;
    PyObject *res;

    if (!PyArg_ParseTuple(args, "s", &in))
    {
        return NULL;
    }

    /* Decoded string is always shorter than encoded string. */
    out = malloc(strlen(in));
    if (out == NULL)
    {
        return PyErr_NoMemory();
    }
    n = sqlite_decode_binary(in, out);
    res = Py_BuildValue("s#", out, n);
    free(out);
    return res;
}

static PyObject* _con_execute(pysqlc* self, PyObject *args)
{
    int ret;
    int record_number;
    char* sql;
    pysqlrs* p_rset;
    char *errmsg;
    char* buf;
    char* iterator;
    char* token;
    PyObject* logfile_writemethod;
    PyObject* logfile_writeargs;

    record_number = 0;

    if(!PyArg_ParseTuple(args,"s:execute", &sql))
    {
        return NULL;
    }

    if(self->p_db == 0)
    {
        /* There is no open database. */
        PyErr_SetString(_sqlite_ProgrammingError, "There is no open database.");
        return NULL;
    }

    if(self->sql != NULL)
    {
        /* Free last SQL statment string */
        free((void*)self->sql);
        self->sql = NULL;
    }

    /* Save SQL statement */
    self->sql = strdup(sql);

    /* Log SQL statement */
    if (self->command_logfile != Py_None)
    {
        logfile_writemethod = PyObject_GetAttrString(self->command_logfile,
                                                    "write");
        logfile_writeargs = PyTuple_New(1);
        PyTuple_SetItem(logfile_writeargs, 0, PyString_FromString(sql));

        PyObject_CallObject(logfile_writemethod, logfile_writeargs);

        Py_DECREF(logfile_writeargs);

        logfile_writeargs = PyTuple_New(1);
        PyTuple_SetItem(logfile_writeargs, 0, PyString_FromString(";\n"));
        PyObject_CallObject(logfile_writemethod, logfile_writeargs);

        Py_DECREF(logfile_writeargs);
        Py_DECREF(logfile_writemethod);

        if (PyErr_Occurred())
        {
            free((void*)(self->sql));
            self->sql = NULL;
            return NULL;
        }
    }

    p_rset = PyObject_New(pysqlrs, &pysqlrs_Type);
    if (p_rset == NULL)
    {
        return NULL;
    }

    Py_INCREF(self);
    p_rset->con = self;
    p_rset->p_row_list = PyList_New(0);
    p_rset->p_col_def_list = NULL;
    p_rset->row_count = 0;

    if (strstr(sql, "-- types "))
    {
        Py_DECREF(self->expected_types);
        self->expected_types = PyList_New(0);
        if (PyErr_Occurred())
        {
            Py_INCREF(Py_None);
            self->expected_types = Py_None;
            return NULL;
        }

        if ((buf = strdup(sql)) == NULL)
        {
            PyErr_SetString(PyExc_MemoryError, "Cannot allocate buffer for copying SQL statement!");
            return NULL;
        }

        iterator = buf + strlen("-- types ");

        if (*iterator == 0)
        {
            free(buf);
            PyErr_SetString(PyExc_ValueError, "Illegal pragma!");
            return NULL;
        }

        while (iterator != NULL)
        {
            token = pysqlite_strsep(&iterator, ",");
            while (*token == ' ')
            {
                token++;
            }

            PyList_Append(self->expected_types, Py_BuildValue("s", token));
        }

        free(buf);
        p_rset->p_col_def_list = PyTuple_New(0);
        return (PyObject*)p_rset;
    }

    /* Run a query: process_record is called back for each record returned. */
    MY_BEGIN_ALLOW_THREADS(self->tstate)
    ret = pysqlite3_exec(self->p_db,
			 sql,
			 process_record,
			 p_rset,
			 &errmsg);
    MY_END_ALLOW_THREADS(self->tstate)

    Py_DECREF(self->expected_types);
    Py_INCREF(Py_None);
    self->expected_types = Py_None;

    /* Maybe there occurred an error in a user-defined function */
    if (PyErr_Occurred())
    {
        free((void*)(self->sql));
        self->sql = NULL;
        Py_DECREF(p_rset);
        return NULL;
    }

    if (p_rset->p_col_def_list == NULL)
    {
        p_rset->p_col_def_list = PyTuple_New(0);
    }

    if(ret != SQLITE_OK)
    {
	PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->p_db));
        free((void*)(self->sql));
        self->sql = NULL;
        Py_DECREF(p_rset);
        return NULL;
    }

    return (PyObject*)p_rset;
}

static PyObject* _con_prepare(pysqlc* self, PyObject *args)
{
    int ret;
    int record_number;
    int sql_len;
    char* sql;
    pysqlstmt* pystmt;
    const char* query_tail;
    PyObject* logfile_writemethod;
    PyObject* logfile_writeargs;

    record_number = 0;

    if(!PyArg_ParseTuple(args,"s#:prepare", &sql, &sql_len))
    {
        return NULL;
    }
    
    if(self->p_db == 0)
    {
        /* There is no open database. */
        PyErr_SetString(_sqlite_ProgrammingError, "There is no open database.");
        return NULL;
    }

    /* Log SQL statement */
    if (self->command_logfile != Py_None)
    {
        logfile_writemethod = PyObject_GetAttrString(self->command_logfile,
                                                    "write");

        logfile_writeargs = PyTuple_New(1);
        PyTuple_SetItem(logfile_writeargs, 0, PyString_FromString(sql));
        PyObject_CallObject(logfile_writemethod, logfile_writeargs);
        PyTuple_SetItem(logfile_writeargs, 0, PyString_FromString(";\n"));
        PyObject_CallObject(logfile_writemethod, logfile_writeargs);

        Py_DECREF(logfile_writeargs);
        Py_DECREF(logfile_writemethod);

        if (PyErr_Occurred())
        {
            return NULL;
        }
    }

    pystmt = PyObject_New(pysqlstmt, &pysqlstmt_Type);
    if (pystmt == NULL)
    {
        return NULL;
    }
    pystmt->p_col_def_list = PyTuple_New(0);
    pystmt->reset = 1;

    Py_INCREF(self);
    pystmt->con = self;
    ret = sqlite3_prepare(self->p_db, sql, sql_len, &pystmt->p_stmt, &query_tail);
    if (*query_tail != '\0') {
        Py_DECREF(pystmt);
	PyErr_SetString(_sqlite_ProgrammingError,
			"SQL must only contain one statement.");
        return NULL;
    }
    
    if (ret != SQLITE_OK) {
	const char *errmsg = sqlite3_errmsg(self->p_db);
        Py_DECREF(pystmt);
	PyErr_SetString(_sqlite_DatabaseError, errmsg);
        return NULL;
    }

    return (PyObject*)pystmt;
}

static PyObject *build_col_def_list(int num_fields, char **p_col_names)
{
    PyObject* p_col_def_list;
    PyObject* p_col_def;
    PyObject* type_code;
    char type_name[255];
    int i, l, j;
    
    if ((p_col_def_list = PyTuple_New(num_fields)) == NULL)
    {
        return NULL;
    }

    for (i=0; i < num_fields; i++)
    {
        p_col_def = PyTuple_New(7);

        /* 1. Column Name */
        PyTuple_SetItem(p_col_def, 0, Py_BuildValue("s", p_col_names[i]));

        /* 2. Type code */
        /* Make a copy of column type. */
        if (p_col_names[num_fields + i] == NULL)
        {
            strcpy(type_name, "TEXT");
        }
        else
        {
            strncpy(type_name, p_col_names[num_fields + i], sizeof(type_name) - 1);
        }

        /* Get its length. */
        l = strlen(type_name);

        /* Convert to uppercase. */
        for(j=0; j < l; j++)
        {
            type_name[j] = toupper(type_name[j]);
        }

        /* Init/unset value */
        type_code = NULL;

        /* Try to determine column type. */
        if (strstr(type_name, "INTERVAL"))
        {
            type_code = tc_INTERVAL;
        }
        else if (strstr(type_name, "INT"))
        {
            type_code = tc_INTEGER;
        }
        else if (strstr(type_name, "CHAR")
              || strstr(type_name, "TEXT"))
        {
            type_code = tc_STRING;
        }
        else if (strstr(type_name, "UNICODE"))
        {
            type_code = tc_UNICODESTRING;
        }
        else if (strstr(type_name, "BINARY")
              || strstr(type_name, "BLOB"))
        {
            type_code = tc_BINARY;
        }
        else if (strstr(type_name, "FLOAT")
              || strstr(type_name, "NUMERIC")
              || strstr(type_name, "NUMBER")
              || strstr(type_name, "DECIMAL")
              || strstr(type_name, "REAL")
              || strstr(type_name, "DOUBLE"))
        {
            type_code = tc_FLOAT;
        }
        else if (strstr(type_name, "TIMESTAMP"))
        {
            type_code = tc_TIMESTAMP;
        }
        else if (strstr(type_name, "DATE"))
        {
            type_code = tc_DATE;
        }
        else if (strstr(type_name, "TIME"))
        {
            type_code = tc_TIME;
        }
        else if (type_code == NULL)
        {
            type_code = Py_None;
        }

        /* Assign type. */
        Py_INCREF(type_code);
        PyTuple_SetItem(p_col_def, 1, type_code);

        /* 3. Display Size */
        Py_INCREF(Py_None);
        PyTuple_SetItem(p_col_def, 2, Py_None);

        /* 4. Internal Size */
        Py_INCREF(Py_None);
        PyTuple_SetItem(p_col_def, 3, Py_None);

        /* 5. Precision */
        Py_INCREF(Py_None);
        PyTuple_SetItem(p_col_def, 4, Py_None);

        /* 6. Scale */
        Py_INCREF(Py_None);
        PyTuple_SetItem(p_col_def, 5, Py_None);

        /* 7. NULL Okay */
        Py_INCREF(Py_None);
        PyTuple_SetItem(p_col_def, 6, Py_None);

        PyTuple_SetItem(p_col_def_list, i, p_col_def);
    }
    return p_col_def_list;
}
     

static PyObject *build_row(pysqlc* p_con, int num_fields, char** p_fields,
			   PyObject* p_col_def_list)
{
    PyObject* p_row;
    PyObject* p_col_def;
    PyObject* expected_types;
    PyObject* type_code;
    PyObject* converters;
    PyObject* converted;
    PyObject* expected_type_name = NULL;
    PyObject* callable;
    PyObject* callable_args;
    int i;

    expected_types = p_con->expected_types;
    converters = p_con->converters;

    /* Create a row */
    p_row = PyTuple_New(num_fields);

    for (i=0; i < num_fields; i++)
    {
        /* Store the field value */
        if(p_fields[i] != 0)
        {
            p_col_def = PyTuple_GetItem(p_col_def_list, i);

            type_code = PyTuple_GetItem(p_col_def, 1);

            if (expected_types != Py_None)
            {
                if (i < PySequence_Length(expected_types))
                {
                    expected_type_name = PySequence_GetItem(expected_types, i);
                    callable = PyDict_GetItem(converters, expected_type_name);
                    if (callable == NULL)
                    {
                        Py_INCREF(Py_None);
                        PyTuple_SetItem(p_row, i, Py_None);
                    }
                    else
                    {
                        callable_args = PyTuple_New(1);
                        PyTuple_SetItem(callable_args, 0, Py_BuildValue("s", p_fields[i]));

                        converted = PyObject_CallObject(callable, callable_args);
                        if (PyErr_Occurred())
                        {
                            PRINT_OR_CLEAR_ERROR
                            Py_INCREF(Py_None);
                            converted = Py_None;
                        }

                        PyTuple_SetItem(p_row, i, converted);

                        Py_DECREF(callable_args);
                    }
                }
                else
                {
                    Py_INCREF(Py_None);
                    PyTuple_SetItem(p_row, i, Py_None);
                }
            }
            else if (type_code == tc_INTEGER)
            {
                PyTuple_SetItem(p_row, i, Py_BuildValue("i", atol(p_fields[i])));
            }
            else if (type_code == tc_FLOAT)
            {
                PyTuple_SetItem(p_row, i, Py_BuildValue("f", atof(p_fields[i])));
            }
            else if (type_code == tc_DATE || type_code == tc_TIME
                    || type_code == tc_TIMESTAMP || type_code == tc_INTERVAL)
            {
                if (type_code == tc_DATE)
                    expected_type_name = PyString_FromString("date");
                else if (type_code == tc_TIME)
                    expected_type_name = PyString_FromString("time");
                else if (type_code == tc_TIMESTAMP)
                    expected_type_name = PyString_FromString("timestamp");
                else if (type_code == tc_INTERVAL)
                    expected_type_name = PyString_FromString("interval");

                callable = PyDict_GetItem(converters, expected_type_name);
                if (callable == NULL)
                {
                    PyTuple_SetItem(p_row, i, PyString_FromString(p_fields[i]));
                }
                else
                {
                    callable_args = PyTuple_New(1);
                    PyTuple_SetItem(callable_args, 0, Py_BuildValue("s", p_fields[i]));

                    converted = PyObject_CallObject(callable, callable_args);
                    if (PyErr_Occurred())
                    {
                        PRINT_OR_CLEAR_ERROR
                        converted = PyString_FromString(p_fields[i]);
                    }

                    PyTuple_SetItem(p_row, i, converted);

                    Py_DECREF(callable_args);
                }

                Py_DECREF(expected_type_name);
            }
            else if ((type_code == tc_UNICODESTRING) || (type_code == tc_BINARY))
            {
                if (type_code == tc_UNICODESTRING)
                    expected_type_name = PyString_FromString("unicode");
                else
                    expected_type_name = PyString_FromString("binary");

                callable = PyDict_GetItem(converters, expected_type_name);

                if (callable == NULL)
                {
                    PyTuple_SetItem(p_row, i, PyString_FromString(p_fields[i]));
                }
                else
                {
                    callable_args = PyTuple_New(1);
                    PyTuple_SetItem(callable_args, 0, Py_BuildValue("s", p_fields[i]));

                    converted = PyObject_CallObject(callable, callable_args);
                    if (PyErr_Occurred())
                    {
                        PRINT_OR_CLEAR_ERROR
                        converted = PyString_FromString(p_fields[i]);
                    }

                    PyTuple_SetItem(p_row, i, converted);

                    Py_DECREF(callable_args);
                }

                Py_DECREF(expected_type_name);
            }
            else
            {
                PyTuple_SetItem(p_row, i, Py_BuildValue("s", p_fields[i]));
            }
        }
        else
        {
            /* A NULL field */
            Py_INCREF(Py_None);
            PyTuple_SetItem(p_row, i, Py_None);
        }
    }
    return p_row;
}

int process_record(void* p_data, int num_fields, char** p_fields, char** p_col_names)
{
    pysqlrs* p_rset;
    PyObject* p_row;

    p_rset = (pysqlrs*)p_data;
    MY_END_ALLOW_THREADS(p_rset->con->tstate)

    if(p_rset->row_count == 0)
    {
	p_rset->p_col_def_list = build_col_def_list(num_fields, p_col_names);
	if (p_rset->p_col_def_list == NULL) {
	    PRINT_OR_CLEAR_ERROR;
	    MY_BEGIN_ALLOW_THREADS(p_rset->con->tstate);
            return 1;
	}
    }

    if (p_fields != NULL)
    {
	p_rset->row_count++;
	p_row = build_row(p_rset->con, num_fields, p_fields,
			  p_rset->p_col_def_list);
        PyList_Append(p_rset->p_row_list, p_row);
        Py_DECREF(p_row);
    }

    MY_BEGIN_ALLOW_THREADS(p_rset->con->tstate)
    return 0;
}

static PyObject* _con_register_converter(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    static char *kwlist[] = { "name", "converter", NULL };

    PyObject* name;
    PyObject* converter;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO:register_converter",
                                      kwlist, &name, &converter))
    {
        return NULL;
    }

    if (!PyString_Check(name)) {
        PyErr_SetString(PyExc_ValueError, "name must be a string");
        return NULL;
    }

    PyDict_SetItem(self->converters, name, converter);

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject* _con_set_expected_types(pysqlc* self, PyObject *args, PyObject* kwargs)
{
    static char *kwlist[] = {"types", NULL};

    PyObject* types;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O:set_expected_types", kwlist, &types))
    {
        return NULL;
    }

    if ((types != Py_None) && (!PySequence_Check(types)))
    {
        PyErr_SetString(PyExc_ValueError, "types must be a sequence");
        return NULL;
    }

    Py_DECREF(self->expected_types);

    Py_INCREF(types);
    self->expected_types = types;

    Py_INCREF(Py_None);
    return Py_None;
}

/*------------------------------------------------------------------------------
** Result Set Object Implementation
**------------------------------------------------------------------------------
*/

static struct memberlist _rs_memberlist[] =
{
    {"row_list", T_OBJECT, offsetof(pysqlrs, p_row_list),     RO},
    {"col_defs", T_OBJECT, offsetof(pysqlrs, p_col_def_list), RO},
    {"rowcount", T_INT, offsetof(pysqlrs, row_count),         RO},
    {NULL}
};

static PyMethodDef _rs_methods[] =
{
    { NULL, NULL}
};

static void
_rs_dealloc(pysqlrs* self)
{
    if(self)
    {
        Py_DECREF(self->con);

        if(self->p_row_list != 0)
        {
            Py_DECREF(self->p_row_list);

            self->p_row_list = 0;
        }

        if(self->p_col_def_list != 0)
        {
            Py_DECREF(self->p_col_def_list);

            self->p_col_def_list = 0;
        }

        PyObject_Del(self);
    }
}

static PyObject* _rs_get_attr(pysqlrs *self, char *attr)
{
    PyObject *res;

    res = Py_FindMethod(_rs_methods, (PyObject *) self,attr);

    if(NULL != res)
    {
        return res;
    }
    else
    {
        PyErr_Clear();
        return PyMember_Get((char *) self, _rs_memberlist, attr);
    }
}

static PyObject* sqlite_version_info(PyObject* self, PyObject* args)
{
    PyObject* vi_list;
    PyObject* vi_tuple;
    char* buf;
    char* iterator;
    char* token;

    if (!PyArg_ParseTuple(args, ""))
    {
        return NULL;
    }

    buf = strdup(SQLITE_VERSION);
    iterator = buf;

    vi_list = PyList_New(0);

    while ((token = pysqlite_strsep(&iterator, ".")) != NULL)
    {
        PyList_Append(vi_list, PyInt_FromLong((long)atoi(token)));
    }

    vi_tuple = PyList_AsTuple(vi_list);
    Py_DECREF(vi_list);

    return vi_tuple;
}

/*------------------------------------------------------------------------------
** Statement Object Implementation
**------------------------------------------------------------------------------
*/

static char _stmt_execute_doc [] =
"execute()\n\
Executes a statement and returns a result set object.";

static PyObject* _stmt_execute(pysqlstmt *self, PyObject *args)
{
    pysqlrs* p_rset;
    int ret;

    if(self->p_stmt == NULL)
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Statement has already been finalized.");
        return NULL;
    }
    
    p_rset = PyObject_New(pysqlrs, &pysqlrs_Type);
    if (p_rset == NULL)
    {
        return NULL;
    }

    Py_INCREF(self);
    p_rset->con = self->con;
    p_rset->p_row_list = PyList_New(0);
    p_rset->p_col_def_list = NULL;
    p_rset->row_count = 0;

    MY_BEGIN_ALLOW_THREADS(self->con->tstate)
    ret = pysqlite3_execute_statement(self->con->p_db,
				      self->p_stmt,
				      process_record,
				      p_rset, NULL);
    MY_END_ALLOW_THREADS(self->con->tstate)

    /* Maybe there occurred an error in a user-defined function */
    if (PyErr_Occurred())
    {
	PyObject *err_type, *err_value, *err_traceback;
	PyErr_Fetch(&err_type, &err_value, &err_traceback);
        Py_DECREF(p_rset);
	PyErr_Restore(err_type, err_value, err_traceback);
        return NULL;
    }

    if (p_rset->p_col_def_list == NULL)
    {
        p_rset->p_col_def_list = PyTuple_New(0);
    }

    if(ret != SQLITE_OK)
    {
        Py_DECREF(p_rset);
	PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->con->p_db));
        return NULL;
    }

    return (PyObject*)p_rset;
}

static char _stmt_step_doc [] =
"step()\n\
Fetch the next row from a the statement.";

static PyObject* _stmt_step(pysqlstmt *self, PyObject *args)
{
    int result;
    int num_fields=0;
    int i;
    const char **p_fields=NULL;
    const char **p_col_names=NULL;
    sqlite3_stmt *stmt = self->p_stmt;

    num_fields = sqlite3_column_count(self->p_stmt);
    p_col_names = malloc(3*num_fields*sizeof(const char *));
    p_fields = p_col_names + (2 * num_fields);

    if (num_fields && !p_fields) {
        PyErr_SetString(PyExc_MemoryError, "Cannot allocate memory for results.");
        return NULL;
    }
    
    if(self->p_stmt == NULL)
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Statement has already been finalized.");
        return NULL;
    }
    
    result = sqlite3_step(self->p_stmt);

    if (result == SQLITE_ROW) {
	if (self->reset) {
	    for(i=0; i < num_fields; i++){
		char *name = (char *) sqlite3_column_name(stmt, i);
		char *ctype = (char *) sqlite3_column_decltype(stmt, i);
		if (ctype == NULL)
		    ctype = (char *) ctype_to_str(sqlite3_column_type(stmt, i));
		p_col_names[i] = name;
		p_col_names[num_fields + i] = ctype;
	    }
	    self->p_col_def_list = build_col_def_list(num_fields,
						      (char **)p_col_names);
	    if (self->p_col_def_list == NULL) {
		return NULL;
	    }
	    self->reset = 0;
	}
	return build_row(self->con, num_fields, (char **)p_fields,
			 self->p_col_def_list);
    } else if (result == SQLITE_DONE) {
	Py_INCREF(Py_None);
	return Py_None;
    } else {
	PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->con->p_db));
	return NULL;
    }
}

static char _stmt_reset_doc [] =
"reset()\n\
Reset the virtual machine associated with a stmtiled SQL statement.";

static PyObject* _stmt_reset(pysqlstmt *self, PyObject *args)
{
    int result;

    if (!PyArg_ParseTuple(args,""))
    {
        return NULL;
    }

    if(self->p_stmt == NULL)
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Statement has already been finalized.");
        return NULL;
    }
    
    result = sqlite3_reset(self->p_stmt);
    if (result != SQLITE_OK) {
	PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->con->p_db));
	return NULL;
    }
    
    Py_INCREF(Py_None);
    return Py_None;
}

static char _stmt_finalize_doc [] =
"finalize()\n\
Frees the virtual machine associated with a stmtiled SQL statement.";

static PyObject* _stmt_finalize(pysqlstmt *self, PyObject *args)
{
    int result;

    if (!PyArg_ParseTuple(args,""))
    {
        return NULL;
    }

    if(self->p_stmt == NULL)
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Statement has already been finalized.");
        return NULL;
    }

    if (self->p_stmt != NULL) {
	result = sqlite3_finalize(self->p_stmt);
	if (result != SQLITE_OK) {
	    PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->con->p_db));
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

static PyObject* _stmt_bind(pysqlstmt *self, PyObject *args)
{
    int idx, rc;
    PyObject *obj;

    if (!PyArg_ParseTuple(args, "iO", &idx, &obj))
    {
        return NULL;
    }

    if(self->p_stmt == NULL)
    {
        PyErr_SetString(_sqlite_ProgrammingError, "Statement has already been finalized.");
        return NULL;
    }

    rc = -1;
    if (PyInt_Check(obj)) {
	rc = sqlite3_bind_int(self->p_stmt, idx, PyInt_AsLong(obj));
    } else if (PyLong_Check(obj)) {
	rc = sqlite3_bind_int64(self->p_stmt, idx, PyLong_AsLongLong(obj));
    } else if (PyString_Check(obj)) {
	char *buf;
	int len;
	PyString_AsStringAndSize(obj, &buf, &len);
	if (memchr(buf, '\0', len))
	    rc = sqlite3_bind_blob(self->p_stmt, idx, buf, len, SQLITE_TRANSIENT);
	else
	    rc = sqlite3_bind_text(self->p_stmt, idx, buf, len, SQLITE_TRANSIENT);
    } else if (PyFloat_Check(obj)) {
	rc = sqlite3_bind_double(self->p_stmt, idx, PyFloat_AsDouble(obj));	
    } else if (obj == Py_None) {
	rc = sqlite3_bind_null(self->p_stmt, idx);	
    }
    if (rc != SQLITE_OK) {
	if (rc == -1)
	    PyErr_SetString(_sqlite_ProgrammingError, "unknown type, unable to bind");
	else
	    PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->con->p_db));
	return NULL;
    }
    
    Py_INCREF(Py_None);
    return Py_None;
}

static struct memberlist _stmt_memberlist[] =
{
    {"col_defs", T_OBJECT, offsetof(pysqlstmt, p_col_def_list), RO},
    {NULL}
};

static PyMethodDef _stmt_methods[] =
{
    {"bind", (PyCFunction) _stmt_bind, METH_VARARGS, _stmt_bind_doc},
    {"execute", (PyCFunction) _stmt_execute, METH_VARARGS, _stmt_execute_doc},
    {"finalize", (PyCFunction) _stmt_finalize, METH_VARARGS, _stmt_finalize_doc},
    {"reset", (PyCFunction) _stmt_reset, METH_VARARGS, _stmt_reset_doc},
    {"step", (PyCFunction) _stmt_step, METH_VARARGS, _stmt_step_doc},
    { NULL, NULL}
};

static void
_stmt_dealloc(pysqlstmt* self)
{
    int result, have_error;
    PyObject *err_type, *err_value, *err_traceback;
    have_error = PyErr_Occurred() ? 1 : 0;
    if (have_error)
	PyErr_Fetch(&err_type, &err_value, &err_traceback);    
    
    if(self->p_col_def_list != 0) {
	Py_DECREF(self->p_col_def_list);
	self->p_col_def_list = 0;
    }
    
    if (self->p_stmt != NULL) {
	result = sqlite3_finalize(self->p_stmt);
	if (result != SQLITE_OK) {
	    PyErr_SetString(_sqlite_DatabaseError, sqlite3_errmsg(self->con->p_db));
	    PyErr_WriteUnraisable(PyString_FromString("_sqlite.Statement.__del__"));
	    if (have_error)
		PyErr_Restore(err_type, err_value, err_traceback);
	}
    }
    Py_DECREF(self->con);
    PyObject_Del(self);
}

static PyObject* _stmt_get_attr(pysqlstmt *self, char *attr)
{
    PyObject *res;

    res = Py_FindMethod(_stmt_methods, (PyObject *) self, attr);

    if(NULL != res)
    {
        return res;
    }
    else
    {
        PyErr_Clear();
        return PyMember_Get((char *) self, _stmt_memberlist, attr);
    }
}

/*------------------------------------------------------------------------------
** Module Definitions / Initialization
**------------------------------------------------------------------------------
*/
static PyMethodDef pysqlite_functions[] =
{
    { "connect", (PyCFunction)pysqlite_connect, METH_VARARGS | METH_KEYWORDS, pysqlite_connect_doc},
    { "sqlite_version", (PyCFunction)sqlite_library_version, METH_VARARGS},
    { "sqlite_version_info", (PyCFunction)sqlite_version_info, METH_VARARGS},
    { "enable_callback_debugging", (PyCFunction)sqlite_enable_callback_debugging, METH_VARARGS},
    { "encode", (PyCFunction)pysqlite_encode, METH_VARARGS, pysqlite_encode_doc},
    { "decode", (PyCFunction)pysqlite_decode, METH_VARARGS, pysqlite_decode_doc},
    { NULL, NULL }
};

/*------------------------------------------------------------------------------
** Connection Object Implementation
**------------------------------------------------------------------------------
*/

static struct memberlist _con_memberlist[] =
{
    {"sql",             T_STRING, offsetof(pysqlc, sql), RO},
    {"filename",        T_STRING, offsetof(pysqlc, database_name), RO},
    {NULL}
};

static PyMethodDef _con_methods[] =
{
    {"close", (PyCFunction) _con_close, METH_VARARGS, _con_close_doc},
    {"execute",  (PyCFunction)_con_execute, METH_VARARGS},
    {"prepare",  (PyCFunction)_con_prepare, METH_VARARGS},
    {"register_converter", (PyCFunction)_con_register_converter, METH_VARARGS | METH_KEYWORDS},
    {"set_expected_types", (PyCFunction)_con_set_expected_types, METH_VARARGS | METH_KEYWORDS},
    {"set_command_logfile", (PyCFunction)_con_set_command_logfile, METH_VARARGS | METH_KEYWORDS, _con_set_command_logfile_doc},
    {"create_function", (PyCFunction)_con_create_function, METH_VARARGS | METH_KEYWORDS, _con_create_function_doc},
    {"create_aggregate", (PyCFunction)_con_create_aggregate, METH_VARARGS | METH_KEYWORDS, _con_create_aggregate_doc},
    {"sqlite_exec", (PyCFunction)_con_sqlite_exec, METH_VARARGS | METH_KEYWORDS, _con_sqlite_exec_doc},
    {"sqlite_last_insert_rowid", (PyCFunction)_con_sqlite_last_insert_rowid, METH_VARARGS},
    {"sqlite_changes", (PyCFunction)_con_sqlite_changes, METH_VARARGS},
    {"sqlite_busy_handler", (PyCFunction)_con_sqlite_busy_handler, METH_VARARGS | METH_KEYWORDS, _con_sqlite_busy_handler_doc},
    {"sqlite_busy_timeout", (PyCFunction)_con_sqlite_busy_timeout, METH_VARARGS | METH_KEYWORDS, _con_sqlite_busy_timeout_doc},
    { NULL, NULL}
};

PySQLite_MODINIT_FUNC(init_sqlite3)
{
    PyObject *module, *dict;
    PyObject* sqlite_version;
    PyObject* args;
    long tc = 0L;

    pysqlc_Type.ob_type = &PyType_Type;
    pysqlrs_Type.ob_type = &PyType_Type;

    module = Py_InitModule("_sqlite3", pysqlite_functions);

    if (!(dict = PyModule_GetDict(module)))
    {
        goto error;
    }

    required_sqlite_version = PyTuple_New(3);
    PyTuple_SetItem(required_sqlite_version, 0, PyInt_FromLong((long)2));
    PyTuple_SetItem(required_sqlite_version, 1, PyInt_FromLong((long)5));
    PyTuple_SetItem(required_sqlite_version, 2, PyInt_FromLong((long)6));

    args = PyTuple_New(0);
    sqlite_version = sqlite_version_info(NULL, args);
    Py_DECREF(args);
    if (PyObject_Compare(sqlite_version, required_sqlite_version) < 0)
    {
        Py_DECREF(sqlite_version);
        PyErr_SetString(PyExc_ImportError, "Need to be linked against SQLite 2.5.6 or higher.");
        return;
    }
    Py_DECREF(sqlite_version);

    /*** Initialize type codes */
    tc_INTEGER = PyInt_FromLong(tc++);
    tc_FLOAT = PyInt_FromLong(tc++);
    tc_TIMESTAMP = PyInt_FromLong(tc++);
    tc_DATE = PyInt_FromLong(tc++);
    tc_TIME = PyInt_FromLong(tc++);
    tc_INTERVAL = PyInt_FromLong(tc++);
    tc_STRING = PyInt_FromLong(tc++);
    tc_UNICODESTRING = PyInt_FromLong(tc++);
    tc_BINARY = PyInt_FromLong(tc++);

    PyDict_SetItemString(dict, "INTEGER", tc_INTEGER);
    PyDict_SetItemString(dict, "FLOAT", tc_FLOAT);
    PyDict_SetItemString(dict, "TIMESTAMP", tc_TIMESTAMP);
    PyDict_SetItemString(dict, "DATE", tc_DATE);
    PyDict_SetItemString(dict, "TIME", tc_TIME);
    PyDict_SetItemString(dict, "INTERVAL", tc_INTERVAL);
    PyDict_SetItemString(dict, "STRING", tc_STRING);
    PyDict_SetItemString(dict, "UNICODESTRING", tc_UNICODESTRING);
    PyDict_SetItemString(dict, "BINARY", tc_BINARY);

    /*** Create DB-API Exception hierarchy */

    _sqlite_Error = PyErr_NewException("_sqlite.Error", PyExc_StandardError, NULL);
    PyDict_SetItemString(dict, "Error", _sqlite_Error);

    _sqlite_Warning = PyErr_NewException("_sqlite.Warning", PyExc_StandardError, NULL);
    PyDict_SetItemString(dict, "Warning", _sqlite_Warning);

    /* Error subclasses */

    _sqlite_InterfaceError = PyErr_NewException("_sqlite.InterfaceError", _sqlite_Error, NULL);
    PyDict_SetItemString(dict, "InterfaceError", _sqlite_InterfaceError);

    _sqlite_DatabaseError = PyErr_NewException("_sqlite.DatabaseError", _sqlite_Error, NULL);
    PyDict_SetItemString(dict, "DatabaseError", _sqlite_DatabaseError);

    /* DatabaseError subclasses */

    _sqlite_InternalError = PyErr_NewException("_sqlite.InternalError", _sqlite_DatabaseError, NULL);
    PyDict_SetItemString(dict, "InternalError", _sqlite_InternalError);

    _sqlite_OperationalError = PyErr_NewException("_sqlite.OperationalError", _sqlite_DatabaseError, NULL);
    PyDict_SetItemString(dict, "OperationalError", _sqlite_OperationalError);

    _sqlite_ProgrammingError = PyErr_NewException("_sqlite.ProgrammingError", _sqlite_DatabaseError, NULL);
    PyDict_SetItemString(dict, "ProgrammingError", _sqlite_ProgrammingError);

    _sqlite_IntegrityError = PyErr_NewException("_sqlite.IntegrityError", _sqlite_DatabaseError,NULL);
    PyDict_SetItemString(dict, "IntegrityError", _sqlite_IntegrityError);

    _sqlite_DataError = PyErr_NewException("_sqlite.DataError", _sqlite_DatabaseError, NULL);
    PyDict_SetItemString(dict, "DataError", _sqlite_DataError);

    _sqlite_NotSupportedError = PyErr_NewException("_sqlite.NotSupportedError", _sqlite_DatabaseError, NULL);
    PyDict_SetItemString(dict, "NotSupportedError", _sqlite_NotSupportedError);

  error:

    if (PyErr_Occurred())
    {
        PyErr_SetString(PyExc_ImportError, "sqlite: init failed");
    }
}
