#include <Python.h>

void numericstreaminit(PyObject * m);
void streamsetinit(PyObject * m);
void stringstreaminit(PyObject * m);

extern PyTypeObject StringStreamType;
extern PyTypeObject NumericStreamType;
extern PyTypeObject IntStreamType;
extern PyTypeObject ShortStreamType;
extern PyTypeObject StreamSetType;

#define STRING_STREAM	    0
#define NUMERIC_STREAM	    1
#define INT_STREAM	    2
#define SHORT_STREAM	    3
#define STREAM_SET	    4

struct singleStream {
    PyTypeObject pyType;
};

extern struct singleStream allStreams[5];
