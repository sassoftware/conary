#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import os
import string
import errno
import sys
import traceback
import pdb
import exceptions

def mkdirChain(*paths):
    for path in paths:
        if path[0] != "/":
            path = os.getcwd() + "/" + path
            
        paths = string.split(path, "/")
            
        for n in (range(2,len(paths) + 1)):
            p = string.join(paths[0:n], "/")
            if not os.path.exists(p):
                os.mkdir(p)

def _searchVisit(arg, dirname, names):
    file = arg[0]
    path = arg[1]
    testname = '%s/%s' %(dirname, file)
    if os.path.exists(testname):
	path[0] = testname
	del names

def searchPath(file, basepath):
    path = [ None ]
    # XXX replace with os.walk in python 2.3, to cut short properly
    os.path.walk(basepath, _searchVisit, (file, path))
    return path[0]

def searchFile(file, searchdirs, error=None):
    for dir in searchdirs:
        s = "%s/%s" %(dir, file)
        if os.path.exists(s):
            return s
    if error:
        raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT))
    return None

def findFile(file, searchdirs):
    return searchFile(file, searchdirs, error=1)

def excepthook(type, value, tb):
    if type is exceptions.IOError:
        sys.stderr.write('%s\n' % value)
        sys.exit(1)
    sys.excepthook = sys.__excepthook__
    lines = traceback.format_exception(type, value, tb)
    print string.joinfields(lines, "")
    pdb.post_mortem(tb)

def execute(cmd):
    print '+', cmd
    rc = os.system(cmd)
    if rc:
	raise RuntimeError, ('Shell command "%s" returned '
	                     'non-zero status %d' % (cmd, rc))
