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

def mkdirChain(*paths):
    for path in paths:
        if path[0] != "/":
            path = os.getcwd() + "/" + path
            
        paths = string.split(path, "/")
            
        for n in (range(2,len(paths) + 1)):
            p = string.join(paths[0:n], "/")
            if not os.path.exists(p):
                os.mkdir(p)

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
    sys.excepthook = sys.__excepthook__
    lines = traceback.format_exception(type, value, tb)
    print string.joinfields(lines, "")
    pdb.post_mortem(tb)
