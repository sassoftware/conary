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
import glob
import shutil

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
    #if type is exceptions.IOError:
        #sys.stderr.write('%s\n' % value)
        #sys.exit(1)
    sys.excepthook = sys.__excepthook__
    lines = traceback.format_exception(type, value, tb)
    print string.joinfields(lines, "")
    pdb.post_mortem(tb)

def execute(cmd):
    print '+', cmd
    rc = os.system(cmd)
    if rc:
	if not os.WIFEXITED(rc):
	    info = 'Shell command "%s" killed with signal %d' \
		    %(cmd, os.WTERMSIG(rc))
	if os.WEXITSTATUS(rc):
	    info = 'Shell command "%s" exited with exit code %d' \
		    %(cmd, os.WEXITSTATUS(rc))
	raise RuntimeError, info



# shutil module extensions, with {}-expansion and globbing

# XXX -- this does not do {{}} nested {}-expansion -- we should
# just write a C extension that simply exports GLOB_BRACE and
# use it instead.
def braceExpand(path):
    obrace = string.find(path, "{")
    if obrace < 0:
	return [path]
    start = path[0:obrace]
    cbrace = string.find(path, "}")
    if cbrace < 0:
	raise ValueError, 'path %s has unbalanced {}' %path
    segments = string.split(path[obrace+1:cbrace], ',')
    end = path[cbrace+1:]
    pathlist = []
    for segment in segments:
	pathlist.extend(braceExpand(start+segment+end))
    return pathlist

def braceGlob(paths):
    pathlist = []
    for path in braceExpand(paths):
	pathlist.extend(glob.glob(path))
    return pathlist

def rmtree(paths, ignore_errors=False, onerror=None):
    for path in braceGlob(paths):
	print '+ deleting [tree] %s' %path
	shutil.rmtree(path, ignore_errors, onerror)

def remove(paths):
    for path in braceGlob(paths):
	print '+ deleting [file] %s' %path
	os.remove(path)

def copyfile(source, dest):
    print '+ copying %s to %s' %(source, dest)
    shutil.copyfile(source, dest)

def rename(source, dest):
    print '+ renaming %s to %s' %(source, dest)
    os.rename(source, dest)
