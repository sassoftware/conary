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
import string

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


# string extensions

def find(s, subs, start=0):
    ret = -1
    found = None
    for sub in subs:
	this = string.find(s, sub, start)
	if this > -1 and ( ret < 0 or this < ret):
	    ret = this
	    found = s[this:this+1]
    return (ret, found)


# shutil module extensions, with {}-expansion and globbing

def braceExpand(path):
    obrace = string.find(path, "{")
    if obrace < 0:
	return [path]

    level=1
    pathlist = []
    h = obrace
    while level:
	(h, it) = find(path, "{}", h)
	if h < 0:
	    raise ValueError, 'path %s has unbalanced {}' %path
	if it == "{":
	    level = level + 1
	    obrace = h
	else:
	    segments = string.split(path[obrace+1:h], ',')
	    start = path[:obrace]
	    end = path[h+1:]
	    for segment in segments:
		newbits = braceExpand(start+segment+end)
		for bit in newbits:
		    if not bit in pathlist:
			pathlist.append(bit)
	    return pathlist
	h = h + 1

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
