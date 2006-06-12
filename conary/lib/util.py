#
# Copyright (c) 2004-2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import bdb
import bz2
import debugger
import errno
import log
import misc
import os
import select
import shutil
import signal
import stat
import string
import sys
import tempfile
import time
import traceback
import weakref

from conary.lib import fixedglob, log

# Simple ease-of-use extensions to python libraries

def normpath(path):
    s = os.path.normpath(path)
    if s.startswith(os.sep + os.sep):
	return s[1:]
    return s

def realpath(path):
    # returns the real path of a file, if and only if it is not a symbolic
    # link
    if not os.path.exists(path):
        return path
    if stat.S_ISLNK(os.lstat(path)[stat.ST_MODE]):
        return path
    return os.path.realpath(path)

def isregular(path):
    return stat.S_ISREG(os.lstat(path)[stat.ST_MODE])

def mkdirChain(*paths):
    for path in paths:
        if path[0] != os.sep:
            path = os.getcwd() + os.sep + path
            
        paths = path.split(os.sep)
            
        for n in (range(2,len(paths) + 1)):
            p = string.join(paths[0:n], os.sep)
            if not os.path.exists(p):
                # don't die in case of the race condition where someone
                # made the directory after we stat'ed for it.
                try:
                    os.mkdir(p)
                except OSError, exc:
                    if exc.errno == errno.EEXIST:
                        s = os.lstat(p)
                        if stat.S_ISDIR(s.st_mode):
                            pass
                        else:
                            raise
                    else:
                        raise

def _searchVisit(arg, dirname, names):
    file = arg[0]
    path = arg[1]
    testname = '%s%s%s' %(dirname, os.sep, file)
    if file in names:
	path[0] = testname
	del names

def searchPath(file, basepath):
    path = [ None ]
    # XXX replace with os.walk in python 2.3, to cut short properly
    os.path.walk(basepath, _searchVisit, (file, path))
    return path[0]

def searchFile(file, searchdirs, error=None):
    for dir in searchdirs:
        s = "%s%s%s" %(dir, os.sep, file)
        if os.path.exists(s):
            return s
    if error:
        raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT))
    return None

def findFile(file, searchdirs):
    return searchFile(file, searchdirs, error=1)

errorMessage = '''
*******************************************************************
*** An error has occurred in conary:
***
*** %(filename)s:%(lineno)s
*** %(errtype)s: %(errmsg)s
***
*** Receiving this message is always a due to a bug in conary, not
*** user error.
***
*** The related traceback has been output to %(stackfile)s
***
*** To report this error, please run the following script:
***
*** conary-debug "%(command)s"
***
*** You can attach the resulting archive to a bug report at
*** http://issues.rpath.com/.
***
*******************************************************************

For more information, or if you have trouble with the conary-debug
command, go to http://wiki.conary.com/HowToReportProblems for more
help.

To get a debug prompt, rerun this command with --debug-all
'''
_debugAll = False

def genExcepthook(debug=True,
                  debugCtrlC=False, prefix='conary-error-',
                  catchSIGUSR1=True, error=errorMessage):
    def SIGUSR1Handler(signum, frame):
        global _debugAll
        _debugAll = True
        print >>sys.stderr, '<Turning on KeyboardInterrupt catching>'

    def excepthook(type, value, tb):
        if type is bdb.BdbQuit:
            sys.exit(1)
        sys.excepthook = sys.__excepthook__
        if not _debugAll and (type == KeyboardInterrupt and not debugCtrlC):
            sys.exit(1)

        lines = traceback.format_exception(type, value, tb)
        if log.syslog is not None:
            log.syslog.traceback(lines)

        if debug or _debugAll:
            sys.stderr.write(string.joinfields(lines, ""))
            if sys.stdout.isatty() and sys.stdin.isatty():
                debugger.post_mortem(tb, type, value)
            else:
                sys.exit(1)
        elif log.getVerbosity() is log.DEBUG:
            log.debug(''.join(lines))
        else:
            cmd = sys.argv[0]
            if cmd.endswith('/commands/conary'):
                cmd = cmd[:len('/commands/conary')] + '/bin/conary'
            elif cmd.endswith('/commands/cvc'):
                cmd = cmd[:len('/commands/cvc')] + '/bin/cvc'
                
            cmd = normpath(cmd)
            sys.argv[0] = cmd
            while tb.tb_next: tb = tb.tb_next
            lineno = tb.tb_frame.f_lineno
            filename = tb.tb_frame.f_code.co_filename
            tmpfd, stackfile = tempfile.mkstemp('.txt', prefix)
            os.write(tmpfd, ''.join(lines))
            os.close(tmpfd)
            sys.stderr.write(error % dict(command=' '.join(sys.argv),
                                                 filename=filename,
                                                 lineno=lineno,
                                                 errtype=type.__name__,
                                                 errmsg=value,
                                                 stackfile=stackfile))

    if catchSIGUSR1:
        signal.signal(signal.SIGUSR1, SIGUSR1Handler)
    return excepthook



def _handle_rc(rc, cmd):
    if rc:
	if not os.WIFEXITED(rc):
	    info = 'Shell command "%s" killed with signal %d' \
		    %(cmd, os.WTERMSIG(rc))
	if os.WEXITSTATUS(rc):
	    info = 'Shell command "%s" exited with exit code %d' \
		    %(cmd, os.WEXITSTATUS(rc))
        log.error(info)
	raise RuntimeError, info

def execute(cmd, destDir=None, verbose=True):
    if verbose:
	log.info(cmd)
    if destDir:
	rc = os.system('cd %s; %s' %(destDir, cmd))
    else:
	rc = os.system(cmd)
    _handle_rc(rc, cmd)

class popen:
    """
    Version of popen() that throws errors on close(), unlike os.popen()
    """
    # unfortunately, can't derive from os.popen.  Add methods as necessary.
    def __init__(self, *args):
	self.p = os.popen(*args)
        self.write = self.p.write
        self.read = self.p.read
        self.readline = self.p.readline
        self.readlines = self.p.readlines
        self.writelines = self.p.writelines

    def close(self, *args):
	rc = self.p.close(*args)
	_handle_rc(rc, self.p.name)
        return rc

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

def literalRegex(s):
    "escape all regex magic characters in s"
    l = []
    for character in s:
        if character in '+*[].&^$+{}\\':
            l.append('\\')
        l.append(character)
    return ''.join(l)


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
	    segments = path[obrace+1:h].split(',')
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
	pathlist.extend(fixedglob.glob(path))
    return pathlist

def rmtree(paths, ignore_errors=False, onerror=None):
    for path in braceGlob(paths):
	log.debug('deleting [tree] %s', path)
	# act more like rm -rf -- allow files, too
	if (os.path.islink(path) or 
                (os.path.exists(path) and not os.path.isdir(path))):
	    os.remove(path)
	else:
	    os.path.walk(path, _permsVisit, None)
	    shutil.rmtree(path, ignore_errors, onerror)

def _permsVisit(arg, dirname, names):
    for name in names:
	path = dirname + os.sep + name
	mode = os.lstat(path)[stat.ST_MODE]
	# has to be executable to cd, readable to list, writeable to delete
	if stat.S_ISDIR(mode) and (mode & 0700) != 0700:
	    log.warning("working around illegal mode 0%o at %s", mode, path)
	    mode |= 0700
	    os.chmod(path, mode)

def remove(paths, quiet=False):
    for path in braceGlob(paths):
	if os.path.isdir(path) and not os.path.islink(path):
	    log.warning('Not removing directory %s', path)
	elif os.path.exists(path) or os.path.islink(path):
            if not quiet:
                log.debug('deleting [file] %s', path)
	    os.remove(path)
	else:
	    log.warning('file %s does not exist when attempting to delete [file]', path)

def copyfile(sources, dest, verbose=True):
    for source in braceGlob(sources):
	if verbose:
	    log.info('copying %s to %s', source, dest)
	shutil.copy2(source, dest)

def copyfileobj(source, dest, callback = None, digest = None,
                abortCheck = None, bufSize = 128*1024, rateLimit = None):

    if hasattr(dest, 'send'):
        write = dest.send
    else:
        write = dest.write

    if rateLimit is None:
        rateLimit = 0

    if not rateLimit == 0:
        if rateLimit < 8 * 1024:
            bufSize = 4 * 1024
        else:
            bufSize = 8 * 1024

        rateLimit = float(rateLimit)

    starttime = time.time()

    total = 0
    buf = source.read(bufSize)

    if abortCheck:
        sourceFd = source.fileno()
    else:
        sourceFd = None

    while True:
        if not buf:
            break

        total += len(buf)
        write(buf)

        now = time.time()
        if now == starttime:
            rate = 0 # don't bother limiting download until now > starttime.
        else:
            rate = total / ((now - starttime)) 

        if rateLimit > 0 and rate > rateLimit:
            time.sleep((total / rateLimit) - (total / rate))

        if digest: digest.update(buf)
        if callback:
            callback(total, rate)

        if abortCheck:
            # if we need to abortCheck, make sure we check it every time
            # read returns, and every five seconds
            l1 = []
            while not l1:
                if abortCheck and abortCheck():
                    return None
                l1, l2, l3 = select.select([ sourceFd ], [], [], 5)
        buf = source.read(bufSize)

    return total

def rename(sources, dest):
    for source in braceGlob(sources):
	log.debug('renaming %s to %s', source, dest)
	os.rename(source, dest)

def _copyVisit(arg, dirname, names):
    sourcelist = arg[0]
    sourcelen = arg[1]
    dest = arg[2]
    filemode = arg[3]
    dirmode = arg[4]
    if dirmode:
	os.chmod(dirname, dirmode)
    for name in names:
	if filemode:
	    os.chmod(dirname+os.sep+name, filemode)
	sourcelist.append(os.path.normpath(
	    dest + os.sep + dirname[sourcelen:] + os.sep + name))

def copytree(sources, dest, symlinks=False, filemode=None, dirmode=None):
    """
    Copies tree(s) from sources to dest, returning a list of
    the filenames that it has written.
    """
    sourcelist = []
    for source in braceGlob(sources):
	if os.path.isdir(source):
	    if source[-1] == '/':
		source = source[:-1]
	    thisdest = '%s%s%s' %(dest, os.sep, os.path.basename(source))
	    log.debug('copying [tree] %s to %s', source, thisdest)
	    shutil.copytree(source, thisdest, symlinks)
	    if dirmode:
		os.chmod(thisdest, dirmode)
	    os.path.walk(source, _copyVisit,
			 (sourcelist, len(source), thisdest, filemode, dirmode))
	else:
	    log.debug('copying [file] %s to %s', source, dest)
	    shutil.copy2(source, dest)
	    if dest.endswith(os.sep):
		thisdest = dest + os.sep + os.path.basename(source)
	    else:
		thisdest = dest
	    if filemode:
		os.chmod(thisdest, filemode)
	    sourcelist.append(thisdest)
    return sourcelist

def checkPath(binary, root=None):
    """
    Examine $PATH to determine if a binary exists, returns full pathname
    if it exists; otherwise None.
    """
    path = os.environ.get('PATH', '')
    for path in path.split(os.pathsep):
        if root:
            path = joinPaths(root, path)
        candidate = os.path.join(path, binary)
        if os.access(candidate, os.X_OK):
            if root:
                return candidate[len(root):]
            return candidate
    return None

def joinPaths(*args):
    return normpath(os.sep.join(args))

def assertIteratorAtEnd(iter):
    try:
	iter.next()
	raise AssertionError
    except StopIteration:
	return True

ref = weakref.ref
class ObjectCache(dict):
    """
    Implements a cache of arbitrary (hashable) objects where an object
    can be looked up and have its cached value retrieved. This allows
    a single copy of immutable objects to be kept in memory.
    """
    def __init__(self, *args):
        dict.__init__(self, *args)

        def remove(k, selfref=ref(self)):
            self = selfref()
            if self is not None:
                return dict.__delitem__(self, k)
        self._remove = remove

    def __setitem__(self, key, value):
        return dict.__setitem__(self, ref(key, self._remove), ref(value))

    def __contains__(self, key):
        return dict.__contains__(self, ref(key))

    def has_key(self, key):
        return key in self

    def __delitem__(self, key):
        return dict.__delitem__(self, ref(key))

    def __getitem__(self, key):
        return dict.__getitem__(self, ref(key))()

    def setdefault(self, key, value):
        return dict.setdefault(self, ref(key, self._remove), ref(value))()

def memsize():
    pfn = "/proc/%d/status" % os.getpid()
    lines = open(pfn).readlines()
    f = lines[10].split()
    return int(f[1])

def createLink(src, to):
    name = os.path.basename(to)
    path = os.path.dirname(to)
    mkdirChain(path)
    tmpfd, tmpname = tempfile.mkstemp(name, '.ct', path)
    os.close(tmpfd)
    os.remove(tmpname)
    os.link(src, tmpname)
    os.rename(tmpname, to)

def tupleListBsearchInsert(haystack, newItem, cmpFn):
    """
    Inserts newItem into haystack, maintaining the sorted order. The
    cmpIdx is the item number in the list of tuples to base comparisons on.
    Duplicates items aren't added.

    @type l: list of tuples
    @type cmpIdx: int
    @type needle: object
    @type newItem: tuple
    """
    start = 0
    finish = len(haystack) - 1
    while start < finish:
        i = (start + finish) / 2

        rc = cmpFn(haystack[i], newItem)
        if rc == 0:
            start = i
            finish = i
            break
        elif rc < 0:
            start = i + 1
        else:
            finish = i - 1

    if start >= len(haystack):
        haystack.append(newItem)
    else:
        rc = cmpFn(haystack[start], newItem)
        if rc < 0:
            haystack.insert(start + 1, newItem)
        elif rc > 0:
            haystack.insert(start, newItem)

_tempdir = tempfile.gettempdir()
def settempdir(tempdir):
    # XXX add locking if we ever go multi-threadded
    global _tempdir
    _tempdir = tempdir

def mkstemp(suffix="", prefix=tempfile.template, dir=None, text=False):
    """
    a wrapper for tempfile.mkstemp that uses a common prefix which
    is set through settempdir()
    """
    if dir is None:
        global _tempdir
        dir = _tempdir
    return tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=dir, text=text)

class NestedFile:

    def close(self):
	pass

    def read(self, bytes = -1):
        if self.needsSeek:
            self.file.seek(self.pos + self.start, 0)
            self.needsSeek = False

	if bytes < 0 or (self.end - self.pos) <= bytes:
	    # return the rest of the file
	    count = self.end - self.pos
	    self.pos = self.end
	    return self.file.read(count)
	else:
	    self.pos = self.pos + bytes
	    return self.file.read(bytes)

    def __init__(self, file, size):
	self.file = file
	self.size = size
	self.end = self.size
	self.pos = 0
        self.start = 0
        self.needsSeek = False

class SeekableNestedFile(NestedFile):

    def __init__(self, file, size, start = -1):
        NestedFile.__init__(self, file, size)

        if start == -1:
            self.start = file.tell()
        else:
            self.start = start

        self.needsSeek = True

    def read(self, bytes = -1):
        self.needsSeek = True
        return NestedFile.read(self, bytes)

    def seek(self, offset, whence = 0):
        if whence == 0:
            newPos = offset
        elif whence == 1:
            newPos = self.pos + offset
        else:
            newPos = self.size + offset
            
        if newPos > self.size or newPos < 0:
            raise IOError
        
        self.pos = newPos
        self.needsSeek = True

    def tell(self):
        return self.pos

class BZ2File:
    def __init__(self, fobj):
        self.decomp = bz2.BZ2Decompressor()
        self.fobj = fobj
        self.leftover = ''

    def read(self, bytes):
        while 1:
            buf = self.fobj.read(2048)
            if not buf:
                # ran out of compressed input
                if self.leftover:
                    # we have some uncompressed stuff left, return
                    # it
                    rc = self.leftover[:]
                    self.leftover = None
                    return rc
                # done returning all data, return None as the EOF
                return None
            # decompressed the newly read compressed data
            self.leftover += self.decomp.decompress(buf)
            # if we have at least what the caller asked for, return it
            if len(self.leftover) > bytes:
                rc = self.leftover[:bytes]
                self.leftover = self.leftover[bytes:]
                return rc
            # read some more data and try to get enough uncompressed
            # data to return

class PeekIterator:

    def _next(self):
        try:
            self.val = self.iter.next()
        except StopIteration:
            self.done = True

    def peek(self):
        if self.done:
            raise StopIteration

        return self.val

    def next(self):
        if self.done:
            raise StopIteration

        val = self.val
        self._next()
        return val

    def __iter__(self):
        while True:
            yield self.next()

    def __init__(self, iter):
        self.done = False
        self.iter = iter
        self._next()

class IterableQueue:

    def add(self, item):
        self.l.append(item)

    def __iter__(self):
        while self.l:
            yield self.l.pop(0)

        raise StopIteration

    def __init__(self):
        self.l = []

def lstat(path):
    """
    Return None if the path doesn't exist.
    """
    if not misc.exists(path):
        return None

    try:
        sb = os.lstat(path)
    except OSError, e:
        if e.errno != errno.ENOENT:
            raise
        return None

    return sb

exists = misc.exists
removeIfExists = misc.removeIfExists
