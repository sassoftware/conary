#
# Copyright (c) 2004-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import bdb
import bz2
import debugger
import fcntl
import errno
import log
import misc
import os
import re
import select
import shutil
import signal
import stat
import string
import StringIO
import subprocess
import sys
import tempfile
import time
import urllib
import urlparse
import weakref
import xmlrpclib
import zlib

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
        normpath = os.path.normpath(path)

        # don't die in case the dir already exists
        try:
            os.makedirs(normpath)
        except OSError, exc:
            if exc.errno == errno.EEXIST:
                if os.path.isdir(normpath):
                    continue
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

def recurseDirectoryList(topdir, withDirs=False):
    """Recursively list all files in the directory"""
    items = [topdir]
    while items:
        item = items.pop()
        if os.path.islink(item) or os.path.isfile(item):
            yield item
            continue
        # Directory
        listdir = os.listdir(item)
        # Add the contents of the directory in reverse order (we use pop(), so
        # last element in the list is the one popped out)
        listdir.sort()
        listdir.reverse()
        listdir = [ os.path.join(item, x) for x in listdir ]
        items.extend(listdir)

        if withDirs:
            # This is useful if one wants to catch empty directories
            yield item

def normurl(url):
    surl = list(urlparse.urlsplit(url))
    if surl[2] == '':
        surl[2] = '/'
    elif surl[2] != '/':
        tail = ''
        if surl[2].endswith('/'):
            tail = '/'
        surl[2] = normpath(surl[2]) + tail
    return urlparse.urlunsplit(surl)

errorMessage = '''
*******************************************************************
*** An error has occurred in conary:
***
*** %(filename)s:%(lineno)s
*** %(errtype)s: %(errmsg)s
***
*** Receiving this message is always due to a bug in conary, not
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
command, go to: 

http://wiki.rpath.com/wiki/Conary:How_To_File_An_Effective_Bug_Report

for more help on reporting issues.

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

    def excepthook(typ, value, tb):
        if typ is bdb.BdbQuit:
            sys.exit(1)
        sys.excepthook = sys.__excepthook__
        if not _debugAll and (typ == KeyboardInterrupt and not debugCtrlC):
            sys.exit(1)

        out = BoundedStringIO()
        formatTrace(typ, value, tb, stream = out, withLocals = False)
        out.write("\nFull stack:\n")
        formatTrace(typ, value, tb, stream = out, withLocals = True)
        out.seek(0)
        tbString = out.read()
        del out
        if log.syslog is not None:
            log.syslog("command failed\n%s", tbString)

        if debug or _debugAll:
            formatTrace(typ, value, tb, stream = sys.stderr,
                        withLocals = False)
            if sys.stdout.isatty() and sys.stdin.isatty():
                debugger.post_mortem(tb, typ, value)
            else:
                sys.exit(1)
        elif log.getVerbosity() is log.DEBUG:
            log.debug(tbString)
        else:
            cmd = sys.argv[0]
            if cmd.endswith('/commands/conary'):
                cmd = cmd[:len('/commands/conary')] + '/bin/conary'
            elif cmd.endswith('/commands/cvc'):
                cmd = cmd[:len('/commands/cvc')] + '/bin/cvc'
                
            origTb = tb
            cmd = normpath(cmd)
            sys.argv[0] = cmd
            while tb.tb_next: tb = tb.tb_next
            lineno = tb.tb_frame.f_lineno
            filename = tb.tb_frame.f_code.co_filename
            tmpfd, stackfile = tempfile.mkstemp('.txt', prefix)
            os.write(tmpfd, tbString)
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
    """
    similar to os.system, but raises errors if exit code != 0 and closes stdin
    so processes can never block on user input
    """
    if verbose:
        log.info(cmd)
    rc = subprocess.call(cmd, shell=True, cwd=destDir, stdin=open(os.devnull))
    # form the rc into a standard exit status
    if rc < 0:
        # turn rc positive
        rc = rc * -1
    else:
        # shift the return code into the high bits
        rc = rc << 8
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
    return re.escape(s)


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
                abortCheck = None, bufSize = 128*1024, rateLimit = None,
                sizeLimit = None, total=0):
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

    copied = 0

    if abortCheck:
        pollObj = select.poll()
        pollObj.register(source.fileno(), select.POLLIN)
    else:
        pollObj = None

    while True:
        if sizeLimit and (sizeLimit - copied < bufSize):
            bufSize = sizeLimit - copied

        if abortCheck:
            # if we need to abortCheck, make sure we check it every time
            # read returns, and every five seconds
            l = []
            while not l:
                if abortCheck():
                    return None
                l = pollObj.poll(5000)

        buf = source.read(bufSize)
        if not buf:
            break

        total += len(buf)
        copied += len(buf)
        write(buf)

        if digest:
            digest.update(buf)

        now = time.time()
        if now == starttime:
            rate = 0 # don't bother limiting download until now > starttime.
        else:
            rate = copied / ((now - starttime)) 

        if callback:
            callback(total, rate)

        if copied == sizeLimit:
            break

        if rateLimit > 0 and rate > rateLimit:
            time.sleep((copied / rateLimit) - (copied / rate))

    return copied

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
    if binary[0] == '/':
        # handle case where binary starts with / seperately 
        # because os.path.join will not do the right
        # thing with root set.
        if root:
            if os.path.exists(root + binary):
                return root + binary
        elif os.path.exists(binary):
            return binary
        return None

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

def splitPathReverse(path):
    """Split the path at the operating system's separators.
    Returns a list with the path components in reverse order.
    Empty path components are stripped out.
    Example: 'a//b//c/d' -> ['d', 'c', 'b', 'a']
    """
    while 1:
        path, tail = os.path.split(path)
        if not tail:
            break
        yield tail

def splitPath(path):
    """Split the path at the operating system's separators
    Empty path components are stripped out
    Example: 'a//b//c/d' -> ['a', 'b', 'c', 'd']
    """
    ret = list(splitPathReverse(path))
    ret.reverse()
    return ret

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

def memsize(pid = None):
    return memusage(pid = pid)[0]

def memusage(pid = None):
    """Get the memory usage.
    @param pid: Process to analyze (None for current process)
    """
    if pid is None:
        pfn = "/proc/self/statm"
    else:
        pfn = "/proc/%d/statm" % pid
    line = open(pfn).readline()
    # Assume page size is 4k (true for i386). This can be adjusted by reading
    # resource.getpagesize() 
    arr = [ 4 * int(x) for x in line.split()[:6] ]
    vmsize, vmrss, vmshared, text, lib, data = arr

    # The RHS in the following description is the fields in /proc/self/status
    # text is VmExe
    # data is VmData + VmStk
    return vmsize, vmrss, vmshared, text, lib, data

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

    @param haystack: list of tuples.
    @type haystack: list
    @param newItem: The item to be inserted
    @type newItem: tuple
    @param cmpFn: Comparison function
    @type cmpFn: function
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

def verFormat(cfg, version):
    """Format the version according to the options in the cfg object"""
    print >> sys.stderr, 'util.verFormat is obsolete as of Conary 1.2'
    if cfg.fullVersions:
        return str(version)
    if cfg.showLabels:
        ret = "%s/%s" % (version.branch().label(), version.trailingRevision())
        return ret
    # If the branch is matching the install label, don't bother to show it
    if version.branch().label() == cfg.installLabel:
        return version.trailingRevision().asString()
    return version.asString()

class ExtendedFile(file):

    def __init__(self, path, mode = "r", buffering = True):
        assert(not buffering)
        file.__init__(self, path, mode, buffering)
        fcntl.fcntl(self.fileno(), fcntl.F_SETFD, 1)

    def pread(self, bytes, offset):
        return misc.pread(self.fileno(), bytes, offset)

class ExtendedStringIO(StringIO.StringIO):
    def pread(self, bytes, offset):
        pos = self.tell()
        self.seek(offset, 0)
        data = self.read(bytes)
        self.seek(pos, 0)
        return data

class PreadWrapper(object):
    # DEPRECATED. Will be removed in 1.1.23.
    __slots__ = ('f', 'path')

    def __init__(self, f):
        self.path = None
        if not hasattr(f, 'mode'):
            if hasattr(f, 'path'):
                # this is an rMake LazyFile
                self.path = f.path
            else:
                raise ValueError('PreadWrapper does not know how to handle this file object')
        elif f.mode != 'r':
            raise ValueError('PreadWrapper.__init__() requires a read-only file object')
        self.f = f

    def __getattr__(self, attr):
        if attr != 'pread':
            return getattr(self.f, attr)
        else:
            return self.pread

    def pread(self, bytes, offset):
        if self.path:
            # hack for rMake compatibility
            f = open(self.path, 'r')
            buf = misc.pread(f.fileno(), bytes, offset)
            f.close()
            return buf
        return misc.pread(self.fileno(), bytes, offset)

class SeekableNestedFile:

    def __init__(self, file, size, start = -1):
        self.file = file
        self.size = size
        self.end = self.size
        self.pos = 0

        if start == -1:
            self.start = file.tell()
        else:
            self.start = start

    def close(self):
        pass

    def read(self, bytes = -1, offset = None):
        if offset is None:
            readPos = self.pos
        else:
            readPos = offset

	if bytes < 0 or (self.end - readPos) <= bytes:
	    # return the rest of the file
	    count = self.end - readPos
	    newPos = self.end
	else:
            count = bytes
            newPos = readPos + bytes

        buf = self.file.pread(count, readPos + self.start)

        if offset is None:
            self.pos = newPos

        return buf

    pread = read

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
        return self.pos

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

class LineReader:

    def readlines(self):
        s = os.read(self.fd, 4096)
        if not s:
            if self.buf:
                s = self.buf
                self.buf = ''
                return [ s ]

            return None

        self.buf += s

        lines = self.buf.split('\n')
        self.buf = lines[-1]
        del lines[-1]

        return [ x + "\n" for x in lines ]

    def __init__(self, fd):
        self.fd = fd
        self.buf = ''

exists = misc.exists
removeIfExists = misc.removeIfExists
pread = misc.pread

class _LazyFile(object):
    __slots__ = ['path', 'marker', 'mode', '_cache', '_hash', '_realFd',
                 '_timestamp']
    def __init__(self, cache, path, mode):
        self.path = path
        self.mode = mode
        self.marker = (0, 0)
        self._hash = cache._getCounter()
        self._cache = weakref.ref(cache, self._closeCallback)
        self._realFd = None
        self._timestamp = time.time()

    def reopen(method):
        """Decorator to perform the housekeeping of opening/closing of fds"""
        def wrapper(self, *args, **kwargs):
            if self._realFd is not None:
                # Object is already open
                # Mark it as being used
                self._timestamp = time.time()
                # Return the real method
                return getattr(self._realFd, method.func_name)(*args, **kwargs)
            if self._cache is None:
                raise Exception("Cache object is closed")
            try:
                self._cache()._getSlot()
            except ReferenceError:
                # re-raise for now, until we decide what to do
                raise
            self._reopen()
            return getattr(self._realFd, method.func_name)(*args, **kwargs)
        return wrapper

    def _reopen(self):
        # Initialize the file descriptor
        self._realFd = ExtendedFile(self.path, self.mode, buffering = False)
        self._realFd.seek(*self.marker)
        self._timestamp = time.time()

    def _release(self):
        assert self._realFd is not None, "Cannot release file descriptor"
        self._close()

    def _closeCallback(self, cache):
        """Called when the cache object gets destroyed"""
        self._close()
        self._cache = None

    @reopen
    def read(self, bytes):
        pass

    @reopen
    def pread(self, bytes, offset):
        pass

    @reopen
    def seek(self, loc, type):
        pass

    @reopen
    def trucate(self):
        pass

    def _close(self):
        # Close only the file descriptor
        if self._realFd is not None:
            self.marker = (self._realFd.tell(), 0)
            self._realFd.close()
            self._realFd = None

    def close(self):
        self._close()
        if self._cache is None:
            return
        cache = self._cache()
        if cache is not None:
            try:
                cache._closeSlot(self)
            except ReferenceError:
                # cache object is already gone
                pass
        self._cache = None

    @reopen
    def tell(self):
        pass

    def __hash__(self):
        return self._hash

    def __del__(self):
        self.close()

class LazyFileCache:
    """An object tracking open files. It will serve file-like objects that get
    closed behind the scene (and reopened on demand) if the number of open 
    files in the current process exceeds a threshold.
    The objects will close automatically when they fall out of scope.
    """
    # Assuming maxfd is 1024, this should be ok
    threshold = 900

    def __init__(self, threshold=None):
        if threshold:
            self.threshold = threshold
        # Counter used for hashing
        self._fdCounter = 0
        self._fdMap = {}
    
    def open(self, path, mode="r"):
        fd = _LazyFile(self, path, mode=mode)
        self._fdMap[fd._hash] = fd
        # Try to open the fd, to push the errors up early
        fd.tell()
        return fd

    def _getFdCount(self):
        return len(os.listdir("/proc/self/fd"))

    def _getCounter(self):
        ret = self._fdCounter;
        self._fdCounter += 1;
        return ret;

    def _getSlot(self):
        if self._getFdCount() < self.threshold:
            # We can open more file descriptors
            return
        # There are several ways we can obtain a slot if the object is full:
        # 1. free one slot
        # 2. free a batch of slots
        # 3. free all slots
        # Running tests which are not localized (i.e. walk over the list of
        # files and do some operation on them) shows that 1. is extremely
        # expensive. 2. and 3. are comparatively similar if we're freeing 10%
        # of the threshold, so that's the current implementation.

        # Sorting would be expensive for selecting just the oldest fd, but
        # when selecting the oldest m fds, performance is m * n. For m large
        # enough, log n will be smaller. For n = 5k, 10% is 500, while log n
        # is about 12. Even factoring in other sorting constants, you're still
        # winning.
        l = sorted([ x for x in self._fdMap.values() if x._realFd is not None],
                   lambda a, b: cmp(a._timestamp, b._timestamp))
        for i in range(int(self.threshold / 10)):
            l[i]._release()

    def _closeSlot(self, fd):
        del self._fdMap[fd._hash]

    def close(self):
        # No need to call fd's close(), we're destroying this object
        for fd in self._fdMap.values():
            fd._close()
            fd._cache = None
        self._fdMap.clear()

    def release(self):
        """Release the file descriptors kept open by the LazyFile objects"""
        for fd in self._fdMap.values():
            fd._close()

    __del__ = close

class Flags(object):

    # set the slots to the names of the flags to support

    __slots__ = []

    def __init__(self, **kwargs):
        for flag in self.__slots__:
            setattr(self, flag, False)

        for (flag, val) in kwargs.iteritems():
            setattr(self, flag, val)

    def __setattr__(self, flag, val):
        if type(val) != bool:
            raise TypeError, 'bool expected'
        object.__setattr__(self, flag, val)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                "".join( flag for flag in self.__slots__
                            if getattr(self, flag) ) )

def stripUserPassFromUrl(url):
    arr = list(urlparse.urlparse(url))
    hostUserPass = arr[1]
    userPass, host = urllib.splituser(hostUserPass)
    arr[1] = host
    return urlparse.urlunparse(arr)

class FileIgnoreEpipe:

    def ignoreEpipe(fn):

        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except IOError, e:
                if e.errno != errno.EPIPE:
                    raise

            return

        return wrapper

    @ignoreEpipe
    def write(self, *args):
        return self.f.write(*args)

    @ignoreEpipe
    def close(self, *args):
        return self.f.close(*args)

    def __getattr__(self, name):
        return getattr(self.f, name)

    def __init__(self, f):
        self.f = f

class BoundedStringIO(object):
    """
    An IO object that behaves like a StringIO.
    Data is stored in memory (just like in a StringIO) if shorter than
    maxMemorySize, or in a temporary file.
    """
    defaultMaxMemorySize = 65536
    __slots__ = ['_backend', '_backendType', 'maxMemorySize']
    def __init__(self, buf='', maxMemorySize=None):
        if maxMemorySize is None:
            maxMemorySize = object.__getattribute__(self, 'defaultMaxMemorySize')
        self.maxMemorySize = maxMemorySize
        # Store in memory by default
        self._backend = StringIO.StringIO(buf)
        self._backendType = "memory"

    def _writeImpl(self, s):
        backend = object.__getattribute__(self, '_backend')
        if isinstance(backend, file):
            # File backend
            return backend.write(s)
        # StringIO backend

        maxMemorySize = object.__getattribute__(self, 'maxMemorySize')

        # Save current position
        curPos = backend.tell()
        if curPos + len(s) < maxMemorySize:
            # No danger to overflow the limit
            return backend.write(s)

        fd, name = tempfile.mkstemp(suffix=".tmp", prefix="tmpBSIO")
        # Get rid of the file from the filesystem, we'll keep an open fd to it
        os.unlink(name)
        fcntl.fcntl(fd, fcntl.F_SETFD, 1)
        backendFile = os.fdopen(fd, "w+")
        # Copy the data from the current StringIO (up to the current position)
        backend.seek(0)
        backendFile.write(backend.read(curPos))
        ret = backendFile.write(s)
        self._backend = backendFile
        self._backendType = "file"
        return ret

    def _truncateImpl(self, size=None):
        if size is None:
            # Truncate to current position by default
            size = self.tell()
        backend = object.__getattribute__(self, '_backend')
        maxMemorySize = object.__getattribute__(self, 'maxMemorySize')

        if not isinstance(backend, file):
            # Memory backend
            # Truncating always reduces size, so we will not switch to a file
            # for this case
            return backend.truncate(size)

        # File backend
        if size > maxMemorySize:
            # truncating a file to a size larger than the memory limit - just
            # pass it through
            return backend.truncate(size)

        # Need to go from file to memory
        # Read data from file first
        backend.seek(0)
        backendMem = StringIO.StringIO(backend.read(size))
        self._backendType = "memory"
        self._backend = backendMem
        backend.close()

    def getBackendType(self):
        return object.__getattribute__(self, '_backendType')

    def __getattribute__(self, attr):
        # Passing calls to known local objects through
        locs = ['_backend', '_backendType', 'getBackendType', 'maxMemorySize']
        if attr in locs:
            return object.__getattribute__(self, attr)

        if attr == 'write':
            # Return the real implementation of the write method
            return object.__getattribute__(self, '_writeImpl')

        if attr == 'truncate':
            # Return the real implementation of the truncate method
            return object.__getattribute__(self, '_truncateImpl')

        backend = object.__getattribute__(self, '_backend')
        return getattr(backend, attr)

class ProtectedString(str):
    """A string that is not printed in tracebacks"""
    def __safe_str__(self):
        return "<Protected Value>"

    __repr__ = __safe_str__

class ProtectedTemplate(str):
    """A string template that hides parts of its components.
    The first argument is a template (see string.Template for a complete
    documentation). The values that can be filled in are using the format
    ${VAR} or $VAR. The keyword arguments are expanding the template.
    If one of the keyword arguments has a __safe_str__ method, its value is
    going to be hidden when this object's __safe_str__ is called."""
    def __new__(cls, templ, **kwargs):
        tmpl = string.Template(templ)
        s = str.__new__(cls, tmpl.safe_substitute(kwargs))
        s._templ = tmpl
        s._substArgs = kwargs
        return s

    def __safe_str__(self):
        nargs = {}
        for k, v in self._substArgs.iteritems():
            if hasattr(v, '__safe_str__'):
                v = "<%s>" % k.upper()
            nargs[k] = v
        return self._templ.safe_substitute(nargs)

def formatTrace(excType, excValue, tb, stream = sys.stderr, withLocals = True):
    import types
    import inspect
    import itertools
    import repr as reprmod
    class Repr(reprmod.Repr):
        def __init__(self, subsequentIndent = ""):
            reprmod.Repr.__init__(self)
            self.maxtuple = 20
            self.maxset = 160
            self.maxlist = 20
            self.maxdict = 20
            self.maxstring = 160
            self.maxother = 160

            self.maxLineLen = 160

            self.subsequentIndent = subsequentIndent
            # Pretty-print?
            self._pretty = True

        def repr_str(self, x, level):
            if hasattr(x, '__safe_str__'):
                return reprmod.Repr.repr_str(x.__safe_str__())
            return reprmod.Repr.repr_str(self, x, level)

        def _pretty_repr(self, pieces, iterLen, level):
            ret = ', '.join(pieces)
            if not self._pretty or len(ret) < self.maxLineLen:
                return ret
            padding = self.subsequentIndent + "  " * (self.maxlevel - level)
            sep = ',\n' + padding
            return '\n' + padding + sep.join(pieces)

        def _repr_iterable(self, x, level, left, right, maxiter, trail=''):
            n = len(x)
            if level <= 0 and n:
                s = '...'
            else:
                newlevel = level - 1
                repr1 = self.repr1
                pieces = [repr1(elem, newlevel) for elem in itertools.islice(x, maxiter)]
                if n > maxiter:  pieces.append('...')
                s = self._pretty_repr(pieces, n, level)
                if n == 1 and trail:  right = trail + right
            return '%s%s%s' % (left, s, right)

        def repr_dict(self, x, level):
            n = len(x)
            if n == 0: return '{}'
            if level <= 0: return '{...}'
            newlevel = level - 1
            repr1 = self.repr1
            pieces = []
            for key in itertools.islice(sorted(x), self.maxdict):
                oldPretty = self._pretty
                self._pretty = False
                keyrepr = repr1(key, newlevel)
                self._pretty = oldPretty

                oldSubsequentIndent = self.subsequentIndent
                self.subsequentIndent += ' ' * 4;
                valrepr = repr1(x[key], newlevel)
                self.subsequentIndent = oldSubsequentIndent

                pieces.append('%s: %s' % (keyrepr, valrepr))
            if n > self.maxdict: pieces.append('...')
            s = self._pretty_repr(pieces, n, level)
            return '{%s}' % (s,)


    def formatOneFrame(tb, stream):
        fileName, lineNo, funcName, text, idx = inspect.getframeinfo(tb)
        frame = tb.tb_frame
        stream.write('  File "%s", line %d, in %s\n' % 
            (fileName, lineNo, funcName))
        if text is not None:
            # If the source file is not available, we may not be able to get 
            # the line
            stream.write('    %s\n' % text[idx].strip())

    stream.write(str(excType))
    stream.write(": ")
    stream.write(str(excValue))
    stream.write("\n\n")

    tbStack = []
    while tb:
        tbStack.append(tb)
        tb = tb.tb_next

    if withLocals:
        tbStack.reverse()
        msg = "Traceback (most recent call first):\n"
    else:
        msg = "Traceback (most recent call last):\n"

    r = Repr(subsequentIndent = " " * 27)
    ignoredTypes = (types.ClassType, types.ModuleType, types.FunctionType,
                    types.TypeType)

    stream.write(msg)
    for tb in tbStack:
        formatOneFrame(tb, stream)

        if not withLocals:
            continue

        frame = tb.tb_frame
        for k, v in sorted(frame.f_locals.items()):
            if k.startswith('__') and k.endswith('__'):
                # Presumably internal data
                continue
            if isinstance(v, ignoredTypes):
                continue
            if hasattr(v, '__class__'):
                if v.__class__.__name__ == 'ModuleProxy':
                    continue
            if hasattr(v, '__safe_str__'):
                vstr = v.__safe_str__()
            else:
                vstr = r.repr(v)
            stream.write("        %15s : %s\n" % (k, vstr))
        stream.write("  %s\n\n" % ("*" * 70))

class XMLRPCMarshaller(xmlrpclib.Marshaller):
    """Marshaller for XMLRPC data"""
    dispatch = xmlrpclib.Marshaller.dispatch.copy()
    def dump_string(self, value, write, escape=xmlrpclib.escape):
        try:
            value = value.encode("ascii")
        except UnicodeError:
            sio = StringIO.StringIO()
            xmlrpclib.Binary(value).encode(sio)
            write(sio.getvalue())
            return
        return xmlrpclib.Marshaller.dump_string(self, value, write, escape)

    def dump(self, values, stream):
        write = stream.write
        if isinstance(values, xmlrpclib.Fault):
            # Fault instance
            write("<fault>\n")
            self._dump({'faultCode' : values.faultCode,
                        'faultString' : values.faultString},
                       write)
            write("</fault>\n")
        else:
            write("<params>\n")
            for v in values:
                write("<param>\n")
                self._dump(v, write)
                write("</param>\n")
            write("</params>\n")

    def dumps(self, values):
        sio = StringIO.StringIO()
        self.dump(values, sio)
        return sio.getvalue()

    def _dump(self, value, write):
        # Incorporates Patch #1070046: Marshal new-style objects like
        # InstanceType
        try:
            f = self.dispatch[type(value)]
        except KeyError:
            # check if this object can be marshalled as a structure
            try:
                value.__dict__
            except:
                raise TypeError, "cannot marshal %s objects" % type(value)
            # check if this class is a sub-class of a basic type,
            # because we don't know how to marshal these types
            # (e.g. a string sub-class)
            for type_ in type(value).__mro__:
                if type_ in self.dispatch.keys():
                    raise TypeError, "cannot marshal %s objects" % type(value)
            f = self.dispatch[InstanceType]
        f(self, value, write)

    dispatch[str] = dump_string
    dispatch[ProtectedString] = dump_string
    dispatch[ProtectedTemplate] = dump_string

class XMLRPCUnmarshaller(xmlrpclib.Unmarshaller):
    dispatch = xmlrpclib.Unmarshaller.dispatch.copy()
    def end_base64(self, data):
        value = xmlrpclib.Binary()
        value.decode(data)
        self.append(value.data)
        self._value = 0

    dispatch["base64"] = end_base64

    def _stringify(self, data):
        try:
            return data.encode("ascii")
        except UnicodeError:
            return xmlrpclib.Binary(data)

def xmlrpcGetParser():
    parser, target = xmlrpclib.getparser()
    # Use our own marshaller
    target = XMLRPCUnmarshaller()
    # Reuse the parser class as computed by xmlrpclib
    parser = parser.__class__(target)
    return parser, target

def xmlrpcDump(params, methodname=None, methodresponse=None, stream=None,
               encoding=None, allow_none=False):
    assert isinstance(params, tuple) or isinstance(params, xmlrpclib.Fault),\
           "argument must be tuple or Fault instance"
    if isinstance(params, xmlrpclib.Fault):
        methodresponse = 1
    elif methodresponse and isinstance(params, tuple):
        assert len(params) == 1, "response tuple must be a singleton"

    if not encoding:
        encoding = "utf-8"

    m = XMLRPCMarshaller(encoding, allow_none)
    if encoding != "utf-8":
        xmlheader = "<?xml version='1.0' encoding='%s'?>\n" % str(encoding)
    else:
        xmlheader = "<?xml version='1.0'?>\n" # utf-8 is default

    if stream is None:
        io = StringIO.StringIO(stream)
    else:
        io = stream

    # standard XML-RPC wrappings
    if methodname:
        if not isinstance(methodname, str):
            methodname = methodname.encode(encoding)
        io.write(xmlheader)
        io.write("<methodCall>\n")
        io.write("<methodName>%s</methodName>\n" % methodname)
        m.dump(params, io)
        io.write("</methodCall>\n")
    elif methodresponse:
        io.write(xmlheader)
        io.write("<methodResponse>\n")
        m.dump(params, io)
        io.write("</methodResponse>\n")
    else:
        # Return as-is
        m.dump(params, io)

    if stream is None:
        return io.getvalue()
    return ""

def xmlrpcLoad(stream):
    p, u = xmlrpcGetParser()
    if hasattr(stream, "read"):
        # A real stream
        while 1:
            data = stream.read(16384)
            if not data:
                break
            p.feed(data)
    else:
        # Assume it's a string
        p.feed(stream)
    p.close()
    return u.close(), u.getmethodname()


class ServerProxy(xmlrpclib.ServerProxy):

    def _request(self, methodname, params):
        # Call a method on the remote server
        request = xmlrpcDump(params, methodname,
            encoding = self.__encoding, allow_none=self.__allow_none)

        response = self.__transport.request(
            self.__host,
            self.__handler,
            request,
            verbose=self.__verbose)

        if len(response) == 1:
            response = response[0]

        return response

    def __getattr__(self, name):
        # magic method dispatcher
        if name.startswith('__'):
            raise AttributeError(name)
        #from conary.lib import log
        #log.debug('Calling %s:%s' % (self.__host.split('@')[-1], name)
        return self._createMethod(name)

    def _createMethod(self, name):
        return xmlrpclib._Method(self._request, name)

def copyStream(src, dest, length = None, bufferSize = 16384):
    """Copy from one stream to another, up to a specified length"""
    amtread = 0
    while amtread != length:
        if length is None:
            bsize = bufferSize
        else:
            bsize = min(bufferSize, length - amtread)
        buf = src.read(bsize)
        if not buf:
            break
        dest.write(buf)
        amtread += len(buf)
    return amtread

def decompressStream(src, bufferSize = 8092):
    sio = BoundedStringIO()
    z = zlib.decompressobj()
    while 1:
        buf = src.read(bufferSize)
        if not buf:
            break
        sio.write(z.decompress(buf))
    sio.write(z.flush())
    return sio

def compressStream(src, level = 5, bufferSize = 16384):
    sio = BoundedStringIO()
    z = zlib.compressobj(level)
    while 1:
        buf = src.read(bufferSize)
        if not buf:
            break
        sio.write(z.compress(buf))
    sio.write(z.flush())
    return sio

def massCloseFileDescriptors(start, unusedCount):
    """Close all file descriptors starting with start, until we hit
    unusedCount consecutive file descriptors that were already closed"""
    return misc.massCloseFileDescriptors(start, unusedCount, 0);

def nullifyFileDescriptor(fdesc):
    """Connects the file descriptor to /dev/null or an open file (if /dev/null
    does not exist)"""
    try:
        fd = os.open('/dev/null', os.O_RDONLY)
    except OSError:
        # in case /dev/null does not exist
        fd, fn = tempfile.mkstemp()
        os.unlink(fn)
    if fd != fdesc:
        os.dup2(fd, fdesc)
        os.close(fd)

class Timer:

    def start(self):
        self.started = time.time()

    def stop(self):
        self.total += (time.time() - self.started)
        self.started = None

    def get(self):
        if self.started:
            running = time.time() - self.started
        else:
            running = 0

        return self.total + running

    def __init__(self, start = False):
        self.started = None
        self.total = 0
        if start:
            self.start()
