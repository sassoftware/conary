#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#


""" Extended pdb """
import stackutil
import pdb
import os
import re
try:
    import rlcompleter
    import readline
except ImportError:
    hasReadline = False
else:
    hasReadline = True
import socket
import string
import sys
import tempfile
import traceback

class Epdb(pdb.Pdb):
    # epdb will print to here instead of to sys.stdout,
    # and restore stdout when done
    __old_stdout = None
    # used to track the number of times a set_trace has been seen
    trace_counts = {}

    def __init__(self):
        self._exc_type = None
        self._exc_msg = None
        self._tb = None
        self._config = {}
        pdb.Pdb.__init__(self)
        if hasReadline:
            self._completer = rlcompleter.Completer()
        self.prompt = '(Epdb) '
    
    def do_savestack(self, path):
        
        if 'stack' in self.__dict__:
            # when we're saving we always 
            # start from the top
            frame = self.stack[-1][0]
        else:
            frame = sys._getframe(1)
            while frame.f_globals['__name__'] in ('epdb', 'pdb', 'bdb', 'cmd'):
                frame = frame.f_back
        if path == "":
            (tbfd,path) = tempfile.mkstemp('', 'conary-stack-')
            output = os.fdopen(tbfd, 'w')
        else:
            output = open(path, 'w')
        stackutil.printStack(frame, output)
        print "Stack saved to %s" % path

    def do_mailstack(self, arg):
        tolist = arg.split()
        subject = '[Conary Stacktrace]'
        if 'stack' in self.__dict__:
            # when we're saving we always 
            # start from the top
            frame = self.stack[-1][0]
        else:
            frame = sys._getframe(1)
            while frame.f_globals['__name__'] in ('epdb', 'pdb', 'bdb', 'cmd'):
                frame = frame.f_back
        sender = os.environ['USER']
        host = socket.getfqdn()
        extracontent = None
        if self._tb:
            lines = traceback.format_exception(self._exc_type, self._exc_msg, 
                                               self._tb)
            extracontent = string.joinfields(lines, "")
        stackutil.mailStack(frame, tolist, sender + '@' + host, subject,
                            extracontent)
        print "Mailed stack to %s" % tolist


    def do_printstack(self, arg):
        if 'stack' in self.__dict__:
            # print only the stack up to our current depth
            frame = self.stack[-1][0]
        else:
            frame = sys._getframe(1)
            while frame.f_globals['__name__'] in ('epdb', 'pdb', 'bdb', 'cmd'):
                frame = frame.f_back
        stackutil.printStack(frame, sys.stderr)

    

    def do_printframe(self, arg):
        if not arg:
            if 'stack' in self.__dict__:
                depth = self.curindex
            else:
                depth = 0
        else:
            depth = int(arg)
            if 'stack' in self.__dict__:
                # start at -1 (top) and go down...
                depth = 0 - (depth + 1)
        if 'stack' in self.__dict__:
            print "Depth = %d" % depth
            frame = self.stack[depth][0]
        else:
            frame = sys._getframe(1)
            while frame.f_globals['__name__'] in ('epdb', 'pdb', 'bdb', 'cmd'):
                frame = frame.f_back
            for i in xrange(0, depth):
                frame = frame.f_back
        stackutil.printFrame(frame, sys.stderr)

    def do_file(self, arg):
        frame, lineno = self.stack[self.curindex]
        filename = self.canonic(frame.f_code.co_filename)
        print "%s:%s" % (filename, lineno) 
    do_f = do_file

    def do_set(self, arg):
        if not arg:
            keys = self._config.keys()
            keys.sort()
            for key in keys:
                print "%s: %s" % (key, self._config[key])
        else:
            args = arg.split(None, 1)
            if len(args) == 1:
                key = args[0]
                if key in self._config:
                    print "Removing %s: %s" % (key, self._config[key])
                    del self._config[key]
                else:
                    print "%s: Not set" % (key)
            else:
                key, value = args
                if(hasattr(self, 'set_' + key)):
                    fn = getattr(self, 'set_' + key)
                    fn(value)
                else:
                    print "No such config value"

    def set_path(self, paths):
        paths = paths.split(' ')
        for path in paths:
            if path[0] != '/':
                print "must give absolute path"
            if not os.path.exists(path):
                print "Path %s does not exist" % path
            if path[-1] == '/':
                path = path[:-1]
            path = os.path.realpath(path)
            if 'path' not in self._config:
                self._config['path'] = []
            self._config['path'].append(path)
        print "Set path to %s" % self._config['path']

    def do_list(self, arg):
        rel = re.compile(r'^[-+] *[0-9]* *$')
        if arg and arg == '.':
            self.lineno = None
            pdb.Pdb.do_list(self, '')
            return
        if rel.match(arg):
            if arg == '-':
                reldist = -7
            else:
                reldist = int(arg)
            if self.lineno is None:
                lineno = 0
            else:
                lineno = self.lineno
            lineno += reldist - 5
            pdb.Pdb.do_list(self, str(lineno))
            self.lastcmd = 'list ' + arg
        else:
            pdb.Pdb.do_list(self, arg)

    do_l = do_list

    def interaction(self, frame, traceback):
        pdb.Pdb.interaction(self, frame, traceback)
        if not self.__old_stdout is None:
            sys.stdout.flush()
            # now we reset stdout to be the whatever it was before
            os.dup2(self.__old_stdout, sys.stdout.fileno())

    def switch_stdout(self):
        if not os.isatty(sys.stdout.fileno()):
            sys.stdout.flush()
            # old_stdout points to whereever stdout was 
            # when called (maybe to file?)
            self.__old_stdout = os.dup(sys.stdout.fileno())
            # now we copy whatever te proxy points to to 1
            os.dup2(os.open('/dev/tty', os.O_WRONLY), sys.stdout.fileno())
        return

    # override for cases where we want to search a different
    # path for the file
    def canonic(self, filename):
        canonic = self.fncache.get(filename)
        if not canonic or not os.path.exists(canonic):
            canonic = os.path.abspath(filename)
            canonic = os.path.normcase(canonic)
            if not os.path.exists(canonic):
                if 'path' in self._config:
                    for path in self._config['path']:
                        pos = matchFileOnDirPath(path, canonic)
                        if pos:
                            canonic = pos
                            break
                self.fncache[filename] = canonic
        return canonic

    def reset_trace_count(klass, marker='default'):
        tc = klass.trace_counts
        try:
            tc[marker][1] = 0
        except KeyError:
            pass
    reset_trace_count = classmethod(reset_trace_count)

    def set_trace_cond(klass, cond=None, marker='default'):
        """ Sets a condition for set_trace statements that have the 
            specified marker.  A condition can either callable, in
            which case it should take one argument, which is the 
            number of times set_trace(marker) has been called,
            or it can be a number, in which case the break will
            only be called.
        """
        tc = klass.trace_counts
        try:
            curVals = tc[marker]
        except KeyError:
            curVals = [ None, 0 ]
        curVals[0] = cond
        tc[marker] = curVals
    set_trace_cond = classmethod(set_trace_cond)

    def set_trace(self, marker='default', skip=0):
        tc = Epdb.trace_counts
        try:
            (cond, curCount) = tc[marker]
            curCount += 1
        except KeyError:
            (cond, curCount) = None, 1
        if cond is None:
            rv = True
        else:
            try:
                rv = cond(curCount)
            except TypeError:
                # assume that if the condition 
                # is not callable, it is an 
                # integer above which we are 
                # supposed to break
                rv = curCount >= cond
        if rv:
            if marker != 'default':
                self.prompt = '(Epdb [%s]) ' % marker
            self._set_trace(skip=skip+1)
        tc[marker] = [cond, curCount]

    def _set_trace(self, skip=0):
        """Start debugging from here."""
        frame = sys._getframe().f_back
        # go up the specified number of frames
        for i in range(0,skip):
            frame = frame.f_back
        self.reset()
        while frame:
            frame.f_trace = self.trace_dispatch
            self.botframe = frame
            frame = frame.f_back
        self.set_step()
        sys.settrace(self.trace_dispatch)

    # bdb hooks
    def user_call(self, frame, argument_list):
        """This method is called when there is the remote possibility
        that we ever need to stop in this function."""
        if self.stop_here(frame):
            self.switch_stdout()
            pdb.Pdb.user_call(self, frame, argument_list)

    def user_line(self, frame):
        """This function is called when we stop or break at this line."""
        self.switch_stdout()
        pdb.Pdb.user_line(self, frame)

    def user_return(self, frame, return_value):
        """This function is called when a return trap is set here."""
        self.switch_stdout()
        pdb.Pdb.user_return(self, frame, return_value)

    def user_exception(self, frame, exc_info):
        """This function is called if an exception occurs,
        but only if we are to stop at or just below this level."""
        self.switch_stdout()
        pdb.Pdb.user_exception(self, frame, exc_info)


    def complete(self, text, state):
        if hasReadline:
            # from cmd.py, override completion to match on local variables
            allvars = {}
            globals = self.curframe.f_globals.copy()
            locals = self.curframe.f_locals.copy()
            allvars.update(globals)
            allvars.update(locals)
            self._completer.namespace = allvars
            self._completer.use_main_ns = 0
            matches = self._completer.complete(text, state)
            return matches
        else:
            return pdb.Pdb.complete(self, text, state)
        
def beingTraced():
    frame = sys._getframe(0)
    while frame:
        if not frame.f_trace is None:
            return True
        frame = frame.f_back
    return False

def set_trace_cond(cond=None, marker='default'):
    """ Sets a condition for set_trace statements that have the 
        specified marker.  A condition can either callable, in
        which case it should take one argument, which is the 
        number of times set_trace(marker) has been called,
        or it can be a number, in which case the break will
        only be called.
    """
    Epdb.set_trace_cond(cond, marker)

def reset_trace_count(marker='default'):
    """ Resets the number a set_trace for a marker has been 
        seen to 0. """
    Epdb.reset_trace_count(marker)

def set_trace(marker='default'):
    """ Starts the debugger at the current location.  Takes an
        optional argument 'marker' (default 'default'), that 
        can be used with the set_trace_cond function to support 
        turning on and off tracepoints based on conditionals
    """

    Epdb().set_trace(marker=marker, skip=1)

def post_mortem(t, exc_type=None, exc_msg=None):
    p = Epdb()
    p._exc_type = exc_type
    p._exc_msg = exc_msg
    p._tb = t
    p.reset()
    while t.tb_next is not None:
        t = t.tb_next
    p.interaction(t.tb_frame, t)

def matchFileOnDirPath(curpath, pathdir):
    """Find match for a file by slicing away its directory elements
       from the front and replacing them with pathdir.  Assume that the
       end of curpath is right and but that the beginning may contain
       some garbage (or it may be short)
       Overlaps are allowed:
       e.g /tmp/fdjsklf/real/path/elements, /all/the/real/ =>
       /all/the/real/path/elements (assuming that this combined
       path exists)
    """
    if os.path.exists(curpath):
        return curpath
    filedirs = curpath.split('/')[1:]
    filename = filedirs[-1]
    filedirs = filedirs[:-1]
    if pathdir[-1] == '/':
        pathdir = pathdir[:-1]
    # assume absolute paths
    pathdirs = pathdir.split('/')[1:]
    lp = len(pathdirs)
    # Cut off matching file elements from the ends of the two paths
    for x in range(1, min(len(filedirs, pathdirs))):
        # XXX this will not work if you have 
        # /usr/foo/foo/filename.py
        if filedirs[-1] == pathdirs[-x]:
            filedirs = filedirs[:-1]
        else:
            break

    # Now cut try cuting off incorrect initial elements of curpath
    while filedirs:
        tmppath = '/' + '/'.join(pathdirs + filedirs + [filename]) 
        if os.path.exists(tmppath):
            return tmppath
        filedirs = filedirs[1:]
    tmppath = '/' + '/'.join(pathdirs + [filename])
    if os.path.exists(tmppath):
       return tmppath
    return None
