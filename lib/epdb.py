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
import inspect
import pdb
import os
import re
try:
    import erlcompleter
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
    trace_counts = {'default' : [ True, 0 ]}

    def __init__(self):
        self._exc_type = None
        self._exc_msg = None
        self._tb = None
        self._config = {}
        pdb.Pdb.__init__(self)
        if hasReadline:
            self._completer = erlcompleter.ECompleter()
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
                if(hasattr(self, '_set_' + key)):
                    fn = getattr(self, '_set_' + key)
                    fn(value)
                else:
                    print "No such config value"

    def do_trace_cond(self, args):
        args = args.split(' ', 1)
        if len(args) not in (1, 2):
            print "trace_cond [marker] <cond>"
        if len(args) == 1:
            cond = args[0]
            marker = 'default'
        else:
            marker, cond = args
        if cond == 'None': 
            cond = None
            self.set_trace_cond(marker, cond)
            return
        try:
            cond = int(cond)
            self.set_trace_cond(marker, cond)
            return
        except ValueError:
            locals = self.curframe.f_locals
            globals = self.curframe.f_globals
            try:
                cond = eval(cond + '\n', globals, locals) 
                # test to be sure that what we code is a 
                # function that can take one arg and return a bool
                rv = (type(cond) == bool) or bool(cond(1))
                self.set_trace_cond(marker, cond)
            except:
                t, v = sys.exc_info()[:2]
                if type(t) == type(''):
                    exc_type_name = t
                else: exc_type_name = t.__name__
                print '***', exc_type_name + ':', v
    do_tc = do_trace_cond

    def _set_path(self, paths):
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

    def default(self, line):
        cmd = line.split('?', 1)
        if len(cmd) == 1:
            return pdb.Pdb.default(self, line)
        cmd, directive = cmd
        if directive and directive not in '?cdmp':
            return pdb.Pdb.default(self, line)
        self.do_define(cmd)
        if directive == '?':
            self.do_doc(cmd)
        if directive == 'c':
            self.do_showclasses(cmd)
        elif directive == 'd':
            self.do_showdata(cmd)
        elif directive == 'm':
            self.do_showmethods(cmd)
        elif directive == 'p':
            pdb.Pdb.default(self, 'print ' + cmd)

    def do_p(self, arg):
        cmd = arg.split('?', 1)
        if len(cmd) == 1:
            pdb.Pdb.do_p(self, arg)
        else:
            self.default(arg)

    def _showmethods(self, obj):
        methods = self._getMembersOfType(obj, 'm')
        methods.sort()
        for (methodName, method) in methods:
            try:
                self._define(method)
            except:
                if hasattr(obj, '__name__'):
                    prefix = obj.__name__
                else:
                    prefix = obj.__class__.__name__
                print prefix + '.' + methodName

    def _showdata(self, obj):
        data = self._getMembersOfType(obj, 'd')
        data.sort()
        print [ x[0] for x in data]

    def _showclasses(self, obj):
        classes = self._getMembersOfType(obj, 'c')
        classes.sort()
        for (className, class_) in classes:
            self._define(class_)
            print

    def _objtype(self, obj):
        if inspect.isroutine(obj) or type(obj).__name__ == 'method-wrapper':
            return 'm'
        elif inspect.isclass(obj):
            return 'c'
        else:
            return 'd'

    def _getMembersOfType(self, obj, objType):
        names = dir(obj)
        members = []
        for n in names:
            member = getattr(obj, n)
            if self._objtype(member) == objType:
                members.append((n, member))
        return members
    
    def do_showmethods(self, arg):
        locals = self.curframe.f_locals
        globals = self.curframe.f_globals
        try:
            result = eval(arg + '\n', globals, locals) 
            self._showmethods(result)
        except:
            t, v = sys.exc_info()[:2]
            if type(t) == type(''):
                exc_type_name = t
            else: exc_type_name = t.__name__
            print '***', exc_type_name + ':', v

    def do_showclasses(self, arg):
        locals = self.curframe.f_locals
        globals = self.curframe.f_globals
        try:
            result = eval(arg + '\n', globals, locals) 
            self._showclasses(result)
        except:
            t, v = sys.exc_info()[:2]
            if type(t) == type(''):
                exc_type_name = t
            else: exc_type_name = t.__name__
            print '***', exc_type_name + ':', v


    def do_showdata(self, arg):
        locals = self.curframe.f_locals
        globals = self.curframe.f_globals
        try:
            result = eval(arg + '\n', globals, locals) 
            self._showdata(result)
        except:
            t, v = sys.exc_info()[:2]
            if type(t) == type(''):
                exc_type_name = t
            else: exc_type_name = t.__name__
            print '***', exc_type_name + ':', v

    def _define(self, obj):
        if inspect.isclass(obj):
            bases = inspect.getmro(obj)
            bases = [ x.__name__ for x in bases[1:] ]
            if bases:
                bases = ' -- Bases (' + ', '.join(bases) + ')'
            else:
                bases = '' 
            if hasattr(obj, '__init__') and inspect.isroutine(obj.__init__):
                try:
                    initfn = obj.__init__.im_func
                    argspec = inspect.getargspec(initfn)
                    # get rid of self from arg list...
                    fnargs = argspec[0][1:] 
                    newArgSpec = (fnargs, argspec[1], argspec[2], argspec[3])
                    argspec = inspect.formatargspec(*newArgSpec)
                except TypeError:
                    argspec = '(?)'
            else:
                argspec = ''
            print "Class " + obj.__name__ + argspec + bases
        elif inspect.ismethod(obj) or type(obj).__name__ == 'method-wrapper':
            m_class = obj.im_class
            m_self = obj.im_self
            m_func = obj.im_func
            name = m_class.__name__ + '.' +  m_func.__name__
            #if m_self:
            #    name = "<Bound>"  + name
            argspec = inspect.formatargspec(*inspect.getargspec(m_func))
            print "%s%s" % (name, argspec)
        elif type(obj).__name__ == 'builtin_function_or_method':
            print obj
        elif inspect.isfunction(obj):
            name = obj.__name__
            argspec = inspect.formatargspec(*inspect.getargspec(obj))
            print "%s%s" % (name, argspec)
        else:
            print type(obj)


    def do_define(self, arg):
        locals = self.curframe.f_locals
        globals = self.curframe.f_globals
        try:
            result = eval(arg + '\n', globals, locals) 
            self._define(result)
        except:
            t, v = sys.exc_info()[:2]
            if type(t) == type(''):
                exc_type_name = t
            else: exc_type_name = t.__name__
            print '***', exc_type_name + ':', v
    do_def = do_define

    def do_doc(self, arg):
        locals = self.curframe.f_locals
        globals = self.curframe.f_globals
        try:
            docloc = None
            result = eval(arg + '\n', globals, locals) 
            if hasattr(result, '__doc__'):
                if result.__doc__ is not None:
                    docstr = result.__doc__
                elif inspect.ismethod(result):
                    bases = inspect.getmro(result.im_class)
                    found = False
                    for base in bases:
                        if hasattr(base, result.__name__):
                            baseres = getattr(base, result.__name__)
                            if (hasattr(baseres, '__doc__')
                                and baseres.__doc__ is not None):
                                docloc = baseres
                                docstr = baseres.__doc__
                                found = True
                                break
                    if not found:
                        docstr = None
                else:
                    docstr = None
                print "\"\"\"%s\"\"\"" % docstr
                if docloc:
                    print "(Found doc in %s)" % docloc
                
            if inspect.isclass(result):
                if hasattr(result, '__init__'):
                    self.do_define(arg + '.__init__')
                    if hasattr(result.__init__, '__doc__'):
                        print "\"\"\"%s\"\"\"" % result.__init__.__doc__
                else:
                    print "No init function"
        except:
            t, v = sys.exc_info()[:2]
            if type(t) == type(''):
                exc_type_name = t
            else: exc_type_name = t.__name__
            print '***', exc_type_name + ':', v

    def interaction(self, frame, traceback):
        pdb.Pdb.interaction(self, frame, traceback)
        if not self.__old_stdout is None:
            sys.stdout.flush()
            # now we reset stdout to be the whatever it was before
            sys.stdout = self.__old_stdout

    def switch_stdout(self):
        isatty = False
        try:
            fileno = sys.stdout.fileno()
            isatty = os.isatty(fileno)
        except AttributeError:
            pass
            # sys.stdout is not a regular file,
            # go through some hoops
            # (this is less desirable because it doesn't redirect
            # low-level writes to 1 
              
        if not isatty:
            sys.stdout.flush()
            self.__old_stdout = sys.stdout
            stdout = open('/dev/tty', 'w')
            sys.stdout = stdout
        else:
            self.__old_stdout = None

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

    def set_trace_cond(klass, marker='default', cond=None):
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
        if cond is True:
            rv = True
        elif cond is None or cond is False:
            rv = False
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

    def do_debug(self, arg):
        sys.settrace(None)
        globals = self.curframe.f_globals
        locals = self.curframe.f_locals
        p = Epdb()
        p.prompt = "(%s) " % self.prompt.strip()
        print "ENTERING RECURSIVE DEBUGGER"
        sys.call_tracing(p.run, (arg, globals, locals))
        print "LEAVING RECURSIVE DEBUGGER"
        sys.settrace(self.trace_dispatch)
        self.lastcmd = p.lastcmd

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

def set_trace_cond(cond=None, **kw):
    """ Sets a condition for set_trace statements that have the 
        specified marker.  A condition can either callable, in
        which case it should take one argument, which is the 
        number of times set_trace(marker) has been called,
        or it can be a number, in which case the break will
        only be called.
    """
    for key, val in kw.iteritems():
        Epdb.set_trace_cond(key, val)
    if not kw or cond is not None:
        Epdb.set_trace_cond('default', cond)
stc = set_trace_cond

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

st = set_trace

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
