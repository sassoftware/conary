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
import socket
import string
import sys
import tempfile
import traceback

class Epdb(pdb.Pdb):
    # epdb will print to here instead of to sys.stdout,
    # and restore stdout when done
    __old_stdout = None

    def __init__(self):
        self._exc_type = None
        self._exc_msg = None
        self._tb = None
        pdb.Pdb.__init__(self)
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

def beingTraced():
    frame = sys._getframe(0)
    while frame:
        if not frame.f_trace is None:
            return True
        frame = frame.f_back
    return False

def set_trace():
    Epdb().set_trace()

def post_mortem(t, exc_type=None, exc_msg=None):
    p = Epdb()
    p._exc_type = exc_type
    p._exc_msg = exc_msg
    p._tb = t
    p.reset()
    while t.tb_next is not None:
        t = t.tb_next
    p.interaction(t.tb_frame, t)

