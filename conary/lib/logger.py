# Copyright (c) 2005-2006 rPath, Inc.
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

import bz2
import errno
import fcntl
import gzip
import os
import pty
import select
import signal
import struct
import sys
import termios

BUFFER=1024*4096

def startLog(path):
    """ Start the log.  Equivalent to Logger(path).startLog() """
    return Logger(path)

class Logger:
    def __init__(self, path):
        self.path = path
        self.logging = False
        self.closed = False
        self.startLog()

    def __del__(self):
        if self.logging and not self.closed:
            self.close()

    def startLog(self):
        """Starts the log to path.  The parent process becomes the "slave" 
        process: its stdin, stdout, and stderr are redirected to a pseudo tty.
        A child logging process controls the real stdin, stdout, and stderr,
        and writes stdout and stderr both to the screen and to the logfile 
        at path.  
        """
        self.restoreTerminalControl = (sys.stdin.isatty() and
            os.tcgetpgrp(0) == os.getpid())

        masterFd, slaveFd = pty.openpty()
        directRd, directWr = os.pipe()
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        pid = os.fork()
        if pid:
            # make parent process the pty slave - the opposite of 
            # pty.fork().  In this setup, the parent process continues
            # to act normally, while the child process performs the 
            # logging.  This makes it simple to kill the logging process
            # when we are done with it and restore the parent process to 
            # normal, unlogged operation.
            os.close(directRd)
            os.close(masterFd)
            self._becomeLogSlave(slaveFd, pid, directWr)
            return
        os.close(directWr)
        os.close(slaveFd)
        if self.path.endswith('.bz2'):
            logFile = bz2.BZ2File(self.path, 'w')
        elif self.path.endswith('.gz'):
            logFile = gzip.GZipFile(self.path, 'w')
        else:
            logFile = open(self.path, 'w')
        logger = _ChildLogger(masterFd, logFile, directRd, 
                              controlTerminal=self.restoreTerminalControl)
        try:
            logger.log()
        finally:
            logFile.close()
        os._exit(0)

    def write(self, data):
        os.write(self.directWr, data)

    def _becomeLogSlave(self, slaveFd, loggerPid, directWr):
        """ hand over control of io to logging process, grab info
            from pseudo tty
        """
        self.directWr = directWr
        self.loggerPid = loggerPid

        if sys.stdin.isatty():
            self.oldTermios = termios.tcgetattr(sys.stdin.fileno())
        else:
            self.oldTermios = None
        self.oldStderr = os.dup(sys.stderr.fileno())
        self.oldStdout = os.dup(sys.stdout.fileno())
        self.oldStdin = os.dup(sys.stdin.fileno())
        os.dup2(slaveFd, 0)
        os.dup2(slaveFd, 1)
        os.dup2(slaveFd, 2)
        os.close(slaveFd)
        self.logging = True
        return

    def close(self):
        """ Reassert control of tty.  Closing stdin, stderr, and and stdout
            will get rid of the last pointer to the slave fd of the pseudo
            tty, which should cause the logging process to stop.  We wait
            for it to die before continuing
        """
        self.closed = True
        # restore old terminal settings before quitting
        os.dup2(self.oldStdin, 0)
        os.dup2(self.oldStdout, 1)
        os.dup2(self.oldStderr, 2)
        if self.oldTermios is not None:
            termios.tcsetattr(0, termios.TCSADRAIN, self.oldTermios)
        os.close(self.oldStdin)
        os.close(self.oldStdout)
        os.close(self.oldStderr)
        os.close(self.directWr)
        try:
            # control stdin -- if stdin is a tty
            # that can be controlled
            if sys.stdin.isatty() and self.restoreTerminalControl:
                os.tcsetpgrp(0, os.getpgrp())
        except AttributeError:
            # stdin might not even have an isatty method
            pass
        # wait for child logging process to die
        os.waitpid(self.loggerPid, 0)

class _ChildLogger:
    def __init__(self, ptyFd, logFile, directRd, controlTerminal):
        # ptyFd is the fd of the pseudo tty master 
        self.ptyFd = ptyFd
        # logFile is a python file-like object that supports the write
        # and close methods
        self.logFile = logFile
        # directRd is for input that goes directly to the log 
        # without being output to screen
        self.directRd = directRd
        self.shouldControlTerminal = controlTerminal

    def _controlTerminal(self):
        try:
            # the child should control stdin -- if stdin is a tty
            # that can be controlled
            if sys.stdin.isatty():
                os.tcsetpgrp(0, os.getpgrp())
        except AttributeError:
           # stdin might not even have an isatty method
            pass

    def _resizeTerminal(self):
        """ If a windowing system has announced to us that the window has
            been resized, pass that information to the pseudo tty so that
            output can be reformated
        """
        s = struct.pack('HHHH', 0, 0, 0, 0)
        result = fcntl.ioctl(sys.stdin.fileno(), termios.TIOCGWINSZ, s)
        rows, cols = struct.unpack('HHHH', result)[0:2]
        s = struct.pack('HHHH', rows, cols, 0, 0)
        fcntl.ioctl(self.ptyFd, termios.TIOCSWINSZ, s)

    def _setTerminalSize(self, rows, cols):
        s = struct.pack('HHHH', rows, cols, 0, 0)
        fcntl.ioctl(self.ptyFd, termios.TIOCSWINSZ, s)

    def log(self):
        if self.shouldControlTerminal:
            self._controlTerminal()
    
        # standardize terminal size at 24, 80 for those programs that 
        # access it.  This should ensure that programs that look at 
        # terminal size for displaying log info will look similar across
        # runs.
        self._setTerminalSize(24, 80)

        # set some local variables that are reused often within the loop
        ptyFd = self.ptyFd
        logFile = self.logFile
        directRd = self.directRd
        stdin = sys.stdin.fileno()
        unLogged = ''

        stdout = sys.stdout.fileno()

        fdList = [directRd, ptyFd]
        if os.isatty(stdin):
            fdList.append(stdin)
        
        # sigwinch is called when the window size is changed
        sigwinch = []
        def sigwinch_handler(s, f):
            sigwinch.append(True)
        # disable to ensure window size is standardized
        #signal.signal(signal.SIGWINCH, sigwinch_handler)

        while True:
            try:
                # XXX is poll more efficient here?
                read, write, error = select.select(fdList, [], [])
            except select.error, msg:
                if msg.args[0] != 4:
                    raise
                read = []
            if directRd in read:
                # read output from pseudo terminal stdout/stderr, and pass to 
                # terminal and log
                try:
                    output = os.read(directRd, BUFFER)
                except OSError, msg:
                    if msg.errno == errno.EIO: 
                        # input/output error - pipe closed
                        # shut down logger
                        break
                        if unLogged:
                            logFile.write(unLogged + '\n')
                    elif msg.errno != errno.EINTR:
                        # EINTR is due to an interrupted read - that could be
                        # due to a SIGWINCH signal.  Raise any other error
                        raise
                else:
                    logFile.write(output)

                if output:
                    # always read all of directWrite before reading anything else
                    continue

            if ptyFd in read:
                # read output from pseudo terminal stdout/stderr, and pass to 
                # terminal and log
                try:
                    output = os.read(ptyFd, BUFFER)
                except OSError, msg:
                    if msg.errno == errno.EIO: 
                        # input/output error - pty closed 
                        # shut down logger
                        break
                        if unLogged:
                            logFile.write(unLogged + '\n')
                    elif msg.errno != errno.EINTR:
                        # EINTR is due to an interrupted read - that could be
                        # due to a SIGWINCH signal.  Raise any other error
                        raise
                else:
                    # avoid writing foo\rbar\rblah to log
                    outputList = output.split('\r\n')

                    if outputList[-1].endswith('\r'):
                        os.write(stdout, output[:-1])
                        # blank out previous line extra bits
                        os.write(stdout, ' '*(78-len(outputList[-1])) + '\r')
                    else:
                        os.write(stdout, output)
                    if unLogged:
                        outputList[0] = unLogged + outputList[0]
                        unLogged = ''
                    outputList = [x.rsplit('\r', 1)[-1] for x in outputList
                                  if not x.endswith('\r')]
                    if outputList:
                        # if output didn't end with \n, save last line for later
                        unLogged = outputList[-1]
                        if unLogged:
                            outputList[-1] = ''
                        logFile.write('\n'.join(outputList))
            if stdin in read:
                # read input from stdin, and pass to 
                # pseudo tty 
                try:
                    input = os.read(stdin, BUFFER)
                except OSError, msg:
                    if msg.errno == errno.EIO: 
                        # input/output error - stdin closed 
                        # shut down logger
                        break
                        if unLogged:
                            logFile.write(unLogged + '\n')
                    elif msg.errno != errno.EINTR:
                        # EINTR is due to an interrupted read - that could be
                        # due to a SIGWINCH signal.  Raise any other error
                        raise
                else:
                    os.write(ptyFd, input)
            if sigwinch:
            #   disable sigwinch to ensure the window width expected in logs is standardized
            #   self._resizeTerminal()
                sigwinch = []

