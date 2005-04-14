#!/usr/bin/python 

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
        self.startLog()
        self.closed = False

    def __del__(self):
        if not self.closed:
            self.close()

    def startLog(self):
        """Starts the log to path.  The parent process becomes the "slave" 
        process: its stdin, stdout, and stderr are redirected to a pseudo tty.
        A child logging process controls the real stdin, stdout, and stderr,
        and writes stdout and stderr both to the screen and to the logfile 
        at path.  
        """
        masterFd, slaveFd = pty.openpty()
        directRd, directWr = os.pipe()
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        pid = os.fork()
        if pid:
            # make parent process slave -- this allows us to 
            # restore the logged process and have the child logging process.
            # this would be difficult if the parent process were doing
            # the logging as would be done with pty.fork()
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
        logger = _ChildLogger(masterFd, logFile, directRd)
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
        self.oldTermios = termios.tcgetattr(sys.stdin.fileno())
        self.oldStderr = os.dup(sys.stderr.fileno())
        self.oldStdout = os.dup(sys.stdout.fileno())
        self.oldStdin = os.dup(sys.stdin.fileno())
        os.dup2(slaveFd, 0)
        os.dup2(slaveFd, 1)
        os.dup2(slaveFd, 2)
        os.close(slaveFd)
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
        termios.tcsetattr(0, termios.TCSADRAIN, self.oldTermios)
        os.close(self.oldStdin)
        os.close(self.oldStdout)
        os.close(self.oldStderr)
        os.close(self.directWr)
        os.setpgrp()
        try:
            # control stdin -- if stdin is a tty
            # that can be controlled
            if sys.stdin.isatty():
                os.tcsetpgrp(0, os.getpgrp())
        except AttributError:
            # stdin might not even have an isatty method
            pass
        # wait for child logging process to die
        os.waitpid(self.loggerPid, 0)

class _ChildLogger:
    def __init__(self, ptyFd, logFile, directRd):
        # ptyFd is the fd of the pseudo tty master 
        self.ptyFd = ptyFd
        # logFile is a python file-like object that supports the write
        # and close methods
        self.logFile = logFile
        # directRd is for input that goes directly to the log 
        # without being output to screen
        self.directRd = directRd

    def _controlTerminal(self):
        try:
            # the child should control stdin -- if stdin is a tty
            # that can be controlled
            if sys.stdin.isatty():
                os.tcsetpgrp(0, os.getpgrp())
        except AttributError:
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
        s = struct.pack('HHHH', rows, columns, 0, 0)
        fcntl.ioctl(self.ptyFd, termios.TIOCSWINSZ, s)

    def log(self):
        self._controlTerminal()

        # set some local variables that are reused often within the loop
        ptyFd = self.ptyFd
        logFile = self.logFile
        directRd = self.directRd
        stdin = sys.stdin.fileno()
        stdout = sys.stdout.fileno()

        fdList = [stdin, directRd, ptyFd]
        
        # sigwinch is called when the window size is changed
        sigwinch = []
        def sigwinch_handler(s, f):
            sigwinch.append(True)
        signal.signal(signal.SIGWINCH, sigwinch_handler)

        while True:
            try:
                # XXX is poll more efficient here?
                read, write, error = select.select(fdList, [], [])
            except select.error, msg:
                if msg.args[0] != 4:
                    raise
                read = []
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
                    elif msg.errno != errno.EINTR:
                        # EINTR is due to an interrupted read - that could be
                        # due to a SIGWINCH signal.  Raise any other error
                        raise
                else:
                    os.write(stdout, output)
                    logFile.write(output)
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
                    elif msg.errno != errno.EINTR:
                        # EINTR is due to an interrupted read - that could be
                        # due to a SIGWINCH signal.  Raise any other error
                        raise
                else:
                    logFile.write(output)
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
                    elif msg.errno != errno.EINTR:
                        # EINTR is due to an interrupted read - that could be
                        # due to a SIGWINCH signal.  Raise any other error
                        raise
                else:
                    os.write(ptyFd, input)
                    logFile.write(input)
            if sigwinch:
                self._signalTerminalResized()
                sigwinch = []

