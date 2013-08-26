#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import base64
import bz2
import errno
import gzip
import os
import re
import select
import signal
import struct
import sys
import threading
import time
from xml.sax import saxutils
from conary.errors import ConaryError

try:
    import fcntl
    import termios
    import tty
except ImportError:
    fcntl = termios = tty = None  # pyflakes=ignore


BUFFER=1024*4096

MARKER, FREETEXT, NEWLINE, CARRIAGE_RETURN, COMMAND, CLOSE = range(6)

LINEBREAKS = ('\r', '\n')

def callable(func):
    func._callable = True
    return func

def makeRecord(d):
    res = "<record>"
    for key, val in sorted(d.iteritems()):
        res += "<%s>%s</%s>" % (key, val, key)
    res += "</record>"
    return res

def getTime():
    """Return ISO8601 compliant time string.

    Return a formatted time string which is ISO8601 compliant.
    Time is expressed in UTC"""
    curTime = time.time()
    msecs = 1000 * (curTime - long(curTime))
    fmtStr = "%Y-%m-%dT%H:%M:%S.%%03dZ"
    return time.strftime(fmtStr, time.gmtime(curTime)) % msecs

def openPath(path):
    class BZ2File(bz2.BZ2File):
        def flush(self):
            pass

    if path.endswith('.bz2'):
        return BZ2File(path, 'w')
    if path.endswith('.gz'):
        return gzip.GzipFile(path, 'w')
    return open(path, 'w')


class Lexer(object):
    def __init__(self, marker, callbacks = None):
        self.marker = marker
        self.callbacks = callbacks or []
        self.stream = ''
        self.mark = False
        self.markMatch = ''

        self.state = FREETEXT

    def registerCallback(self, callback):
        self.callbacks.append(callback)

    def freetext(self, text):
        self.emit((FREETEXT, text))

    def newline(self):
        self.emit((NEWLINE, None))

    def carriageReturn(self):
        self.emit((CARRIAGE_RETURN, None))

    def command(self, text):
        self.emit((COMMAND, text.split(None, 1)))

    def close(self):
        # newline is the only state that can be left half flushed
        if self.state == NEWLINE:
            self.newline()
        self.emit((CLOSE, None))

    def scan(self, sequence):
        """
        scan a sequence of characters, tokenizing it on the fly

        This code is implemented as a simple state machine.
        The general state rules are:
        Freetext can go to freetext or newline;
        Newline can go to newline, freetext or marker;
        Marker can go to marker, freetext, newline or command;
        Command can go to command or freetext.

        If anything is going to be emitted, it generally happens on
        state change.

        If the state machine finishes parsing and it is in freetext,
        it will flush with a freetext token."""
        for char in sequence:
            if self.state == FREETEXT:
                if char in LINEBREAKS:
                    if self.stream:
                        self.freetext(self.stream)
                    self.stream = ''
                    if char == '\n':
                        self.state = NEWLINE
                    else:
                        # emit a CR token, but leave the state as FREETEXT
                        self.carriageReturn()
                else:
                    self.stream += char
            elif self.state == NEWLINE:
                if char in LINEBREAKS:
                    # this means two linebreaks in a row. emit the newline
                    self.newline()
                    self.stream = ''
                    if char == '\r':
                        self.carriageReturn()
                        self.state = FREETEXT
                else:
                    if self.marker.startswith(char):
                        self.stream = char
                        self.state = MARKER
                    else:
                        # emit the newline we were holding
                        self.newline()
                        self.stream = char
                        self.state = FREETEXT
            elif self.state == MARKER:
                if char in LINEBREAKS:
                    # don't forget the newline that was held in abeyance to
                    # get into the marker state.
                    self.newline()
                    if self.stream:
                        self.freetext(self.stream)
                    self.stream = ''
                    if char == '\r':
                        self.carriageReturn()
                        self.state = FREETEXT
                    else:
                        self.state = NEWLINE
                else:
                    candidate = self.stream + char
                    self.stream += char
                    if self.stream == self.marker:
                        self.stream = ''
                        self.state = COMMAND
                    else:
                        if not self.marker.startswith(candidate):
                            self.newline()
                            self.state = FREETEXT
            elif self.state == COMMAND:
                if char == '\n':
                    self.command(self.stream.lstrip())
                    self.stream = ''
                    self.state = FREETEXT
                else:
                    self.stream += char
        if self.state == FREETEXT:
            if self.stream:
                self.freetext(self.stream)
            self.stream = ''

    def write(self, text):
        return self.scan(text)

    def flush(self):
        self.scan('')

    def emit(self, token):
        for callback in self.callbacks:
            callback(token)


class LogWriter(object):
    def handleToken(self, token):
        mode, param = token
        if mode == FREETEXT:
            self.freetext(param)
        elif mode == NEWLINE:
            self.newline()
        elif mode == CARRIAGE_RETURN:
            self.carriageReturn()
        elif mode == COMMAND:
            self.command(*param)
        elif mode == CLOSE:
            self.close()

    def freetext(self, text):
        pass

    def write(self, text):
        # alias to freetext to define a more file-object-like interface
        return self.freetext(text)

    def flush(self):
        pass

    def newline(self):
        pass

    def carriageReturn(self):
        pass

    def start(self):
        pass

    @callable
    def reportMissingBuildRequires(self, data):
        self.freetext("warning: Suggested buildRequires additions: ['%s']"
                      %"', '".join(data.split(' ')))
        self.newline()

    @callable
    def reportExcessBuildRequires(self, data):
        self.freetext("info: Possible excessive buildRequires: ['%s']"
                      %"', '".join(data.split(' ')))
        self.newline()

    @callable
    def reportExcessSuperclassBuildRequires(self, data):
        self.freetext("info: Possible excessive superclass buildRequires: ['%s']"
                      %"', '".join(data.split(' ')))
        self.newline()

    def command(self, cmd, *args):
        func = getattr(self.__class__, cmd, False)
        # silently ignore nonsensical calls because the logger loops over each
        # writer and passes the command separately to all of them
        if func and func.__dict__.get('_callable', False):
            try:
                return func(self, *args)
            except TypeError:
                # Probably the wrong number of arguments; make it
                # possible to debug the problem
                self.freetext('\nERROR: failed attempt to call'
                    ' function %s with arguments %s\n' %(cmd, repr(args)))
            except Exception, e:
                # Unknown problem; provide information so that we can
                # debug it and fix it later
                self.freetext('\nERROR: unhandled exception %s: %s'
                    ' calling function %s with arguments %s\n' %(
                    str(e.__class__), str(e), cmd, repr(args)))

    def close(self):
        pass

class XmlLogWriter(LogWriter):
    def __init__(self, path):
        self.data = threading.local()
        self.messageId = 0
        self.path = path
        self.logging = False
        self.text = ''

        self.stream = None
        LogWriter.__init__(self)

    def flush(self):
        self.stream.flush()

    def start(self):
        self.stream = openPath(self.path)
        print >> self.stream, '<?xml version="1.0"?>'
        print >> self.stream, \
                "<log xmlns='http://www.rpath.com/permanent/log-v1.xsd'>"
        self.log('begin log', 'DEBUG')
        self.stream.flush()
        self.logging = True

    def _getDescriptorStack(self):
        if not hasattr(self.data, 'descriptorStack'):
            self.data.descriptorStack = []
        return self.data.descriptorStack

    def _getRecordData(self):
        if not hasattr(self.data, 'recordData'):
            self.data.recordData = {}
        return self.data.recordData

    def close(self):
        if not self.logging:
            return
        del self._getDescriptorStack()[:]
        self._getRecordData().clear()
        self.log('end log', 'DEBUG')
        print >> self.stream, "</log>"
        self.stream.flush()
        self.stream.close()

    def freetext(self, text):
        self.text += text

    def newline(self):
        if self.text:
            self.log(self.text)
        self.text = ''

    carriageReturn = newline

    def _getDescriptor(self):
        descriptorStack = self._getDescriptorStack()
        return '.'.join(descriptorStack)

    def log(self, message, levelname = 'INFO'):
        # escape xml delimiters and newline characters
        message = saxutils.escape(message)
        message = message.replace('\n', '\\n')
        macros = {}
        recordData = self._getRecordData()
        macros.update(recordData)
        macros['time'] = getTime()
        macros['message'] = message
        macros['level'] = levelname
        macros['pid'] = os.getpid()
        threadName = threading.currentThread().getName()
        if threadName != 'MainThread':
            macros['threadName'] = threadName
        macros['messageId'] = self.messageId
        self.messageId += 1
        descriptor = self._getDescriptor()
        if descriptor:
            macros['descriptor'] = descriptor
        print >> self.stream, makeRecord(macros)

    @callable
    def pushDescriptor(self, descriptor):
        descriptorStack = self._getDescriptorStack()
        descriptorStack.append(descriptor)

    @callable
    def popDescriptor(self, descriptor = None):
        descriptorStack = self._getDescriptorStack()
        desc = descriptorStack.pop()
        if descriptor:
            assert descriptor == desc
        return desc

    @callable
    def addRecordData(self, *args):
        if not args:
            # handle bad input
            return
        if len(args) < 2:
            # called via lexer
            key, val = args[0].split(None, 1)
        else:
            # called via xmllog:addRecordData
            key, val = args
        if key[0].isdigit() or \
                not re.match('^\w[a-zA-Z0-9_.-]*$', key,
                    flags = re.LOCALE | re.UNICODE):
            raise RuntimeError("'%s' is not a legal XML name" % key)
        if isinstance(val, (str, unicode)):
            val = saxutils.escape(val)
        recordData = self._getRecordData()
        recordData[key] = val

    @callable
    def delRecordData(self, key):
        recordData = self._getRecordData()
        recordData.pop(key, None)

    @callable
    def reportMissingBuildRequires(self, data):
        self.pushDescriptor('missingBuildRequires')
        self.log(data, levelname = 'WARNING')
        self.popDescriptor('missingBuildRequires')

    @callable
    def reportExcessBuildRequires(self, data):
        self.pushDescriptor('excessBuildRequires')
        self.log(data, levelname = 'INFO')
        self.popDescriptor('excessBuildRequires')

    @callable
    def reportExcessSuperclassBuildRequires(self, data):
        self.pushDescriptor('excessSuperclassBuildRequires')
        self.log(data, levelname = 'DEBUG')
        self.popDescriptor('excessSuperclassBuildRequires')

class FileLogWriter(LogWriter):
    def __init__(self, path):
        self.path = path
        self.stream = None
        LogWriter.__init__(self)
        self.logging = False

    def start(self):
        self.stream = openPath(self.path)
        self.logging = True

    def freetext(self, text):
        if self.logging:
            self.stream.write(text)
            self.stream.flush()

    def newline(self):
        if self.logging:
            self.stream.write('\n')
            self.stream.flush()

    carriageReturn = newline

    def close(self):
        self.stream.close()
        self.logging = False


class StreamLogWriter(LogWriter):
    def __init__(self, stream = None):
        self.data = threading.local()
        self.data.hideLog = False
        self.stream = stream
        LogWriter.__init__(self)
        self.index = 0
        self.closed = bool(self.stream)

    def start(self):
        if not self.stream:
            self.stream = sys.stdout

    def freetext(self, text):
        if not self.data.__dict__.get('hideLog'):
            self.stream.write(text)
            self.stream.flush()
            self.index += len(text)

    def newline(self):
        if not self.data.__dict__.get('hideLog'):
            self.stream.write('\n')
            self.stream.flush()
            self.index = 0

    def carriageReturn(self):
        if not self.data.__dict__.get('hideLog'):
            if (self.index % 80):
                spaces = 78 - (self.index % 80)
                self.stream.write(spaces * ' ')
            self.stream.write('\r')
            self.stream.flush()
            self.index = 0

    @callable
    def pushDescriptor(self, descriptor):
        if descriptor == 'environment':
            self.data.hideLog = True

    @callable
    def popDescriptor(self, descriptor = None):
        if descriptor is None:
            return
        if descriptor == 'environment':
            self.data.hideLog = False

    @callable
    def reportExcessSuperclassBuildRequires(self, data):
        # This is really only for debugging Conary itself, and so is
        # useful to store in logfiles but not to display
        pass


class SubscriptionLogWriter(LogWriter):
    def __init__(self, path):
        self.path = path
        self.stream = None
        LogWriter.__init__(self)
        self.logging = False
        self.current = None
        self.rePatternList = []
        self.r = None

    @callable
    def subscribe(self, pattern):
        self.rePatternList.append(pattern)
        self.r = re.compile('(%s)' %'|'.join(self.rePatternList))

    @callable
    def synchronizeMark(self, timestamp):
        # Allow consumers to ensure that the log is complete as of
        # output from all previous code
        if self.current:
            self.newline(forceNewline=True)
        self.stream.write(timestamp)
        self.stream.write('\n')
        self.stream.flush()

    def start(self):
        # do not use openPath because this file needs 'a' for
        # immediate synchronize()
        self.stream = file(self.path, 'a')
        self.logging = True

    def freetext(self, text):
        if self.current:
            self.current += text
        else:
            self.current = text

    def newline(self, forceNewline=False):
        if self.current:
            if self.current[-1] == '\\':
                self.current = self.current.rstrip('\\')
                if not forceNewline:
                    return
            if self.r and self.r.match(self.current):
                self.stream.write(self.current)
                self.stream.write('\n')
                # no need to flush, since we explicitly flush on the
                # necessary synchronizeMark
            self.current = None

    carriageReturn = newline

    def close(self):
        self.stream.close()
        self.logging = False



def startLog(path, xmlPath, subscribeLogPath, withStdin = True):
    """ Start the log.  Equivalent to Logger(path).startLog() """
    plainWriter = FileLogWriter(path)
    xmlWriter = XmlLogWriter(xmlPath)
    screenWriter = StreamLogWriter()
    subscriptionWriter = SubscriptionLogWriter(subscribeLogPath)
    # touch this file to ensure that synchronize() works immediately
    file(subscribeLogPath, 'a')
    lgr = Logger(withStdin = withStdin,
                 writers = [plainWriter, xmlWriter,
                            screenWriter, subscriptionWriter],
                 syncPath = subscribeLogPath)
    lgr.startLog()
    return lgr

def escapeMessage(msg):
    # Replace newline (0x0a) with \n (2 chars)
    # For this to work, we need to replace \ with \\ first
    assert('\0' not in msg)
    msg = msg.replace('\\', '\\\\')
    msg.replace('\n', '\\n')
    return msg

def unescapeMessage(msg):
    # Replace double-backslash with \0
    msg = msg.replace('\\\\', '\0')
    # Replace \n with newline and \0 back into \
    msg = msg.replace('\\n', '\n')
    msg = msg.replace('\0', '\\')
    return msg

class Logger:
    def __init__(self, withStdin = True, writers = [], syncPath = None):
        # by using a random string, we ensure that the marker used by the
        # logger class will never appear in any code, not even conary, thus
        # making the logger's code robust against tripping itself.
        # By using 42 octets we ensure that the probability of encountering
        # the marker string accidentally is negligible with a high degree of
        # confidence.
        if not termios:
            raise RuntimeError("The build logger requires the termios module")
        self.marker = base64.b64encode(os.urandom(42))
        self.lexer = Lexer(self.marker)
        for writer in writers:
            self.lexer.registerCallback(writer.handleToken)
        self.writers = writers
        self.syncPath = syncPath
        self.logging = False
        self.closed = False
        self.withStdin = withStdin
        self.data = threading.local()

    def _getDescriptorStack(self):
        if not hasattr(self.data, 'descriptorStack'):
            self.data.descriptorStack = []
        return self.data.descriptorStack

    def directLog(self, msg):
        # We need to escape newline chars in msg
        self.command("directLog %s" % escapeMessage(msg))

    def command(self, cmdStr):
        # Writing to stdout will make the output go through the tty,
        # which is exactly what we want
        sys.stdout.flush()
        sys.stderr.flush()
        msg = "\n%s %s\n" % (self.marker, cmdStr)
        os.write(sys.stdout.fileno(), msg)

    def write(self, *msgs):
        for msg in msgs:
            os.write(sys.stdout.fileno(), msg)

    def flush(self):
        sys.stdout.flush()

    def pushDescriptor(self, descriptor):
        descriptorStack = self._getDescriptorStack()
        descriptorStack.append(descriptor)
        self.command("pushDescriptor %s" % descriptor)

    def popDescriptor(self, descriptor = None):
        descriptorStack = self._getDescriptorStack()
        if descriptor:
            if not descriptorStack:
                raise RuntimeError('Log Descriptor does not match expected '
                        'value: empty stack while expecting %s' %
                            (descriptor, ))
            stackTop = descriptorStack[-1]
            if descriptor != stackTop:
                raise RuntimeError('Log Descriptor does not match expected '
                    'value: stack contained %s but reference value was %s' %
                            (stackTop, descriptor))
            descriptorStack.pop()

            self.command("popDescriptor %s" % descriptor)
            return stackTop

        self.command("popDescriptor")
        return None

    def addRecordData(self, key, val):
        self.command('addRecordData %s %s' % (key, val))

    def delRecordData(self, key):
        self.command('delRecordData %s %s' % key)

    def subscribe(self, pattern):
        self.command('subscribe %s' %pattern)

    def reportMissingBuildRequires(self, reqList):
        self.command('reportMissingBuildRequires %s' %' '.join(reqList))

    def reportExcessBuildRequires(self, reqList):
        self.command('reportExcessBuildRequires %s' %' '.join(reqList))

    def reportExcessSuperclassBuildRequires(self, reqList):
        self.command('reportExcessSuperclassBuildRequires %s' %' '.join(reqList))

    def synchronize(self):
        timestamp = '%10.8f' %time.time()
        timestampLen = len(timestamp)
        self.command('synchronizeMark %s' %timestamp)
        self.flush()
        syncFile = file(self.syncPath)

        def _fileLongEnough():
            syncFile.seek(0, 2)
            return syncFile.tell() > timestampLen

        def _waitedTooLong(i, stage):
            if i > 10000:
                # 100 seconds is too long to wait for a pty; something's wrong
                syncFile.seek(0,2)
                length = syncFile.tell()
                raise ConaryError(
                    'Log file synchronization %s failure with length %d' %(
                    stage, length))

        i = 0
        while not _fileLongEnough():
            _waitedTooLong(i, 'init')
            i += 1
            time.sleep(0.01)

        def _seekTimestamp():
            syncFile.seek(-(timestampLen + 1), 2)

        i = 0
        _seekTimestamp()
        while syncFile.read(timestampLen) != timestamp:
            _waitedTooLong(i, 'search')
            i += 1
            time.sleep(0.01)
            _seekTimestamp()

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

        masterFd, slaveFd = os.openpty()
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)

        pid = os.fork()
        if pid:
            # make parent process the pty slave - the opposite of
            # pty.fork().  In this setup, the parent process continues
            # to act normally, while the child process performs the
            # logging.  This makes it simple to kill the logging process
            # when we are done with it and restore the parent process to
            # normal, unlogged operation.
            os.close(masterFd)
            self._becomeLogSlave(slaveFd, pid)
            return
        try:
            os.close(slaveFd)
            for writer in self.writers:
                writer.start()
            logger = _ChildLogger(masterFd, self.lexer,
                                  self.restoreTerminalControl, self.withStdin)
            try:
                logger.log()
            finally:
                self.lexer.close()
        finally:
            os._exit(0)

    def _becomeLogSlave(self, slaveFd, loggerPid):
        """ hand over control of io to logging process, grab info
            from pseudo tty
        """
        self.loggerPid = loggerPid

        if self.withStdin and sys.stdin.isatty():
            self.oldTermios = termios.tcgetattr(sys.stdin.fileno())
        else:
            self.oldTermios = None

        newTermios = termios.tcgetattr(slaveFd)
        # Don't wait after receiving a character
        newTermios[6][termios.VTIME] = '\x00'
        # Read at least these many characters before returning
        newTermios[6][termios.VMIN] = '\x01'

        termios.tcsetattr(slaveFd, termios.TCSADRAIN, newTermios)
        # Raw mode
        tty.setraw(slaveFd)

        self.oldStderr = os.dup(sys.stderr.fileno())
        self.oldStdout = os.dup(sys.stdout.fileno())
        if self.withStdin:
            self.oldStdin = os.dup(sys.stdin.fileno())
            os.dup2(slaveFd, 0)
        else:
            self.oldStdin = sys.stdin.fileno()
        os.dup2(slaveFd, 1)
        os.dup2(slaveFd, 2)
        os.close(slaveFd)
        self.logging = True

    def close(self):
        """ Reassert control of tty.  Closing stdin, stderr, and and stdout
            will get rid of the last pointer to the slave fd of the pseudo
            tty, which should cause the logging process to stop.  We wait
            for it to die before continuing
        """
        if not self.logging:
            return
        self.closed = True
        # restore old terminal settings before quitting
        if self.oldStdin != 0:
            os.dup2(self.oldStdin, 0)
        os.dup2(self.oldStdout, 1)
        os.dup2(self.oldStderr, 2)
        if self.oldTermios is not None:
            termios.tcsetattr(0, termios.TCSADRAIN, self.oldTermios)
        if self.oldStdin != 0:
            os.close(self.oldStdin)
        os.close(self.oldStdout)
        os.close(self.oldStderr)
        try:
            # control stdin -- if stdin is a tty
            # that can be controlled
            if sys.stdin.isatty() and self.restoreTerminalControl:
                os.tcsetpgrp(0, os.getpgrp())
        except AttributeError:
            # stdin might not even have an isatty method
            pass

        # Wait for child logging process to die.  Send successively ruder
        # signals if it does not do so within a reasonable time.  The primary
        # reason that it would not die immediately is that a process has forked
        # while holding the TTY file descriptor, and thus the logger is still
        # polling it for output.
        signals = [signal.SIGTERM, signal.SIGKILL]
        while signals:
            start = time.time()
            while time.time() - start < 10:
                pid, status = os.waitpid(self.loggerPid, os.WNOHANG)
                if pid:
                    break
                time.sleep(0.1)
            else:
                # Child process did not die.
                signum = signals.pop(0)
                os.kill(self.loggerPid, signum)
                continue
            break
        else:
            # Last signal was a KILL, so wait indefinitely.
            os.waitpid(self.loggerPid, 0)


class _ChildLogger:
    def __init__(self, ptyFd, lexer, controlTerminal, withStdin):
        # ptyFd is the fd of the pseudo tty master
        self.ptyFd = ptyFd
        # lexer is a python file-like object that supports the write
        # and close methods
        self.lexer = lexer
        self.shouldControlTerminal = controlTerminal
        self.withStdin = withStdin

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
        lexer = self.lexer
        stdin = sys.stdin.fileno()
        unLogged = ''

        pollObj = select.poll()
        pollObj.register(ptyFd, select.POLLIN)
        if self.withStdin and os.isatty(stdin):
            pollObj.register(stdin, select.POLLIN)

        # sigwinch is called when the window size is changed
        sigwinch = []
        def sigwinch_handler(s, f):
            sigwinch.append(True)
        # disable to ensure window size is standardized
        #signal.signal(signal.SIGWINCH, sigwinch_handler)

        while True:
            try:
                read = [ x[0] for x in pollObj.poll() ]
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
                    lexer.write(output)

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
            if sigwinch:
            #   disable sigwinch to ensure the window width expected in logs is standardized
            #   self._resizeTerminal()
                sigwinch = []
