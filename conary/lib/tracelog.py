#!/usr/bin/python
#
# Copyright (c) 2005 rPath, Inc.
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

# This logging module is primarily intended to be used by the server side.
# TODO:
# - update to support logging stuff from Apache's request

# LOG level conventions
# 1 - major functions entry points. Assume prod is running at this loglevel
# 2 - helper functions entry points
# 3 - SQL statements with arguments, db state
# 4 - copious debugging

import os
import sys
import traceback
import time

# needed so we know not to log ourselves
_thisFile = os.path.basename(sys.modules[__name__].__file__)

# "static" placeholder for global use
_LOG = None

# we log time in milliseconds
def logTime():
    t = time.time()
    ret = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(t))
    ret = ret + ".%.3f" % (t - int(t),)
    return ret

# this is used to dump a line to stderr
def printErr(*args):
    pid = os.getpid()
    for arg in args:
        sys.stderr.write("%s %s: %s\n" % (
            pid, logTime(), arg))
    sys.stderr.flush()

# Base logging class that figures out where it is being called from
class FileLog:
    def __init__(self, filename="stderr", level=1):
        self.pid = os.getpid()
        self.level = level
        self.filename = filename
        self.isFile = 0        
        if filename in ["stderr", "stdout"]:
            self.fd = getattr(sys, filename)
            self.reset()
            return

        mustChmod = not os.path.exists(self.filename)
        try: # line buffered mode
            self.fd = open(self.filename, "a+", 1)
            if mustChmod:
                os.chmod(self.filename, 0660)
        except:
            printErr("ERROR: Couldn't open log file %s" % (self.filename,),
                   sys.exc_info()[:2])
            self.filename = "stderr"
            self.fd = sys.stderr
        else:
            self.isFile = 1

    # placeholder for something that can be useful for more complex classes
    def reset(self, level=None):
        if level:
            self.level = level
    
    # python cookbooks are great
    def log(self, *args):
        global _thisFile
        tbStack = traceback.extract_stack()
        # trim off the calls from this module
        callid = 0
        while 1:
            # handle .py / .pyc
            if _thisFile.startswith(os.path.basename(tbStack[callid][0])):
                callid = callid - 1
                break            
            callid = callid + 1
        module = ''
        try:
            path = tbStack[callid][0].split('/')
            if len(path) > 1:
                dirname = path[-2] + "/"
            else:
                dirname = ""
            # poor man's way of stripping the extension
            filename = path[-1].split('.')
            module = "%s%s" % (dirname, ".".join(filename[:-1]))
            del path
        except:
            module = ''
        msg = "%s.%s" % (module, tbStack[callid][2])
        if len(args):
            msg = "%s %s" % (msg, " ".join([str(x) for x in args]))
        # format for logging
        msg = self.formatLog(msg)
        self.writeLog(msg)
        
    # this function is mainly here so we can subclass it (ie, tracing)
    def formatLog(self, msg):
        return "%s %d %s" % (logTime(), self.pid, msg)
        
    # simply print a message to the log.
    def writeLog(self, msg):
        self.fd.write("%s\n" % msg)

    # close on exit
    def __del__(self):
        if self.isFile:
            self.fd.close()
        self.level = self.filename = self.isFile = self.fd = None


# a class that keeps tabs on the time spend between calls to the log function
class TraceLog(FileLog):
    lastTime = 0
    
    def formatLog(self, msg):
        t = time.time()
        ret = time.strftime("%H:%M:%S", time.localtime(t))
        return "%s %+.3f %s" % (ret, t - self.lastTime, msg)

    def writeLog(self, msg):
        self.lastTime = time.time()
        FileLog.writeLog(self, msg)
        
    def reset(self, level=None):
        self.lastTime = time.time()
        FileLog.reset(self, level)

# initialize the module global log
def initLog(filename="stderr", level=0, trace=0):
    global _LOG

    if _LOG is not None:        
        if filename is None or _LOG.filename == filename:
            _LOG.reset(level)
            return
        _LOG = None
    elif filename is None:
        filename = "/dev/null"

    path = os.path.dirname(filename)
    # be nice and figure out the permission problems if we're working
    # with a real path
    if path:
        if not os.path.exists(path):
            printErr("ERROR: Invalid pathname for logfile: %s" % (path,))
            filename = "stderr"
        if os.path.exists(filename):
            if not os.access(filename, os.W_OK):
                printErr("ERROR: Could not open log file %s for writing" % (
                    filename,))
                filename = "stderr"
        elif not os.access(path, os.W_OK|os.X_OK):
            printErr("ERROR: could not create logfile %s"
                     "due to directory permissions" % (filename,))
            filename = "stderr"
    # all should be sane now
    if trace:
        _LOG = TraceLog(filename, level)
    else:
        _LOG = FileLog(filename, level)
    return 0

# the basic function to log stuff using this module
def logMe(level, *args):
    global _LOG
    if _LOG and _LOG.level >= level:
        apply(_LOG.log, args)

# shortcut for error logging
def logErr(*args):
    global _LOG
    if not args:
        return
    if _LOG:
        apply(_LOG.log, ["ERROR"] + list(args))
    if _LOG.filename != "stderr":
        # out it on stderr as well
        printErr(" ".join([str(x) for x in args]))
