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

import cfg, cfgtypes

# needed so we know not to log ourselves
_thisFile = os.path.basename(sys.modules[__name__].__file__)

# "static" placeholder for global use
_LOG = None

# Maximum str(arg) length for fully printing complex datatypes
MaxArgLen = 1024

# we log time in milliseconds
def logTime():
    t = time.time()
    ret = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(t))
    ret = ret + ".%03d" % (int((t-int(t))*1000),)
    return ret

# this is used to dump a line to stderr
def printErr(*args):
    pid = os.getpid()
    for arg in args:
        sys.stderr.write("%s %s: %s\n" % (
            pid, logTime(), arg))
    sys.stderr.flush()

# basic class that doesn't log much
class NullLog:
    def __init__(self, filename = None, level = 0):
        self.pid = os.getpid()
        self.level = level
        self.filename = filename
    def log(self, *args):
        pass
    # placeholder for something that can be useful for more complex classes
    def reset(self, level=None):
        if level:
            self.level = level
    def close(self):
        pass
    # log arguments if we're called as a function
    def __call__(self, level, *args):
        if level <= self.level:
            self.log(level, *args)

# Base logging class that figures out where it is being called from
class FileLog(NullLog):
    def __init__(self, filename="stderr", level=1):
        NullLog.__init__(self, filename, level)
        def __getFD():
            isFile = 0
            if self.filename in ["stderr", "stdout"]:
                return (isFile, getattr(sys, self.filename))

            mustChmod = not os.path.exists(self.filename)
            try: # line buffered mode
                fd = open(self.filename, "a+", 1)
                if mustChmod:
                    os.chmod(self.filename, 0660)
            except:
                printErr("ERROR: Couldn't open log file %s" % (self.filename,),
                       sys.exc_info()[:2])
                self.filename = "stderr"
                fd = sys.stderr
            else:
                isFile = 1
            return (isFile, fd)
        (self.isFile, self.fd) = __getFD()
        self.reset()
        self.log(0, "logging level %d for pid %d on '%s'" % (
            self.level, self.pid, self.filename))

    def argStr(self, val):
        global MaxArgLen
        ret = str(val)
        # for really verbose logging levels, print everything out
        if self.level > 3:
            return ret
        # otherwise, of this takes too much space, just print it's length
        if type(val) in set([list,tuple,dict,set]) and len(ret) > MaxArgLen:
            if len(val) > 1:
                ret = "len(%s)=%d" % (type(val), len(val))
        return ret

    # python cookbooks are great
    def log(self, level=0, *args):
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
        location = tbStack[callid][2]
        if location == "?":
            location = "__main__"
        msg = "%s.%s" % (module, location)
        if len(args):
            msg = "%s %s" % (msg, " ".join([self.argStr(x) for x in args]))
        # finally, output the log
        self.printLog(level, msg)

    # this function is mainly here so we can subclass it (ie, tracing)
    def printLog(self, level, msg):
        try:
            self.fd.write("%s %d %s\n" % (logTime(), self.pid, msg))
        except OSError, e:
            pass

    # close on exit
    def close(self):
        if self.isFile:
            self.fd.close()
        self.level = self.filename = self.isFile = self.fd = None
    def __del__(self):
        self.close()


# a class that keeps tabs on the time spend between calls to the log function
class TraceLog(FileLog):
    def __init__(self, filename = "stderr", level = 1):
        self.times = [time.time()] * level
        FileLog.__init__(self, filename, level)

    def printLog(self, level, msg):
        t = time.time()
        timeStr = time.strftime("%H:%M:%S", time.localtime(t))
        timeIdx = max(level-1, 0)
        lineStr = "%d %s %+.3f %s" % (
            self.pid, timeStr, t - self.times[timeIdx], msg)
        # reset the timers for the next call
        for i in range(timeIdx, self.level):
            self.times[i] = t
        try:
            self.fd.write("%s\n" % (lineStr,))
        except OSError, e:
            pass

    def reset(self, level=None):
        FileLog.reset(self, level)
        self.times = [time.time()] * self.level

# instantiate a log object
def getLog(filename = "stderr", level = 0, trace = 0):
    if filename is None:
        return NullLog(None, 0)
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
        return TraceLog(filename, level)
    return FileLog(filename, level)


# initialize the module global log
def initLog(filename = "stderr", level = 0, trace = 0):
    global _LOG

    if _LOG is not None:
        if filename is None or _LOG.filename == filename:
            _LOG.reset(level)
            return
        _LOG = None
    elif filename is None:
        filename = "/dev/null"
    _LOG = getLog(filename, level, trace)
    return 0

# the basic function to log stuff using this module
def logMe(level, *args):
    global _LOG
    if _LOG and _LOG.level >= level:
        _LOG.log(0, *args)

# shortcut for error logging
def logErr(*args):
    global _LOG
    if not args:
        return
    if _LOG:
        _LOG.log(0, *(("ERROR",) + args))
    if _LOG.filename != "stderr":
        # out it on stderr as well
        printErr(" ".join([str(x) for x in args]))

# A class for configuration of a database driver
class CfgTraceLog(cfg.CfgType):
    def parseString(self, str):
        s = str.split()
        if len(s) != 2:
            raise cfgtypes.ParseError("log level and path expected")
        try:
            s = (int(s[0]), cfgtypes.Path(s[1]))
        except Exception, e:
            raise cfgtypes.ParseError(
                "log level (integer) and path (string) expected\n%s" % (e,))
        return s
    def format(self, val, displayOptions = None):
        return "%s %s" % val
