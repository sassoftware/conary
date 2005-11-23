#
# Copyright (c) 2004-2005 rPath, Inc.
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

"""
Implements the logging facility for conary.

Similar to the C printf function, the functions in this module take a
format string that specifies how the subsequent variables should be
converted in the log output.

For example::
   log.error("%s not found", foo)
"""

import logging
import os
import sys
import time

from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from conary import constants

import util

syslog = None

class SysLog:
    # class responsible for /var/log/conary
    def __call__(self, str, *args):
        "Logs a message to /var/log/conary"
        if not self.f:
            self.open()

        msg = str % args
        self.f.write(time.strftime("[%b %d %H:%M:%S] ") + self.indent)
        self.f.write(msg)
        self.f.write("\n")
        self.f.flush()

    def command(self):
        self(("version %s: " + " ".join(sys.argv[1:])) % 
                                                constants.version)
        self.indent = "  "

    def commandComplete(self):
        self.indent = ""
        self("command complete")

    def traceback(self, lines):
        if not self.f:
            self.open()

        for line in lines:
            self.f.write(line)

        self.indent = ""
        self("command failed")

    def open(self):
        util.mkdirChain(os.path.dirname(self.path))
        self.f = open(self.path, "a")

    def __init__(self, root, path):
        self.path = root + os.path.sep + path
        self.indent = ""
        self.f = None

def openSysLog(root, path):
    global syslog
    if not path:
        path = '/dev/null'
    syslog = SysLog(root, path)

def error(*args):
    "Log an error"
    logger.error(*args)
    hdlr.error = True

def warning(*args):
    "Log a warning"    
    logger.warning(*args)

def info(*args):
    "Log an informative message"
    logger.info(*args)

def debug(*args):
    "Log a debugging message"
    logger.debug(*args)

def errorOccurred():
    return hdlr.error

def resetErrorOccured():
    hdlr.error = False

def setVerbosity(val):
    return logger.setLevel(val)

def getVerbosity():
    return logger.getEffectiveLevel()

class ErrorCheckingHandler(logging.StreamHandler):
    def __init__(self, *args, **keywords):
        self.error = False
        logging.StreamHandler.__init__(self, *args, **keywords)
    
    def emit(self, record):
        logging.StreamHandler.emit(self, record)

if not globals().has_key("logger"):
    logging.addLevelName(logging.WARNING, "warning:")
    logging.addLevelName(logging.ERROR, "error:")
    logging.addLevelName(logging.INFO, "info:")
    logging.addLevelName(logging.DEBUG, "+")
    logger = logging.getLogger('conary')
    hdlr = ErrorCheckingHandler(sys.stderr)
    formatter = logging.Formatter('%(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)
