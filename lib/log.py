#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
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
import sys

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
    if val < 1:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.DEBUG)

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
