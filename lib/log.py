#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Implements the logging facility for srs
"""

import logging
import sys

def error(*args):
    "Log an error"
    logger.error(*args)

def warning(*args):
    "Log a warning"    
    logger.warning(*args)

def info(*args):
    "Log an informative message"
    logger.info(*args)

def debug(*args):
    "Log a debugging message"
    logger.debug(*args)

if not globals().has_key("logger"):
    logging.addLevelName(logging.WARNING, "warning:")
    logging.addLevelName(logging.ERROR, "error:")
    logging.addLevelName(logging.INFO, "info:")
    logging.addLevelName(logging.DEBUG, "+")
    logger = logging.getLogger('srs')
    hdlr = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.DEBUG)
