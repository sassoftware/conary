import logging
import sys

def error(*args):
    logger.error(*args)

def warning(*args):
    logger.warning(*args)

def info(*args):
    logger.info(*args)

def debug(*args):
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
