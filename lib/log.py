import logging
import sys

def error(message):
    logger.log(logging.ERROR, message)

def warning(message):
    logger.warning(message)

if not globals().has_key("logger"):
    logging.addLevelName(logging.WARNING, "warning")
    logging.addLevelName(logging.ERROR, "error")
    logger = logging.getLogger('srs')
    hdlr = logging.StreamHandler(sys.stderr)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.setLevel(logging.WARNING)
