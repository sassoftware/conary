#
# Copyright (c) 2011 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import logging
import random
import time


class BackoffTimer(object):
    """Helper for functions that need an exponential backoff."""

    factor = 2.7182818284590451
    jitter = 0.11962656472

    def __init__(self, delay=0.1):
        self.delay = delay

    def sleep(self):
        time.sleep(self.delay)
        self.delay *= self.factor
        self.delay = random.normalvariate(self.delay, self.delay * self.jitter)


class ISOFormatter(logging.Formatter):
    """
    Logging formatter for ISO 8601 timestamps with milliseconds.
    """

    def formatTime(self, record, datefmt=None):
        timetup = time.localtime(record.created)
        if timetup.tm_isdst:
            tz_seconds = time.altzone
        else:
            tz_seconds = time.timezone
        tz_offset = abs(tz_seconds / 60)
        tz_sign = (time.timezone < 0 and '+' or '-')

        timestampPart = time.strftime('%F %T', timetup)
        return '%s.%03d%s%02d%02d' % (timestampPart, record.msecs, tz_sign,
                tz_offset / 60, tz_offset % 60)
