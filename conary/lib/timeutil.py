#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
