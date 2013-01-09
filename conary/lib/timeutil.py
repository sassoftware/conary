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
