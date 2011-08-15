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

from conary.lib import logger

class XmlHandler(logging.StreamHandler):
    """Xml logging class compatible with python's built in logging structures.

    This class defines an interface to logger.XmlWriter that's compatible
    with python's logging module. Note that logger.XmlWriter expects to write
    a complete log from start to finish. This is because it attempts to produce
    well formed XML. It also employs some compression filters that don't
    define an append mode, so not allowing append is more consistent. This
    deviates from normal python logging assumptions. Use with caution."""
    def __init__(self, path):
        stream = logger.XmlLogWriter(path)
        stream.start()
        logging.StreamHandler.__init__(self, stream)

    def close(self):
        self.stream.flush()
        self.stream.close()
        logging.StreamHandler.close(self)

    def emit(self, record):
        # by forcing a newline as a separate even we ensure that all newlines
        # in the log message will be present but escaped and we will have one
        # line per logging event
        self.stream.log(record.getMessage(), record.levelname)

    def pushDescriptor(self, descriptor):
        return self.stream.pushDescriptor(descriptor)

    def popDescriptor(self, descriptor = None):
        return self.stream.popDescriptor(descriptor)

    def addRecordData(self, key, val):
        return self.stream.addRecordData(key, val)

    def delRecordData(self, key):
        return self.stream.delRecordData(key)
