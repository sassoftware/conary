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
