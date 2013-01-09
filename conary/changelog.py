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


"""
Implements changelog entries for repository commits.
"""

import os
import sys
import string
import subprocess
import tempfile

from conary import streams

_CHANGELOG_NAME    = 1
_CHANGELOG_CONTACT = 2
_CHANGELOG_MESSAGE = 3

SMALL = streams.SMALL
LARGE = streams.LARGE

class ChangeLog(streams.StreamSet):

    streamDict = {
        _CHANGELOG_NAME    : (SMALL, streams.StringStream, "name"    ),
        _CHANGELOG_CONTACT : (SMALL, streams.StringStream, "contact" ),
        _CHANGELOG_MESSAGE : (SMALL, streams.StringStream, "message" )
        }

    __slots__ = [ 'name', 'contact', 'message' ]

    def getName(self):
        return self.name()

    def setName(self, value):
        self.name.set(value)

    def getContact(self):
        return self.contact()

    def setContact(self, value):
        self.contact.set(value)

    def getMessage(self):
        return self.message()

    def setMessage(self, value):
        assert(not value or value[-1] == '\n')
        self.message.set(value)

    def getMessageFromUser(self, prompt=''):
        editor = os.environ.get("EDITOR", "/bin/vi")
        (fd, name) = tempfile.mkstemp()
        if not prompt:
            prompt = 'Enter your change log message.'
        msg = "\n-----\n%s\n" % prompt
        os.write(fd, msg)
        os.close(fd)

        def _getMessageNoEditor():
            sys.stderr.write("Error executing %s. Please set the EDITOR\n"
              "environment variable to a valid editor, or enter log message,\n"
              "terminated with single '.' (or CTRL+D to cancel)\n" % editor)
            rows = []
            while 1:
                try:
                    row = raw_input('>> ')
                except EOFError:
                    return None
                if row == '.':
                    # We need a trailing newline
                    rows.append('')
                    break
                rows.append(row)
            return '\n'.join(rows)

        class EditorError(Exception):
            pass

        cmdargs = [editor, name]
        try:
            try:
                # Capture stderr and discard it
                retcode = subprocess.call(" ".join(cmdargs), shell=True,
                    stderr=subprocess.PIPE)
            except OSError:
                raise EditorError
            if retcode != 0:
                raise EditorError
        except EditorError:
            # Error running the editor
            msg = _getMessageNoEditor()
            if msg is None:
                return False
            self.message.set(msg)
            return True

        newMsg = open(name).read()
        os.unlink(name)

        if newMsg == msg:
            return False

        if newMsg[-len(msg):]:
            newMsg = newMsg[:-len(msg)]

        newMsg = string.strip(newMsg)
        newMsg += '\n'
        self.setMessage(newMsg)
        return True

    def __init__(self, name = None, contact = None, message = None):
        if contact is None:
            streams.StreamSet.__init__(self, data = name)
        else:
            assert(not message or message[-1] == '\n')

            streams.StreamSet.__init__(self)

            self.setName(name)
            self.setContact(contact)
            self.setMessage(message)
