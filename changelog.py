#
# Copyright (c) 2004-2005 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Implements changelog entries for repository commits.
"""

import os
import streams
import string
import tempfile

class AbstractChangeLog(streams.TupleStream):

    __slots__ = [ 'items' ]
    makeup = ( ("name",    streams.StringStream, "B"), 
	       ("contact", streams.StringStream, "B"),
	       ("message", streams.StringStream, "!H") )

    def getName(self):
	return self.items[0]()

    def setName(self, value):
	return self.items[0].set(value)

    def getContact(self):
	return self.items[1]()

    def setContact(self, value):
	return self.items[1].set(value)

    def getMessage(self):
	return self.items[2]()

    def setMessage(self, value):
	assert(not value or value[-1] == '\n')
	return self.items[2].set(value)

    def freeze(self, skipSet = None):
	if self.items[0]() or self.items[1]() or \
	   self.items[2]():
	    return streams.TupleStream.freeze(self)
	else:
	    return ""

    def getMessageFromUser(self):
	editor = os.environ.get("EDITOR", "/bin/vi")
	(fd, name) = tempfile.mkstemp()
	msg = "\n-----\nEnter your change log message.\n"
	os.write(fd, msg)
	os.close(fd)

	os.system("%s %s" % (editor, name))

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

    def __eq__(self, other, skipSet = None):
	if not isinstance(other, AbstractChangeLog):
	    return False

	return self.items == other.items

    def __init__(self, data = None):
	if data == "": data = None
	streams.TupleStream.__init__(self, data)

class ChangeLog(AbstractChangeLog):

    __slots__ = [ 'items' ]

    def __init__(self, name, contact = None, message = None):
        if contact is None:
            AbstractChangeLog.__init__(self, data = name)
        else:
            assert(not message or message[-1] == '\n')

            AbstractChangeLog.__init__(self)

            self.setName(name)
            self.setContact(contact)
            self.setMessage(message)

class ThawChangeLog(AbstractChangeLog):

    __slots__ = [ 'items' ]

    pass
