import os
import string
import tempfile

class ChangeLog:

    def freeze(self):
	assert(self.message[-1] == '\n')
	return "%s\n%s\n%s" % (self.name, self.contact, self.message)

    def getMessage(self):
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
	self.message = newMsg
	return True

    def __eq__(self, other):
	return self.__class__ == other.__class__ and \
	       self.name == other.name		and \
	       self.contact == other.contact	and \
	       self.message == other.message

    def __init__(self, name, contact, message):
	assert(not message or message[-1] == '\n')

	self.name = name
	self.contact = contact
	self.message = message

def ThawChangeLog(frzLines):
    name = frzLines[0]
    contact = frzLines[1]
    message = "\n".join(frzLines[2:]) + "\n"
    return ChangeLog(name, contact, message)
