class ChangeLog:

    def freeze(self):
	return "%s\n%s\n%s" % (self.name, self.email, self.message)

    def __eq__(self, other):
	return self.__class__ == other.__class__ and \
	       self.name == other.name		and \
	       self.email == other.email	and \
	       self.message == other.message

    def __init__(self, name, email, message):
	assert(message[-1] == '\n')

	self.name = name
	self.email = email
	self.message = message

def ThawChangeLog(frzLines):
    name = frzLines[0]
    email = frzLines[1]
    message = "\n".join(frzLines[2:]) + "\n"
    return ChangeLog(name, email, message)
