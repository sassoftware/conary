# classes for version structures and strings

import string

class Version:

    def __str__(self):
	if self.child:
	    return "/" + self.formatValue() + str(self.child)
	else:
	    return "/" + self.formatValue()

class BranchVersion(Version):

    def formatValue(self):
	return self.value

    def __init__(self, value, child):
	self.value = value
	self.child = child

class HostnameVersion(BranchVersion):

    def __init__(self, value, child):
	if not child:
	    raise KeyError, "hostnames must have children"
	BranchVersion.__init__(self, value, child)

class VersionRelease(Version):

    def formatValue(self):
	return self.version + '-' + str(self.release)

    def __init__(self, value, child):
	# throws an exception if no - is found
	cut = value.index("-")
	self.version = value[:cut]
	self.release = value[cut + 1:]
	self.child = child
	if self.release.find("-") != -1:
	    raise KeyError, ("version numbers may not have hyphens: %s" % value)

	try:
	    int(self.version[0])
	except:
	    raise KeyError, ("version numbers must be begin with a digit: %s" % value)

	try:
	    self.release = int(self.release)
	except:
	    raise KeyError, ("release numbers must be all numeric: %s" % value)
	
def VersionFromString(str):
    parts = string.split(str, "/")
    if parts[0]:
	raise KeyError, ("relative versions are not yet supported: %s" % str)
    del parts[0]
    parts.reverse()

    if len(parts) % 3 == 0:
	# full version specification
	v = None
    elif len(parts) % 3 == 2:
	# specifies a branch, build the last two items before handling the
	# rest
	v = BranchVersion(parts[0], None)
	v = HostnameVersion(parts[1], v)
	parts = parts[2:]
    elif len(parts) % 3 == 1:
	raise KeyError, ("invalid version string: %s" % str)

    # we know parts is divisible by 3 at this point
    while len(parts):
	v = VersionRelease(parts[0], v)
	v = BranchVersion(parts[1], v)
	v = HostnameVersion(parts[2], v)
	parts = parts[3:]

    return v
	
