#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Provides PackageName object.
"""

class PackageName:

    """
    Class representing package and group names.
    """

    def __str__(self):
	return self.name

    def getName(self, namespace = None):
	if namespace and self.name.startswith(namespace + ":"):
	    return self.name[len(namespace) + 1:]

	return self.name

    def isGroup(self):
	return self.name.count(":") == 2

    def isPackage(self):
	return not self.isGroup()

    def __init__(self, name):
	assert(name[0] == ":")
	self.name = name
