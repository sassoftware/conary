#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Provides the build configuration as a special dictionary that directly
exports its namespace.
"""

class UseClass(dict):
    def __init__(self, d):
	"""
	Should read, or be provided, some sort of configuration information
	relative to the build being done.  For now, we'll intialize a static
	configuration.
	"""
	self.update(d)

    def __getattr__(self, attr):
        return self[attr]

Use = UseClass({
    'foo': True,
    'bar': False,
})

Arch = UseClass({
    'i386': True,
    'i486': True,
    'i586': True,
    'i686': True,
    'sparc': False,
    'ppc': False,
})
