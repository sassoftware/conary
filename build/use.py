#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Provides the build configuration as special dictionaries that directly
export their namespaces.

Should read, or be provided, some sort of configuration information
relative to the build being done.  For now, we'll intialize a static
configuration sufficient to build.

@var Use: Set of flags defined for this build, with their boolean status
@type Use: UseClass
@var Arch: Set of architectures defined for this build, with their boolean status
@type Arch: UseClass
"""

class UseClass(dict):
    """
    Immutable dictionary
    """
    def __init__(self, d):
	self.freeze = 0
	self.update(d)
	self.freeze = 1

    def __setitem__(self, key, value):
	if self.freeze:
	    raise TypeError, 'cannot modify immutable dictionary FIXME reference'
	dict.__setitem__(self, key, value)

    def __getattr__(self, attr):
        return self[attr]

Use = UseClass({
    'pcre':		True,
    'gcj':		True,
    'gnat':		False,
    'selinux':		False,
    'pam':		True,
    'dietlibc':		False,
    'bootstrap':	False,
    'python':		True,	# XXX should this even be an option?
    # temporarily disabled until we build appropriate packages
    'perl':		False,
    'tcl':		False,
    'tk':		False,
    'X':		False,
    'gtk':		False,
    'gnome':		False,
    'kde':		False,
    'readline':		False,
    'ssl':		False,
    'slang':		False,
})

Arch = UseClass({
    'i386':		True,
    'i486':		True,
    'i586':		True,
    'i686':		True,
    'x86_64':		False,
    'sparc':		False,
    'sparc64':		False,
    'ppc64':		False,
    'ia64':		False,
    's390':		False,
    's390x':		False,
})
