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

"""

class UseClass(dict):
    """
    Immutable dictionary
    """
    def __init__(self, d):
	self.frozen = False
	self.update(d)

    def _freeze(self):
	self.frozen = True

    def _thaw(self):
	self.frozen = False

    def __setitem__(self, key, value):
	if self.frozen:
	    raise TypeError, 'cannot modify immutable dictionary FIXME reference'
	dict.__setitem__(self, key, value)

    def __getattr__(self, attr):
        return self[attr]

__doc__ += """
@var Use: Set of flags defined for this build, with their boolean status.
The Use flags have the following meanings:
   - C{pcre}: Use the Perl-compatible regex library
   - C{gcj}: Include gcj (Java) support in gcc; use gcj to enable Java
@type Use: UseClass
"""
Use = UseClass({
    'pcre':		True,
    'gcj':		True,
    'gnat':		False,
    'selinux':		False,
    'pam':		True,
    'dietlibc':		False,
    'bootstrap':	False,
    'python':		True,	# XXX should this even be an option?
    'perl':		True,
    'readline':		True,
    'gdbm':		True,
    # flags to use for special situations
    'builddocs':	True,	# embedded targets should have False
    # temporarily disabled until we build appropriate packages
    'tcl':		False,
    'tk':		False,
    'X':		False,
    'gtk':		False,
    'gnome':		False,
    'kde':		False,
    'ssl':		False,
    'slang':		False,
    'netpbm':		False,
    'nptl':		False,
})
Use._freeze()

__doc__ += """
@var Arch: Set of architectures defined for this build, with their boolean status
The Arch flags have the following meanings:
   - C{i3866}: i386
@type Arch: UseClass
"""
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
Arch['x86'] = (Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686)
Arch._freeze()
