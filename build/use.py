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
        # this is an update, the description needs to be preserved
        if self.has_key(key):
            value = (self[key][0], value)
	dict.__setitem__(self, key, value)

    def __getattr__(self, attr):
        return self[attr][1]

def _addDocs(obj):
    global __doc__
    keys = obj.keys()
    keys.sort()
    for key in keys:
        value = obj[key]
        desc = value[0]
        if not desc:
            desc = '%s flag' %key
        __doc__ += "  - C{%s}: %s.  Default=C{%s}\n" % (key, desc, str(value[1]))

Use = UseClass({
    'pcre':		('Use the Perl-compatible regex library', True),
    'gcj':		('Include gcj (Java) support in gcc; use '
                         'gcj to enable Java)', True),
    'gnat':		('', False),
    'selinux':		('', False),
    'pam':		('', True), 
    'dietlibc':		('', False),
    'bootstrap':	('', False),
    'python':		('', True),
    'perl':		('', True),
    'readline':		('', True),
    'gdbm':		('', True),
    # flags to use for special situations
    'builddocs':	('', True),	# embedded targets should have False
    # temporarily disabled until we build appropriate packages
    'tcl':		('', False),
    'tk':		('', False),
    'X':		('', False),
    'gtk':		('', False),
    'gnome':		('', False),
    'kde':		('', False),
    'ssl':		('', False),
    'slang':		('', False),
    'netpbm':		('', False),
    'nptl':		('', False),
})
Use._freeze()
__doc__ += """
@type Use: UseClass
@var Use: Set of flags defined for this build, with their boolean status.
The Use flags have the following meanings:
"""
_addDocs(Use)

Arch = UseClass({
    'i386':		('', True),
    'i486':		('', True),
    'i586':		('', True),
    'i686':		('', True),
    'x86_64':		('', False),
    'sparc':		('', False),
    'sparc64':		('', False),
    'ppc64':		('', False),
    'ia64':		('', False),
    's390':		('', False),
    's390x':		('', False),
})
Arch['x86'] = ('', (Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686))
Arch._freeze()

__doc__ += """
@type Arch: UseClass
@var Arch: Set of architectures defined for this build, with their boolean status
The Arch flags have the following meanings:
"""
_addDocs(Arch)
