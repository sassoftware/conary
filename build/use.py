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

class Flag:
    def __init__(self, value):
        self.value = value
        self.short = ""
        self.long = ""

    def setShortDoc(self, doc):
        self.short = doc

    def setLongDoc(self, doc):
        self.long = doc

    def set(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)

    def __coerce__(self, other):
        if type(other) is Flag:
            other = other.value
        return (self.value, other)

class UseClass:
    """
    Implements a simple object that contains boolean flags objects.
    Magic is used to make the initialization of the object easy.
    """

    def __init__(self):
        self.initialized = False
	self.frozen = False
        self.flags = {}
        self.initialized = True

    def _freeze(self):
	self.frozen = True

    def _thaw(self):
	self.frozen = False

    def __setitem__(self, key, value):
	if self.frozen:
	    raise TypeError, 'flags are frozen'
        if self.flags.has_key(key):
            self.flags[key].set(bool(value))

    def __repr__(self):
        return repr(self.flags)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        flags = self.__dict__.get('flags', {})
        if flags.has_key(name):
            return flags[name]
        return getattr(self. name)

    def addFlag(self, name, value):
        if self.frozen:
            raise TypeError, 'flags are frozen'
        self.flags[name] = Flag(value)

    def __setattr__(self, name, value):
        initialized = self.__dict__.get('initialized', False)
        if not initialized:
            self.__dict__[name] = value
            return
        if self.__dict__.has_key(name):
            self.__dict__[name] = value
            return
        frozen = self.__dict__.get('frozen', False)
        if frozen:
            raise TypeError, 'flags are frozen'
        if self.flags.has_key(name):
            self.flags[name].set(value)
        else:
            self.flags[name] = Flag(value)

def _addDocs(obj):
    global __doc__
    keys = obj.flags.keys()
    keys.sort()
    for key in keys:
        flag = obj.flags[key]
        desc = flag.short
        if not desc:
            desc = '%s flag' %key
        __doc__ += '  - C{%s}: %s.  Default=C{%s}\n'% (key, desc, str(flag.value))
        if flag.long:
            __doc__ += '      - ' + flag.long + '\n'

__doc__ += """
@type Use: UseClass
@var Use: Set of flags defined for this build, with their boolean status.
The Use flags have the following meanings:
"""
Use = UseClass()
Use.addFlag('pcre', True)
Use.pcre.setLongDoc('Use the Perl-compatible regex library')
Use.pcre.setLongDoc('This is a long description.  It has a lot of words, '
                    'background, etc.  blah blah blah')
Use.addFlag('gcj', True)
Use.gcj.setShortDoc('Include gcj (Java) support in gcc; use gcj to enable Java')
Use.addFlag('gnat', False)
Use.addFlag('selinux', False)
Use.addFlag('pam', True)
Use.addFlag('dietlibc', False)
Use.addFlag('bootstrap', False)
Use.addFlag('python', True)
Use.addFlag('perl', True)
Use.addFlag('readline', True)
Use.addFlag('gdbm', True)
# flags to use for special situations
Use.addFlag('builddocs', True)	# embedded targets should have False
# temporarily disabled until we build appropriate packages
Use.addFlag('tcl', False)
Use.addFlag('tk', False)
Use.addFlag('X', False)
Use.addFlag('gtk', False)
Use.addFlag('gnome', False)
Use.addFlag('kde', False)
Use.addFlag('ssl', False)
Use.addFlag('slang', False)
Use.addFlag('netpbm', False)
Use.addFlag('nptl', False)
Use._freeze()

_addDocs(Use)

Arch = UseClass()
Arch.addFlag('i386', True)
Arch.addFlag('i486', True)
Arch.addFlag('i586', True)
Arch.addFlag('i686', True)
Arch.addFlag('x86_64', False)
Arch.addFlag('sparc', False)
Arch.addFlag('sparc64', False)
Arch.addFlag('ppc64', False)
Arch.addFlag('ia64', False)
Arch.addFlag('s390', False)
Arch.addFlag('s390x', False)
Arch.addFlag('x86', Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686)
Arch._freeze()

__doc__ += """
@type Arch: ArchClass
@var Arch: Set of architectures defined for this build, with their boolean status
The Arch flags have the following meanings:
"""
_addDocs(Arch)

