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
Use.pcre = True
Use.pcre.setLongDoc('Use the Perl-compatible regex library')
Use.pcre.setLongDoc('This is a long description.  It has a lot of words, '
                    'background, etc.  blah blah blah')
Use.gcj = True
Use.gcj.setShortDoc('Include gcj (Java) support in gcc; use gcj to enable Java')
Use.gnat = False
Use.selinux = False
Use.pam = True
Use.dietlibc = False
Use.bootstrap = False
Use.python = True
Use.perl = True
Use.readline = True
Use.gdbm = True
# flags to use for special situations
Use.builddocs = True	# embedded targets should have False
# temporarily disabled until we build appropriate packages
Use.tcl = False
Use.tk = False
Use.X = False
Use.gtk = False
Use.gnome = False
Use.kde = False
Use.ssl = False
Use.slang = False
Use.netpbm = False
Use.nptl = False
Use._freeze()
_addDocs(Use)

__doc__ += """
@type Arch: UseClass
@var Arch: Set of architectures defined for this build, with their boolean status
The Arch flags have the following meanings:
"""
Arch = UseClass()
Arch.i386 = True
Arch.i486 = True
Arch.i586 = True
Arch.i686 = True
Arch.x86_64 = False
Arch.sparc = False
Arch.sparc64 = False
Arch.ppc64 = False
Arch.ia64 = False
Arch.s390 = False
Arch.s390x = False
Arch.x86 = Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686
Arch._freeze()
_addDocs(Arch)

