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
    """
    Implements the object for a single flag; contains the value
    of the flag, and optionally a short summary (a sentence
    fragment) and a longer description (can be multiple paragraphs).
    """
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

    def __nonzero__(self):
        return self.value

class UseClass(dict):
    """
    Implements a simple object that contains boolean flags objects.
    Magic is used to make the initialization of the object easy.
    """

    def __init__(self, showdefaults=True):
	self.showdefaults = showdefaults
        self.initialized = False
	self.frozen = False
        self.initialized = True

    def _freeze(self):
	self.frozen = True

    def _thaw(self):
	self.frozen = False

    def __setitem__(self, key, value):
	if self.frozen:
	    raise TypeError, 'flags are frozen'
        if self.has_key(key):
            self[key].set(bool(value))
        else:
            dict.__setitem__(self, key, value)
            
    def __repr__(self):
        return dict.__repr__(self)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if dict.has_key(self, name):
            return self[name]
        raise AttributeError, "class %s has no attribute '%s'" % (self.__class__.__name__, name)

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
        if self.has_key(name):
            self[name].set(value)
        else:
            self[name] = Flag(value)

def _addDocs(obj):
    global __doc__
    if __doc__ is None:
        return
    keys = obj.keys()
    keys.sort()
    for key in keys:
        flag = obj[key]
	dflt = ''
	if obj.showdefaults:
	    dflt = 'Default=C{%s}; ' %str(flag.value)
        desc = flag.short
        if not desc:
            desc = '%s flag' %key
        __doc__ += '  - B{C{%s}}: %s%s.\n'% (key, dflt, desc)
    __doc__ += '\n\nMore details:\n\n'
    for key in keys:
        flag = obj[key]
        if flag.long:
            __doc__ += 'B{C{'+key+'}}: ' + flag.long + '\n\n'


if __doc__ is not None:
    __doc__ += """
@sort: Use, Arch
@type Use: UseClass
@var Use: Set of flags defined for this build, with their boolean status.
The Use flags have the following meanings:
"""
Use = UseClass(showdefaults=True)

Use.pcre = True
Use.pcre.setShortDoc('Use the Perl-compatible regex library')
Use.pcre.setLongDoc("""
The perl-compatible regular expression library can be used by
several programs to increase the power and uniformity of
available regular expressions.  It adds a dependency and should
generally be disabled for embedded builds.
""")

Use.tcpwrappers = False
Use.tcpwrappers.setShortDoc('Use the tcp_wrappers library')

Use.gcj = True
Use.gcj.setShortDoc('Use the gcj implementation of Java')
Use.gcj.setLongDoc("""
Include gcj (Java) support in gcc;
use gcj to enable Java in other applications.
""")

Use.gnat = False
Use.gnat.setShortDoc('Enable the gnat implementation of Ada')
Use.gnat.setLongDoc("""
Include gnat (Ada) support in gcc;
use gnat to enable Ada in other applications.
""")

Use.selinux = False
Use.selinux.setShortDoc('Enable support for SELinux')
Use.selinux.setLongDoc("""
Build support for the Security-Enhanced Linux Role-based Access
Control system.  Adds dependencies on particular filesystems,
Linux 2.6 or later kernel, and multiple SELinux tools.  Unlikely
to be appropriate for deeply embedded systems.
""")

Use.pam = True
Use.pam.setShortDoc('Enable support for PAM')
Use.pam.setLongDoc("""
Pluggable Authentication Modules (PAM) makes most system authentication
happen in a unified way, but adds a dependency on PAM libraries, and
requires shared libraries.  You may want to disable PAM during the
early stages of bootstrapping a new architecture.
""")

Use.dietlibc = False
Use.bootstrap = False
Use.python = True
Use.perl = True
Use.readline = True
Use.gdbm = True

Use.emacs = True
Use.emacs.setShortDoc('Enable support for the EMACS editor')
Use.emacs.setLongDoc("""
Build the EMACS editor, and include support for the EMACS editor
in other packages.  If not Use.emacs, packages should not have
files in the site-lisp directory and should not have an :emacs
component.
""")

Use.krb = True
Use.krb.setShortDoc('Enable support for Kerberos (V5)')
Use.krb.setLongDoc("""
Build the Kerberos package, and include support for the Kerberos
package in other packages.  You may want to disable Kerberos during
the early stages of bootstrapping a new architecture, or to make
a smaller installable image where network single sign-on is not
required.
""")

# flags to use for special situations
Use.builddocs = True
Use.builddocs.setShortDoc('Build documentation as well as binaries')
Use.builddocs.setLongDoc("""
Some packages have documentation that needs to be built, not just
installed.  Examples include SGML, TeX, groff documents other than
man pages, and texinfo (but not precompiled info pages).  This does
not include man pages, precompiled info pages, and other files
installed on the system in essentially the same form they are
provided.  Simple substitution (sed -i 's/@FOO@/foo/) does not
count as "building documentation".

The purpose of this flag is to disable unnecessary build depedencies
for embedded targets.
""")

# temporarily disabled until we build appropriate packages
Use.tcl = False
Use.tk = False
Use.X = False
Use.gtk = False
Use.gnome = False
Use.kde = False
Use.xfce = False
Use.gd = False

Use.desktop = Use.gnome | Use.kde | Use.xfce
Use.desktop.setShortDoc('Building with support for freedesktop.org specs')
Use.desktop.setLongDoc("""
Set if any graphical desktop platform/environment that attempts to conform
to the freedesktop.org specifications is enabled.  In particular, desktop
and menu entries and the shared mime database at this time.  This flag
should mediate dependence on implementation of these capabilities.
""")

Use.ssl = False
Use.slang = False
Use.netpbm = False
Use.nptl = False
Use.ipv6 = True
Use._freeze()
_addDocs(Use)

if __doc__ is not None:
    __doc__ += """
@type Arch: UseClass
@var Arch: Set of architectures defined for this build, with their boolean status.
The Arch flags have the following meanings:
"""
Arch = UseClass(showdefaults=False)
Arch.i386 = True
Arch.i486 = True
Arch.i586 = True
Arch.i686 = True
Arch.x86 = Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686
Arch.x86.setShortDoc('True if any IA32 architecture is set')
Arch.x86_64 = False
Arch.sparc = False
Arch.sparc64 = False
Arch.ppc64 = False
Arch.ia64 = False
Arch.s390 = False
Arch.s390x = False
Arch.s390_all = Arch.s390 | Arch.s390x
Arch.s390_all.setShortDoc('True if s390 or s390x is set')
Arch._freeze()
_addDocs(Arch)

