#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Provides the build configuration as special dictionaries that directly
export their namespaces.

Should read, or be provided, some sort of configuration information
relative to the build being done.  For now, we'll intialize a static
configuration sufficient to build.

"""

class Flag(dict):
    """
    Implements a dictionary which also has its own value; used to
    create hierarchical dictionaries.  It also may contain a
    short summary (a sentence fragment) and a longer description
    (can be multiple paragraphs) of documentation.

    Magic is used to make the initialization of the object easy.
    """
    def __init__(self, value=None, showdefaults=True):
	self._showdefaults = showdefaults
        self._value = value
        self._short = ""
        self._long = ""
	self._frozen = False
        self._track = False
        self._usedFlags = {}
	self._overrides = {}
        # this must be set last
        self._initialized = True

    def setShortDoc(self, doc):
        self._short = doc

    def setLongDoc(self, doc):
        self._long = doc

    def _set(self, value):
        self._value = value

    def __repr__(self):
	if self._value == None:
	    # top-level flag, no point in printing out None...
	    return repr(self.copy())
	if self.values():
	    return repr(self._value) + ': ' + repr(self.copy())
	else:
	    return repr(self._value)

    def __ror__(self, other):
        if type(other) is Flag:
            other = other._value
        return self._value | other

    def __or__(self, other):
	return self.__ror__(other)

    def __rand__(self, other):
        if type(other) is Flag:
            other = other._value
        return self._value & other

    def __and__(self, other):
	return self.__rand__(other)

    def __nonzero__(self):
        return self._value

    def _freeze(self):
	self._frozen = True

    def _thaw(self):
	self._frozen = False

    def _override(self, key, value):
	self._overrides[key] = value

    def getUsed(self):
        return self._usedFlags

    def trackUsed(self, val):
        self._track = val

    def __setitem__(self, key, value):
	if self._frozen:
	    raise TypeError, 'flags are frozen'
        if key in self:
            self[key]._set(bool(value))
        else:
            dict.__setitem__(self, key, value)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if name in self or name in self._overrides:
	    if name in self._overrides:
		flag = self._overrides[name]
	    else:
		flag = self[name]
            if self._track:
                self._usedFlags[name] = flag
            return flag
        raise AttributeError, "class %s has no attribute '%s'" % (self.__class__.__name__, name)

    def __setattr__(self, name, value):
        initialized = self.__dict__.get('_initialized', False)
        # this allows us to add instance variables during __init__
        if not initialized:
            self.__dict__[name] = value
            return
        # after init, only set instance variables that already exist
        if name in self.__dict__:
            self.__dict__[name] = value
            return
        # everything else should be handled as a Use flag
        if self._frozen:
            raise TypeError, 'flags are frozen'
        if name in self:
            self[name]._set(value)
        else:
            self[name] = Flag(value)


def _addShortDoc(baseobj, obj, keys, level=1):
    global __doc__
    for key in keys:
        flag = obj[key]
	dflt = ''
	if baseobj._showdefaults:
	    dflt = 'Default=C{%s}; ' %str(flag._value)
        desc = flag._short
        if not desc:
            desc = '%s flag' %key
        __doc__ += ' '*(2*level) + '- B{C{%s}}: %s%s.\n'% (key, dflt, desc)
	newkeys = flag.keys()
	if newkeys:
	    newkeys.sort()
	    _addShortDoc(baseobj, flag, newkeys, level=level+1)

def _addLongDoc(baseobj, obj, keys, prefix=''):
    global __doc__
    for key in keys:
        flag = obj[key]
        if flag._long:
            __doc__ += 'B{C{'+key+'}}: ' + flag._long + '\n\n'
	newkeys = flag.keys()
	if newkeys:
	    newkeys.sort()
	    if prefix:
		newprefix = '%s.%s' %(prefix, key)
	    else:
		newprefix = key
	    _addLongDoc(baseobj, flag, newkeys, newprefix)

def _addDocs(obj):
    global __doc__
    if __doc__ is None:
        return
    keys = obj.keys()
    keys.sort()
    _addShortDoc(obj, obj, keys)
    __doc__ += '\n\nMore details:\n\n'
    _addLongDoc(obj, obj, keys)


if __doc__ is not None:
    __doc__ += """
@sort: Use, Arch
@type Use: Flag
@var Use: Set of flags defined for this build, with their boolean status.
The Use flags have the following meanings:
"""
Use = Flag(showdefaults=True)

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

Use.buildtests = True
Use.buildtests.setShortDoc('Build test suites')
Use.builddocs.setLongDoc("""
Conary supports the installation of build-time test suites in a
manner that allows them to be run later, using the installed
package.  However, testsuites often require the compilation of
extra files and extra post processing.  Use this flag to turn
off building testsuites.
""")


# temporarily disabled until we build appropriate packages
Use.alternatives = False
Use.tcl = True
Use.tk = True
Use.X = True
Use.gtk = True
Use.gnome = True
Use.qt = False
Use.kde = False
Use.xfce = False
Use.gd = False
Use.ldap = True
Use.sasl = False
Use.sasl.setShortDoc('Build with support for SASL Simple Authenication '
                     'and Security Layer')
Use.pie = False

Use.desktop = Use.gnome | Use.kde | Use.xfce
Use.desktop.setShortDoc('Build with support for freedesktop.org specs')
Use.desktop.setLongDoc("""
Set if any graphical desktop platform/environment that attempts to conform
to the freedesktop.org specifications is enabled.  In particular, desktop
and menu entries and the shared mime database at this time.  This flag
should mediate dependence on implementation of these capabilities.
""")

Use.ssl = True
Use.slang = False
Use.netpbm = False
Use.nptl = False
Use.ipv6 = True
Use._freeze()
_addDocs(Use)

if __doc__ is not None:
    __doc__ += """
@type Arch: Flag
@var Arch: Set of architectures defined for this build, with their boolean status.
The Arch flags have the following meanings:
"""

# All Arch flags default to False; deps/arch.py sets any that should be
# True to True
Arch = Flag(showdefaults=False)
# Arch.x86 = Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686 | Arch.x86_64
Arch.x86 = True
Arch.x86.setShortDoc('True if any IA32-compatible architecture is set')
Arch.x86.i386 = False
Arch.x86.i486 = False
Arch.x86.i586 = False
Arch.x86.i686 = True
Arch.x86.x86_64 = False
Arch.x86.cmov = False
Arch.x86.sse = False
Arch.x86.sse2 = False
Arch.x86.mmx = False
Arch.x86.threednow = False # '3dnow' is an illegal identifier name
Arch.sparc = False
Arch.sparc.sparc64 = False
Arch.ppc = False
Arch.ppc.ppc64 = False
Arch.ia64 = False
Arch.s390 = False
Arch.s390.s390x = False
Arch.alpha = False
# Arch.LE = Arch.x86 | Arch.ia64
Arch.LE = True
Arch.LE.setShortDoc('True if current architecture is little-endian')
# Arch.BE = Arch.sparc | Arch.ppc | Arch.s390
Arch.BE = False
Arch.BE.setShortDoc('True if current architecture is big-endian')
Arch.bits32= True
Arch.bits32.setShortDoc('True if the current architecture is 32-bit')
Arch.bits64 = False
Arch.bits64.setShortDoc('True if the current architecture is 64-bit')
Arch._freeze()
_addDocs(Arch)

LocalFlags = Flag(showdefaults=False)

def track(arg):
    """
    Turns Use flag tracking on or off.
    """
    Arch.trackUsed(arg)
    Use.trackUsed(arg)
    LocalFlags.trackUsed(arg)

def overrideFlags(config, pkgname):
    Use._thaw()
    for key in config.useKeys():
	Use._override(key, config['Use.' + key])

    for key in config.archKeys():
	flags = key.split('.')
	lastflag = flags[-1]
	flags = flags[:-1]
	curflag = Arch
	for flag in flags:
	    curflag = curflag[flag]
	curflag._override(lastflag, config['Arch.' + key])

    prefix = 'Flags.%s.' % pkgname
    for key in config.pkgKeys(pkgname):
	LocalFlags._override(key, config[prefix + key])

    Use._freeze()
