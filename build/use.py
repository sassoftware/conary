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
from deps import deps
from lib import log

class Flag(dict):
    """
    Implements a dictionary which also has its own value; used to
    create hierarchical dictionaries.  It also may contain a
    short summary (a sentence fragment) and a longer description
    (can be multiple paragraphs) of documentation.

    Magic is used to make the initialization of the object easy.
    """
    def __init__(self, value=None, name=None, showdefaults=True, parent=None, 
                 createOnAccess=False, required=True, track=False):
	self._showdefaults = showdefaults
        assert(isinstance(value, bool) or value is None)
        self._value = value
        self._short = ""
        self._long = ""
        self._parent = parent
        self._name = name
	self._frozen = False
        self._track = track
        self._used = False
	self._overrides = {}
	self._deprecated = {}
        self._createOnAccess = createOnAccess
        self._required = required
        # this must be set last
        self._initialized = True

    def setShortDoc(self, doc):
        self._short = doc

    def setLongDoc(self, doc):
        self._long = doc

    def setRequired(self, required):
        self._required = required

    def getRequired(self):
        return self._required

    def _set(self, value):
        assert(isinstance(value, bool) or value is None)
        self._value = value

    def _get(self):
        # don't track this access to _value
        return self._value

    def __repr__(self):
	if self._value == None:
	    # top-level flag, no point in printing out None...
	    return repr(self.copy())
	if self.values():
	    return repr(self._value) + ': ' + repr(self.copy())
	else:
	    return repr(self._value)

    def __eq__(self, other):
        if not isinstance(other, (Flag, bool)):
            # if other is not a bool or a flag, don't even try to 
            # check equivalency
            return False
        return bool(self) == bool(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __ror__(self, other):
        return bool(self) | other

    def __or__(self, other):
	return self.__ror__(other)

    def __rand__(self, other):
        return bool(self) & other

    def __and__(self, other):
	return self.__rand__(other)

    def __nonzero__(self):
        if not isinstance(self._value, bool):
            raise RuntimeError, ('checking nonzero of %s,'
                                 ' which is not bool' % self._name)
        if self._track and self._value is not None:
            self._used = True
        return self._value

    def _freeze(self):
	self._frozen = True

    def _thaw(self):
	self._frozen = False

    def _override(self, key, value):
        if key in self:
            self[key]._set(bool(value))
        else:
            # override flag values that haven't been entered yet
            self[key] = Flag(value=bool(value), name=key, parent=self,
                createOnAccess=self._createOnAccess, required=self._required,
                track=self._track)
	self._overrides[key] = bool(value)

    def _clearOverrides(self):
        self._overrides = {}

    def __delattr__(self, key):
        """ Remove a flag from this flag set """
        if self._frozen:
	    raise TypeError, 'flags are frozen'
        del self[key]

    def _addEquivalentFlagSets(self, other, parent=None):
        """ Add together two sets of flags or flag sets, assuming that 
            the sets being added have the same name and context,
            and differ only in value or in child flags.
        """
        assert(other._name == self._name)
        # RHS value (other) is propogated 
        # use other's value unless it is not set, in which case
        # use our value
        value = other._value
        if value is None:
            value = self._value
        new = Flag(value=value, name=other._name, parent=parent)
        # overrides and usedFlags are combined
        # from both
        new._overrides = self._overrides.copy()
        new._overrides.update(other._overrides)
        new._used = (self._used or other._used)
        
        for key in self.keys():
            if key in other:
                # if the flag is in both self and other sets, recurse
                new[key] = self[key]._addEquivalentFlagSets(other[key], new)
            else:
                new[key] = self[key].deepCopy(new)
        
        for key in other.keys():
            if key in self:
                continue
            new[key] = other[key].deepCopy(new)
        return new

    def __add__(self, other):
        """ Add together two flags or flag sets.  Where a and b are flag sets,
            if a and b have non-overlapping flags set, a + b is the union 
            is a set of flags with the union of a and b flag set.
            For any overlapping flags, b value overrides a. 
            If a or b is a single flag instead of a set, it is converted
            to a flag set before addition """
        # make sure these are equivalent flag sets
        if self._name != '__GLOBAL__':
            self = self.asSet()
        if other._name != '__GLOBAL__':
            other = other.asSet()
        # add the flag sets
        return self._addEquivalentFlagSets(other)
        
    def __neg__(self):
        """ -Flag -- negates all flags in a flag set.  Converts to a
            flag set if necessary """
        if self._name != '__GLOBAL__':
            new = self.asSet()
        else:
            new = self.deepCopy()
        flags = new.values()
        # negate every flag at this level, and add any child values
        # to the end of the list to convert
        while flags:
            flag = flags.pop()
            if flag._value is not None:
                flag._value = not flag._value
            flags.extend(flag.values())
        return new

    def __sub__(self, other):
        """ FlagA - FlagB: adds a negated version of FlagB to FlagA,
            converting to  a flag sets first if necessary """
        new = self + -other
        return new

    def getUsed(self):
        d = {}
        for flagname, flag in self.iteritems():
            if flag._used:
                d[flagname] = flag
            for usedFlag, subflag in flag.getUsed().iteritems():
                d['.'.join((flagname, usedFlag))] = subflag
        return d

    def getUsedSet(self):
        """ Create a flag set based on used flags """
        flagSet = nullSet()
        for flag in self.getUsed().itervalues():
            if flag._value is None:
                continue
            if flag._value is False:
                flag = -flag
            flagSet = flagSet + flag
        return flagSet

    def setUsed(self, usedDict):
        for flag, value in usedDict.iteritems():
            keys = flag.split('.')
            curflag = self
            for key in keys[:-1]:
                curflag = curflag[key]
            key = keys[-1]
            if isinstance(value, bool):
                curflag[key]._value = value
            else:
                curflag[key]._value = value._value
            curflag[key]._used = True

    def fullName(self):
        """ Return the name a user would use to access this flag """
        cursor = self
        namelist = [ ] 
        while cursor is not None:
            namelist.append(cursor._name)
            cursor = cursor._parent
        namelist.reverse()
        return '.'.join(namelist)

    def trackUsed(self, val):
        self._track = val
        for flag in self.iterkeys():
            self[flag].trackUsed(val)

    def resetUsed(self):
        self._used = False
        for flag in self.itervalues():
            flag.resetUsed()
    
    def deepCopy(self, parent=None):
        """ Create a copy of a flag set, creating new Flag instances
            for all children """
        new = Flag(value=self._value, name=self._name,
                   showdefaults=self._showdefaults, parent=parent,
                   createOnAccess=self._createOnAccess,
                   required=self._required, track=self._track)
        new._overrides = self._overrides.copy()
        new._used = self._used
        new._track = self._track
        for key in self:
            new[key] = self[key].deepCopy(self)
        # freeze new copy at end
        new._frozen = self._frozen
        return new

    def allAsSet(self):
        """ Convert self and child flags to a flag set """
        if self._name == '__GLOBAL__':
            return self
        top = parent = Flag(value=None, name=self._name, 
                                        required=self._required)
        cursor = self._parent
        while cursor is not None:
            child = parent
            parent = Flag(value=None, name=cursor._name)
            parent[child._name] = child
            child._parent = parent
            cursor = cursor._parent
        # Use, Arch, etc Flag instances don't have/need a parent
        # named __GLOBAL__ but sets containing both Use and Arch
        # need a higher level to connect them.
        if parent._name != '__GLOBAL__':
            child = parent
            globalFlag = Flag(value=None, name='__GLOBAL__')
            child._parent = globalFlag
            globalFlag[child._name] = child
        else:
            globalFlag = parent

        childflags = []
        for flag in self.keys():
            childflags.append((flag, top, self))
        while childflags:
            (flagname, parent, current) = childflags.pop()
            parent[flagname] = Flag(value=current[flagname]._get(), 
                             name=flagname,
                             parent=parent,
                             required=current[flagname]._required)
            for childflag in current[flagname].keys():
                childflags.append((childflag, parent[flagname], 
                                              current[flagname]))
            
        top._value = self._get()
        return globalFlag

    def asSet(self, *flags):
        """ Convert a flag to a flag set, containing only this flag.
            If any child flags are passed as arguments, a flag set is created
            containing this flag and the child flags """
        # a) create a Flag with knowledge about this flag and its
        # parents (parents all set to none)
        if self._name == '__GLOBAL__':
            return self
        top = parent = Flag(value=None, name=self._name, 
                                        required=self._required)
        cursor = self._parent
        while cursor is not None:
            child = parent
            parent = Flag(value=None, name=cursor._name)
            parent[child._name] = child
            child._parent = parent
            cursor = cursor._parent
        # Use, Arch, etc Flag instances don't have/need a parent
        # named __GLOBAL__ but sets containing both Use and Arch
        # need a higher level to connect them.
        if parent._name != '__GLOBAL__':
            child = parent
            parent = Flag(value=None, name='__GLOBAL__')
            child._parent = parent
            parent[child._name] = child

        # b) set the value of any child flags passed in 
        #    to True
        if flags:
            for flag in flags:
                top[flag] = Flag(value=True, name=flag, parent=top,
                                 required=self[flag]._required)
        else:
            top._value = True
        return parent

    def toDependency(self, recipename=''):
        """ Convert this flag set to a list of dependencies """
        # XXX this code should probably disappear with the reworking of 
        # flavors and their relationship with deps, but for now, 
        # it is very handy
        set = deps.DependencySet()
        if self._name != '__GLOBAL__':
            self = self.asSet()
        if 'Use' in self or 'Flags' in self:
            depFlags = []
            if 'Use' in self and self.Use.keys():
                for flag in self.Use.iterkeys():
                    depFlags.extend(self.Use[flag].toDepFlags(topflag=self.Use))
            if 'Flags' in self and self.Flags.keys():
                for flag in self.Flags.iterkeys():
                    depFlags.extend(
                        self.Flags[flag].toDepFlags(prefix=recipename,
                                                   topflag=self.Flags))
            if depFlags:
                dep = deps.Dependency('use', depFlags)
                set.addDep(deps.UseDependency, dep)
        if 'Arch' in self:
            excludeFlags = ['bits64', 'bits32', 'BE', 'LE' ] 
            for arch, topflag in self['Arch'].iteritems():
                if topflag._value is None:
                    # if we didn't check the top level arch, 
                    # we can get it's value from Arch.
                    if not Arch[arch]:
                        continue
                elif not topflag:
                    # if we checked the arch and it is not our arch
                    # don't add a dependency
                    continue
                if arch in excludeFlags:
                    continue
                depFlags = []
                for subarch, flag in topflag.iteritems():
                    depFlags.extend(flag.toDepFlags(topflag=topflag))
                dep = deps.Dependency(arch, depFlags)
                set.addDep(deps.InstructionSetDependency, dep)
        return set

    def toDepFlags(self, prefix=None, topflag=None):
        flags = []
        namelist = []
        if topflag is None:
            topflag = self
        cur = self
        while cur._name != topflag._name:
            namelist.insert(0, cur._name)
            cur = cur._parent
        name = ".".join(namelist)
        if prefix:
            name = '.'.join((prefix, name))
        if self._value is not None:
            if self._value:
                if self._required:
                    flags.append((name, deps.FLAG_SENSE_REQUIRED))
                else:
                    flags.append((name, deps.FLAG_SENSE_PREFERRED))
            else:
                flags.append((name, deps.FLAG_SENSE_PREFERNOT))
        for subflag in self.iterkeys():
            flags.extend(self[subflag].toDepFlags(prefix=prefix,
                                                      topflag=topflag))
        return flags

    def __setitem__(self, key, value):
	if self._frozen:
	    raise TypeError, 'flags are frozen'
        if key in self:
            if key not in self._overrides:
                self[key]._set(bool(value))
        else:
            dict.__setitem__(self, key, value)

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if name in self:
            flag = self[name]
            return flag
        if name in self._deprecated:
            return self._deprecated[name]
        elif self._createOnAccess and name[0] != '_':
            # flag doesn't exist, add it
            flag =  Flag(value=None, name=name, parent=self,
                              createOnAccess=True, track=self._track)
            dict.__setitem__(self, name, flag)
            # don't check frozen value -- it's okay to create empty flags, 
            # which is useful for creating flagsets (-Flags.kerenel.smp)
            # we will catch errors on setting an actual value to this flag
            return self[name]
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
            if name in self._overrides:
                return
            self[name]._set(value)
        else:
            self[name] = Flag(value=value, name=name, parent=self,
                              createOnAccess=self._createOnAccess,
                              track=self._track)
    def addDeprecated(self, name, newFlag=None):
        self._deprecated[name] = DeprecatedFlag(newFlag, name=name, parent=self)

class DeprecatedFlag(Flag):
    def __init__(self, newFlag, *args, **kw):
        self._newFlag = newFlag
        Flag.__init__(self, *args, **kw)

    def alert(self):
        if self._newFlag is not None:
            log.warning("Flag %s is deprecated, use %s instead" % 
                        (self.fullName(), self._newFlag.fullName()))
        else:
            raise RuntimeError, "Flag %s no longer exists" % self.fullName()

    def __nonzero__(self):
        self.alert()
        return self._newFlag.__nonzero__()

    def __repr__(self):
        self.alert()
        return self._newFlag.__repr__()


def nullSet():
    return Flag(value=None, name='__GLOBAL__')

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
Use = Flag(showdefaults=True, name='Use')

Use.pcre = True
Use.pcre.setShortDoc('Use the Perl-compatible regex library')
Use.pcre.setLongDoc("""
The perl-compatible regular expression library can be used by
several programs to increase the power and uniformity of
available regular expressions.  It adds a dependency and should
generally be disabled for embedded builds.
""")

Use.tcpwrappers = True
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
Use.bootstrap.setRequired(False)
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
Use.builddocs.setRequired(False)
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
Use.buildtests.setRequired(False)
Use.buildtests.setShortDoc('Build test suites')
Use.buildtests.setLongDoc("""
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
Use.qt = True
Use.kde = False
Use.xfce = False
Use.gd = False
Use.ldap = True
Use.sasl = False
Use.sasl.setShortDoc('Build with support for SASL Simple Authenication '
                     'and Security Layer')
Use.pie = False

Use.desktop = Use.gnome | Use.kde | Use.xfce
Use.desktop.setRequired(False)
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
Arch = Flag(showdefaults=False, name='Arch')
# Arch.x86 = Arch.i386 | Arch.i486 | Arch.i586 | Arch.i686
Arch.x86 = True
Arch.x86.setShortDoc('True if any IA32-compatible architecture is set')
Arch.x86.i386 = False
Arch.x86.i486 = False
Arch.x86.i586 = False
Arch.x86.i686 = True
Arch.x86.cmov = True
Arch.x86.sse = False
Arch.x86.sse2 = False
Arch.x86.mmx = False
Arch.x86.threednow = False # '3dnow' is an illegal identifier name

Arch.x86_64 = False
Arch.x86_64.setShortDoc('x86_64 base 64-bit extensions')
Arch.x86_64.amd64 = False
Arch.x86_64.amd64.setShortDoc('x86_64 with base AMD64 extensions')
Arch.x86_64.em64t = False
Arch.x86_64.em64t.setShortDoc('x86_64 with base EM64T extensions')
Arch.x86_64.sse3 = False

# we used to support Arch.x86.x86_64, has been changed to Arch.x86_64
Arch.x86.addDeprecated('x86_64', Arch.x86_64)
Arch.x86.x86_64.setShortDoc('Deprecated - Use Arch.x86_64 instead')


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

LocalFlags = Flag(showdefaults=False, name='Flags', createOnAccess=True)

def resetUsed():
    Use.resetUsed()
    Arch.resetUsed()
    LocalFlags.resetUsed()

def getUsedSet():
    return Arch.getUsedSet() + Use.getUsedSet() + LocalFlags.getUsedSet()
     
def getUsed():
    """
    A method for retreive the flags used by a recipe in dict form, separated
    by Flag set.  Can be used to store and restore a set of used flags 
    to allow for the separation of loading and setting up of a recipe and 
    cooking that recipe.
    """
    used = {}
    used['Arch'] = Arch.getUsed()
    used['Use'] = Use.getUsed()
    used['Flags'] = LocalFlags.getUsed()
    return used

def setUsed(usedDict):
    """
    A method for updating the used flags to include the flags passed in.
    Can be used to store and restore a set of used flags to allow for the 
    separation of loading and setting up of a recipe and cooking that recipe.
    """
    Arch.setUsed(usedDict['Arch'])
    Use.setUsed(usedDict['Use'])
    LocalFlags.setUsed(usedDict['Flags'])
    
def track(arg):
    """
    Turns Use flag tracking on or off.
    """
    Arch.trackUsed(arg)
    Use.trackUsed(arg)
    LocalFlags.trackUsed(arg)

def overrideFlags(config, pkgname):
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

def clearOverrides(config, pkgname):
    Use._clearOverrides()
    Arch._clearOverrides()
    for majarch in Arch.keys():
        Arch[majarch]._clearOverrides()
    LocalFlags._clearOverrides()
    for key in LocalFlags.keys():
        LocalFlags[key]._clearOverrides()

def allAsSet():
    return Use.allAsSet() + LocalFlags.allAsSet() + Arch.allAsSet()
