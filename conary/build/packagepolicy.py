#
# Copyright (c) 2004-2006 rPath, Inc.
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
Module used after C{%(destdir)s} has been finalized to create the
initial packaging.  Also contains error reporting.
"""
import itertools
import os
import re
import site
import stat
import sys

from conary import files
from conary.build import buildpackage, filter, policy
from conary.build import tags, use
from conary.deps import deps
from conary.lib import elf, util, log, pydeps
from conary.local import database



class _filterSpec(policy.Policy):
    """
    Pure virtual base class from which C{ComponentSpec} and C{PackageSpec}
    are derived.
    """
    bucket = policy.PACKAGE_CREATION
    def __init__(self, *args, **keywords):
	self.extraFilters = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	Call derived classes (C{ComponentSpec} or C{PackageSpec}) as::
	    ThisClass('<name>', 'filterexp1', 'filterexp2')
	where C{filterexp} is either a regular expression or a
	tuple of C{(regexp[, setmodes[, unsetmodes]])}
	"""
	if args:
	    theName = args[0]
	    for filterexp in args[1:]:
		self.extraFilters.append((theName, filterexp))
	policy.Policy.updateArgs(self, **keywords)


class _addInfo(policy.Policy):
    """
    Pure virtual class for policies that add information such as tags,
    requirements, and provision, to files.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    keywords = {
	'included': {},
	'excluded': {}
    }

    def updateArgs(self, *args, **keywords):
	"""
	Call as::
	    C{I{ClassName}(I{info}, I{filterexp})}
	or::
	    C{I{ClassName}(I{info}, exceptions=I{filterexp})}
	where C{I{filterexp}} is either a regular expression or a
	tuple of C{(regexp[, setmodes[, unsetmodes]])}
	"""
	if args:
	    args = list(args)
	    info = args.pop(0)
	    if args:
                if not self.included:
                    self.included = {}
		if info not in self.included:
		    self.included[info] = []
		self.included[info].extend(args)
	    elif 'exceptions' in keywords:
		# not the usual exception handling, this is an exception
                if not self.excluded:
                    self.excluded = {}
		if info not in self.excluded:
		    self.excluded[info] = []
		self.excluded[info].append(keywords.pop('exceptions'))
            else:
                raise TypeError, 'no paths provided'
	policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
        # for filters
	self.rootdir = self.rootdir % recipe.macros

	# instantiate filters
	d = {}
	for info in self.included:
            newinfo = info % recipe.macros
	    l = []
	    for item in self.included[info]:
		l.append(filter.Filter(item, recipe.macros))
	    d[newinfo] = l
	self.included = d

	d = {}
	for info in self.excluded:
            newinfo = info % recipe.macros
	    l = []
	    for item in self.excluded[info]:
		l.append(filter.Filter(item, recipe.macros))
	    d[newinfo] = l
	self.excluded = d

	policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
	fullpath = self.recipe.macros.destdir+path
	if not util.isregular(fullpath) and not os.path.islink(fullpath):
	    return
        self.runInfo(path)

    def runInfo(self, path):
        'pure virtual'
        pass


class Config(policy.Policy):
    """
    NAME
    ====

    B{C{r.Config()}} - Mark files as configuration files
    
    SYNOPSIS
    ========

    C{r.Config([I{filterexp}] || [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========

    Mark all files below C{%(sysconfdir)s} (C{/etc}) and C{%(taghandlerdir)s} 
    as configuration files. To mark files as exceptions, use:
    C{r.Config(exceptions=filterexp)} To mark explicit inclusions as 
    configuration files, use: C{r.Config(filterexp)}

    EXAMPLES
    ========

    C{r.Config(exceptions='/etc/X11/xkb/xkbcomp')}

    In the above example, the file C{/etc/X11/xkb/xkbcomp} is marked as an 
    exception, since it is not actually a configuration file.

    C{r.Config('%(mmdir)s/Mailman/mm_cfg.py')}

    The above example demonstrates inclusion of the configuration 
    file C{%(mmdir)s/Mailman/mm_cfg.py}.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        # for :config component
        ('ComponentSpec', policy.REQUIRED_SUBSEQUENT),
    )
    invariantinclusions = [ '%(sysconfdir)s/', '%(taghandlerdir)s/']

    def doFile(self, filename):
        m = self.recipe.magic[filename]
        if m and m.name == "ELF":
            # an ELF file cannot be a config file, some programs put
            # ELF files under /etc (X, for example), and tag handlers
            # can be ELF or shell scripts; we just want tag handlers
            # to be config files if they are shell scripts.
            # Just in case it was not intentional, warn...
            if self.macros.sysconfdir in filename:
                self.info('ELF file %s found in config directory', filename)
            return
        fullpath = self.macros.destdir + filename
        if os.path.isfile(fullpath) and util.isregular(fullpath):
            self._markConfig(filename, fullpath)

    def _markConfig(self, filename, fullpath):
        self.info(filename)
        f = file(fullpath)
        f.seek(0, 2)
        if f.tell():
            # file has contents
            f.seek(-1, 2)
            lastchar = f.read(1)
            f.close()
            if lastchar != '\n':
                self.error("config file %s missing trailing newline" %filename)
        f.close()
        self.recipe.ComponentSpec(_config=filename)


class ComponentSpec(_filterSpec):
    """
    NAME
    ====

    B{C{r.ComponentSpec()}} - Determines which component each file is in
    
    SYNOPSIS
    ========

    C{r.ComponentSpec([I{componentname}, I{filterexp}] || [I{packagename:component}, I{filterexp}])}

    DESCRIPTION
    ===========

    This class includes the filter expressions that specify the default
    assignment of files to components.  The expressions are considered
    in the order in which they are evaluated in the recipe, and the
    first match wins.  After all the recipe-provided expressions are
    evaluated, the default expressions are evaluated.  If no expression
    matches, then the file is assigned to the C{catchall} component.
    
    PARAMETERS
    ==========

    B{recipe} : holds the recipe object, which is used for the macro set and 
    package objects.

    KEYWORDS
    ========

    B{catchall} : Specify the  component name which gets all otherwise 
    unassigned files. Default: C{runtime}

    EXAMPLES
    ========

    C{r.ComponentSpec('manual', '%(contentdir)s/manual/')}

    The above example, uses C{r.ComponentSpec} to specify the 
    C{%(contentdir)s/manual/} directory is in the C{:manual} component.
    """
    requires = (
        ('Config', policy.REQUIRED_PRIOR),
        ('PackageSpec', policy.REQUIRED_SUBSEQUENT),
    )
    invariantFilters = (
        # These must never be overridden; keeping this separate allows for
        # r.ComponentSpec('runtime', '.*')
	('test',      ('%(testdir)s/')),
	('debuginfo', ('%(debugsrcdir)s/',
		       '%(debuglibdir)s/')),
    )
    # FIXME: baseFilters should be initialized like macros at some point
    baseFilters = (
        # development docs go in :devel
        ('devel',     ('%(mandir)s/man(2|3)/')),
	# note that gtk-doc is not well-named; it is a shared system, like info,
	# and is used by unassociated tools (devhelp).  This line needs to
        # come first because "lib" in these paths should not mean :lib
	('doc',       ('%(datadir)s/(gtk-doc|doc|man|info)/')),
	# automatic subpackage names and sets of regexps that define them
	# cannot be a dictionary because it is ordered; first match wins
	('runtime',   ('%(datadir)s/gnome/help/.*/C/')), # help menu stuff
        # python is potentially architecture-specific because of %(lib)
	('python',    ('/usr/(%(lib)s|lib)/python.*/site-packages/')),
        # perl is potentially architecture-specific because of %(lib)
	('perl',      ('/usr/(%(lib)s|lib)/perl.*/vendor_perl/')),
        # devellib is architecture-specific
        ('devellib',  (r'\.so',), stat.S_IFLNK),
	('devellib',  (r'\.a',
                       '%(libdir)s/pkgconfig/')),
        # devel is architecture-generic -- no %(lib)s/%(libdir)s
        ('devel',     (r'.*/include/.*\.h',
		       '%(includedir)s/',
		       '%(datadir)s/aclocal/',
		       '%(bindir)s/..*-config')),
	('locale',    ('%(datadir)s/locale/',
		       '%(datadir)s/gnome/help/.*/')),
	('emacs',     ('%(datadir)s/emacs/site-lisp/',)),
        # Anything else in /usr/share should be architecture-independent
        # data files (thus the "datadir" name)
        ('data',      ('%(datadir)s/',)),
        # Anything in {,/usr}/lib{,64} is architecture-specific
        ('lib',       (r'.*/(%(lib)s|lib)/')),
    )
    keywords = { 'catchall': 'runtime' }

    def __init__(self, *args, **keywords):
        """
        @keyword catchall: The component name which gets all otherwise
        unassigned files.  Default: C{runtime}
        """
        _filterSpec.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        if '_config' in keywords:
            config=keywords.pop('_config')
            self.recipe.PackageSpec(_config=config)
            # disable creating the automatic :config component
            # until/unless we handle files moving between
            # components
            #self.extraFilters.append(('config', util.literalRegex(config)))
	_filterSpec.updateArgs(self, *args, **keywords)

    def doProcess(self, recipe):
	compFilters = []
	self.macros = recipe.macros
	self.rootdir = self.rootdir % recipe.macros

        # The extras need to come before base in order to override decisions
        # in the base subfilters; invariants come first for those very few
        # specs that absolutely should not be overridden in recipes.
        for filteritem in itertools.chain(self.invariantFilters,
                                          self.extraFilters,
                                          self.baseFilters):
            main = ''
	    name = filteritem[0] % self.macros
            if ':' in name:
                main, name = name.split(':')
	    assert(name != 'source')
	    filterargs = self.filterExpression(filteritem[1:], name=name)
	    compFilters.append(filter.Filter(*filterargs))
            if main:
                # we've got a package as well as a component, pass it on
                recipe.PackageSpec(main, filteritem[1:])
	# by default, everything that hasn't matched a filter pattern yet
	# goes in the catchall component ('runtime' by default)
	compFilters.append(filter.Filter('.*', self.macros, name=self.catchall))

	# pass these down to PackageSpec for building the package
	recipe.PackageSpec(compFilters=compFilters)


class PackageSpec(_filterSpec):
    """
    NAME
    ====

    B{C{r.PackageSpec()}} - Determines package / component file is in
    
    SYNOPSIS
    ========

    C{r.PackageSpec([I{packagename},] [I{filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.PackageSpec()} is typically called from within a 
    Conary recipe to determine which package, and optionally, which component 
    each file is in.
    
    PARAMETERS
    ==========

    B{recipe} : Holds the recipe object, which is used for the macro set and 
    package objects.

    
    EXAMPLES
    ========
    
    C{r.PackageSpec('openssh-server', '%(sysconfdir)s/pam.d/sshd')}
    
    The example above specifies the file C{%(sysconfdir)s/pam.d/sshd} is in 
    the package C{openssh-server}.
    """
    requires = (
        ('ComponentSpec', policy.REQUIRED_PRIOR),
    )
    keywords = { 'compFilters': None }

    def __init__(self, *args, **keywords):
        """
        @keyword compFilters: reserved for C{ComponentSpec} to pass information
        needed by C{PackageSpec}.
        """
        _filterSpec.__init__(self, *args, **keywords)
        self.configFiles = []
        
    def updateArgs(self, *args, **keywords):
        if '_config' in keywords:
            self.configFiles.append(keywords.pop('_config'))
        # keep a list of packages filtered for in PackageSpec in the recipe
        if args:
            newTrove = args[0] % self.recipe.macros
            self.recipe.packages[newTrove] = True
        _filterSpec.updateArgs(self, *args, **keywords)

    def doProcess(self, recipe):
	pkgFilters = []
	self.macros = recipe.macros
	self.rootdir = self.rootdir % self.macros

	for (filteritem) in self.extraFilters:
	    name = filteritem[0] % self.macros
	    filterargs = self.filterExpression(filteritem[1:], name=name)
	    pkgFilters.append(filter.Filter(*filterargs))
	# by default, everything that hasn't matched a pattern in the
	# main package filter goes in the package named recipe.name
	pkgFilters.append(filter.Filter('.*', self.macros, name=recipe.name))

	# OK, all the filters exist, build an autopackage object that
	# knows about them
	recipe.autopkg = buildpackage.AutoBuildPackage(
	    pkgFilters, self.compFilters, recipe)

	# now walk the tree -- all policy classes after this require
	# that the initial tree is built
        recipe.autopkg.walk(self.macros['destdir'])

        # flag all config files
        for confname in self.configFiles:
            self.recipe.autopkg.pathMap[confname].flags.isConfig(True)




class InitialContents(policy.Policy):
    """
    NAME
    ====

    B{C{r.InitialContents()}} - Mark only explicit inclusions as initial
    contents files
    
    SYNOPSIS
    ========

    C{InitialContents([I{filterexp}])}

    DESCRIPTION
    ===========

    Specify only explicit inclusions to be marked as initial contents files, 
    which provide their contents only if the file does not yet exist.

    
    EXAMPLES
    ========
    
    C{r.InitialContents('%(sysconfdir)s/conary/.*gpg')}
    
    In the above example, the files C{%(sysconfdir)s/conary/.*gpg} are being 
    marked as initial contents files.
    """
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Config', policy.REQUIRED_PRIOR),
    )
    bucket = policy.PACKAGE_CREATION

    # change inclusions to default to none, instead of all files
    keywords = policy.Policy.keywords.copy()
    keywords['inclusions'] = []

    def updateArgs(self, *args, **keywords):
	policy.Policy.updateArgs(self, *args, **keywords)
        self.recipe.Config(exceptions=args)

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
        recipe = self.recipe
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.info(filename)
            f = recipe.autopkg.pathMap[filename]
            f.flags.isInitialContents(True)
            if f.flags.isConfig():
                self.error(
                    '%s is marked as both a configuration file and'
                    ' an initial contents file', filename)


class Transient(policy.Policy):
    """
    NAME
    ====

    B{C{r.Transient()}} - Mark files that have transient contents
    
    SYNOPSIS
    ========

    C{r.Transient([I{filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.Transient()} is typically called from within a 
    Conary recipe to mark files with transient contents.
        
    Files containing transient contents are almost the opposite of
    configuration files, in that they should be overwritten by a new
    version without question at update time.
    
    EXAMPLES
    ========
    
    C{r.Transient('%(libdir)s/firefox/extensions/')}
    
    The above usage example demonstrates marking files in the directory 
    C{%(libdir)s/firefox/extensions/} as having transient contents.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Config', policy.REQUIRED_PRIOR),
        ('InitialContents', policy.REQUIRED_PRIOR),
    )

    invariantinclusions = [
	r'..*\.py(c|o)$',
        r'..*\.elc$',
    ]

    def doFile(self, filename):
	fullpath = self.macros.destdir + filename
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            recipe = self.recipe
            f = recipe.autopkg.pathMap[filename]
	    f.flags.isTransient(True)
            if f.flags.isConfig() or f.flags.isInitialContents():
                self.error(
                    '%s is marked as both a transient file and'
                    ' a configuration or initial contents file', filename)


class TagDescription(policy.Policy):
    """
    NAME
    ====

    B{C{r.TagDescription()}} - Marks tag description files
    
    SYNOPSIS
    ========

    C{r.TagDescription([I{groupname}, I{filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.TagDescription} is typically called from within a 
    Conary recipe to mark tag description files as such so that conary 
    handles them correctly.  Every file in C{%(tagdescriptiondir)s/}
    is marked as a tag description file by default.  
    
    No file outside of C{%(tagdescriptiondir)s/} will be considered by this 
    policy.
    
    EXAMPLES
    ========
    
    This policy class should not require explicit use.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    invariantsubtrees = [ '%(tagdescriptiondir)s/' ]

    def doFile(self, file):
	fullpath = self.macros.destdir + file
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.info('conary tag file: %s', file)
	    self.recipe.autopkg.pathMap[file].tags.set("tagdescription")


class TagHandler(policy.Policy):
    """
    NAME
    ====

    B{C{r.TagHandler()}} - Mark tag handler files
    
    SYNOPSIS
    ========

    C{r.TagHandler([I{filterexp}])}

    DESCRIPTION
    ===========

    By default, all  files in C{%(taghandlerdir)s/} are marked as a tag 
    handler files. The policy class C{r.TagHandler()} is typically called from
    within a Conary recipe to mark tag handler files as such so that conary 
    handles them
    correctly.
    
    Note: No files outside of the C{%(taghandler)s/} directory will be 
    considered by this policy, and thus it should never be required to invoke
    this policy explicitly.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    invariantsubtrees = [ '%(taghandlerdir)s/' ]

    def doFile(self, file):
	fullpath = self.macros.destdir + file
	if os.path.isfile(fullpath) and util.isregular(fullpath):
            self.info('conary tag handler: %s', file)
	    self.recipe.autopkg.pathMap[file].tags.set("taghandler")


class TagSpec(_addInfo):
    """
    NAME
    ====

    B{C{r.TagSpec()}} - Apply tags defined by tag descriptions
    
    SYNOPSIS
    ========

    C{r.TagSpec([I{tagname}, I{filterexp}] || [I{tagname}, I{exceptions=filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.TagSpec()} is typically called from within a Conary 
    recipe to apply tags defined by tag descriptions in both the current 
    system and C{%(destdir)s} to all files in C{%(destdir)}.
    
    To apply tags manually, use the syntax: C{r.TagSpec(I{tagname}, I{filterexp})}, 
    or to set an exception to this policy, use: 
    C{r.TagSpec(I{tagname}, I{exceptions=filterexp})}.

    
    EXAMPLES
    ========
    
    C{r.TagSpec('initscript', '%(initdir)s/')}
    
    The example invocation of C{r.TagSpec} above applies the C{initscript} tag
    to the directory C{%(initdir)s/}.
    """
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    def doProcess(self, recipe):
	self.tagList = []
	# read the system and %(destdir)s tag databases
	for directory in (recipe.macros.destdir+'/etc/conary/tags/',
			  '/etc/conary/tags/'):
	    if os.path.isdir(directory):
		for filename in os.listdir(directory):
		    path = util.joinPaths(directory, filename)
		    self.tagList.append(tags.TagFile(path, recipe.macros, True))
        self.db = database.Database(self.recipe.cfg.root, self.recipe.cfg.dbPath)
        _addInfo.doProcess(self, recipe)

    def markTag(self, name, tag, path, tagFile=None):
        # commonly, a tagdescription will nominate a file to be
        # tagged, but it will also be set explicitly in the recipe,
        # and therefore markTag will be called twice.
        if (len(tag.split()) > 1 or
            not tag.replace('-', '').replace('_', '').isalnum()):
            # handlers for multiple tags require strict tag names:
            # no whitespace, only alphanumeric plus - and _ characters
            self.error('illegal tag name %s for file %s' %(tag, path))
            return
        tags = self.recipe.autopkg.pathMap[path].tags
        if tag not in tags:
            self.info('%s: %s', name, path)
            tags.set(tag)
            if tagFile:
                for trove in self.db.iterTrovesByPath(tagFile.tagFile):
                    troveName = trove.getName()
                    if troveName not in self.recipe.buildRequires:
                        # XXX should be error, change after bootstrap
                        self.warn("%s assigned by %s to file %s, so add '%s'"
                                   ' to buildRequires or call r.TagSpec()'
                                   %(tag, tagFile.tagFile, path, troveName))

    def runInfo(self, path):
        for tag in self.included:
	    for filt in self.included[tag]:
		if filt.match(path):
                    isExcluded = False
                    if tag in self.excluded:
		        for filt in self.excluded[tag]:
                            if filt.match(path):
                                self.info('ignoring tag match for %s: %s',
                                         tag, path)
                                isExcluded = True
                                break
                    if not isExcluded:
		        self.markTag(tag, tag, path)
                
	for tag in self.tagList:
	    if tag.match(path):
		if tag.name:
		    name = tag.name
		else:
		    name = tag.tag
                isExcluded = False
		if tag.tag in self.excluded:
		    for filt in self.excluded[tag.tag]:
			# exception handling is per-tag, so handled specially
			if filt.match(path):
                            self.info('ignoring tag match for %s: %s',
                                      name, path)
                            isExcluded = True
			    break
                if not isExcluded:
		    self.markTag(name, tag.tag, path, tag)


class MakeDevices(policy.Policy):
    """
    NAME
    ====

    B{C{r.MakeDevices()}} - Make device nodes
    
    SYNOPSIS
    ========

    C{MakeDevices([I{path},] [I{type},] [I{major},] [I{minor},] [I{owner},] [I{groups},] [I{mode}])}

    DESCRIPTION
    ===========

    The policy class C{r.MakeDevices()} is typically called from within a 
    Conary recipe to create device nodes.  Conary's policy of non-root builds
    requires that these nodes exist only in the package, and not in the 
    filesystem, as only root may actually create device nodes.

    
    EXAMPLES
    ========
    
    C{r.MakeDevices(I{'/dev/tty', 'c', 5, 0, 'root', 'root', mode=0666})}
    
    The example above creates the device node C{/dev/tty}, as type 'c' 
    (character, as opposed to type 'b', or block) with a major number of '5', 
    minor number of '0', owner, and group are both the root user, and 
    permissions are 0666.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Ownership', policy.REQUIRED_SUBSEQUENT),
    )

    def __init__(self, *args, **keywords):
	self.devices = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	MakeDevices(path, devtype, major, minor, owner, group, mode=0400)
	"""
	if args:
            args = list(args)
            if 'mode' in keywords:
                args.append(keywords.pop('mode'))
	    l = len(args)
	    # mode is optional, all other arguments must be there
	    assert((l > 5) and (l < 8))
	    if l == 6:
		args.append(0400)
	    self.devices.append(args)
	policy.Policy.updateArgs(self, **keywords)

    def do(self):
        for device in self.devices:
            r = self.recipe
            r.autopkg.addDevice(*device)
            filename = device[0]
            owner = device[4]
            group = device[5]
            r.Ownership(owner, group, filename)


class setModes(policy.Policy):
    """
    Do not call from recipes; this is used internally by C{r.SetModes}
    and C{r.ParseManifest}
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('WarnWriteable', policy.REQUIRED_SUBSEQUENT),
    )
    def __init__(self, *args, **keywords):
	self.fixmodes = {}
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	setModes(mode, path(s)...)
	"""
	if args:
	    for path in args[1:]:
		self.fixmodes[path] = args[0]
	policy.Policy.updateArgs(self, **keywords)

    def doFile(self, path):
	if path in self.fixmodes:
	    mode = self.fixmodes[path]
	    # set explicitly, do not warn
	    self.recipe.WarnWriteable(
                exceptions=util.literalRegex(path.replace('%', '%%')))
            self.info('suid/sgid: %s mode 0%o', path, mode & 07777)
	    self.recipe.autopkg.pathMap[path].inode.perms.set(mode)


class LinkType(policy.Policy):
    """
    NAME
    ====

    B{C{r.LinkType()}} - Ensures only regular, non-configuration files have hardlinks
    
    SYNOPSIS
    ========

    C{LinkLinkType([I{filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.LinkType()} is typically called from within a 
    Conary recipe to to ensure only regular, non-configuration files are 
    hardlinked.

    
    EXAMPLES
    ========
    
    FIXME : is an example needed?  could not find an example in recipes.
    
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('Config', policy.REQUIRED_PRIOR),
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    def do(self):
        for component in self.recipe.autopkg.getComponents():
            for path in component.hardlinks:
                if self.recipe.autopkg.pathMap[path].flags.isConfig():
                    self.error("Config file %s has illegal hard links", path)
            for path in component.badhardlinks:
                self.error("Special file %s has illegal hard links", path)


class LinkCount(policy.Policy):
    """
    NAME
    ====

    B{C{r.LinkCount()}} - Define exceptions to hardlinking rules
    
    SYNOPSIS
    ========

    C{LinkCount([I{filterexp}] | [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.LinkCount()} is typically called from within a Conary
    recipe to allow for exceptions to the hardlinks across directories policy.
    
    It is generally an error to have hardlinks across directories, except when
    the packager knows that there is no reasonable chance that they will be on
    separate filesystems

    In cases where the packager is certain hardlinks will not cross 
    filesystems,  a list of regular expressions specifying directory names 
    which are exceptions to the hardlink policy may be passed to 
    C{r.LinkCount}.


    EXAMPLES
    ========
    
    C{r.LinkCount(exceptions='/usr/share/zoneinfo/.*')}
    
    The above example uses C{r.LinkCount} to except zoneinfo files, 
    located in C{/usr/share/zoneinfo/} to the hardlinks policy.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    def __init__(self, *args, **keywords):
        policy.Policy.__init__(self, *args, **keywords)
        self.excepts = set()

    def updateArgs(self, *args, **keywords):
        if 'exceptions' in keywords:
            exceptions = keywords.pop('exceptions')
            if type(exceptions) is str:
                self.excepts.add(exceptions)
            elif type(exceptions) in (tuple, list):
                self.excepts.update(set(exceptions))
        # FIXME: we may want to have another keyword argument
        # that passes information down to the buildpackage
        # that causes link groups to be broken for some
        # directories but not others.  We need to research
        # first whether this is useful; it may not be.

    def do(self):
        filters = [filter.Filter(x, self.macros) for x in self.excepts]
        for component in self.recipe.autopkg.getComponents():
            for inode in component.linkGroups:
                # ensure all in same directory, except for directories
                # matching regexps that have been passed in
                dirSet = set(os.path.dirname(x) + '/'
                             for x in component.linkGroups[inode]
                             if not [y for y in filters if y.match(x)])
                if len(dirSet) > 1:
                    self.error('files %s are hard links across directories %s',
                               ', '.join(sorted(component.linkGroups[inode])),
                               ', '.join(sorted(list(dirSet))))
                    self.error('If these directories cannot reasonably be'
                               ' on different filesystems, disable this'
                               ' warning by calling'
                               " r.LinkCount(exceptions=('%s')) or"
                               " equivalent"
                               % "', '".join(sorted(list(dirSet))))


class ExcludeDirectories(policy.Policy):
    """
    NAME
    ====

    B{C{r.ExcludeDirectories()}} - Exclude directories from package
    
    SYNOPSIS
    ========

    C{r.ExcludeDirectories([I{filterexp}] | [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========

    Causes directories to be excluded from the package by default. Use 
    C{r.ExcludeDirectories(exceptions=filterexp)} to set exceptions to 
    this policy, and directories matching the regular expression 
    C{filterexp} will be included in the package.
    
    There are only two reasons to explicitly package a directory: the 
    directory needs permissions other than 0755, or it must exist even 
    if it is empty.

    Therefore, it should generally not be necessary to invoke this policy 
    directly.  If your directory requires permissions other than 0755, simply
    use C{r.SetMode} to specify the permissions, and the directory will be 
    automatically included.
    
    Packages do not need to explicitly include directories to ensure
    existence of a target to place a file in. Conary will appropriately
    create the directory, and delete it later if the directory becomes empty.
    

    EXAMPLES
    ========
    
    C{r.ExcludeDirectories(exceptions='/tftpboot')}
    
    The above example sets the file C{/tftboot} as an exception to the 
    C{r.ExcludeDirectories} policy, and ensures C{/tftpboot} will be included
    in the package.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]

    def doFile(self, path):
	fullpath = self.recipe.macros.destdir + os.sep + path
	s = os.lstat(fullpath)
	mode = s[stat.ST_MODE]
	if mode & 0777 != 0755:
            self.info('excluding directory %s with mode %o', path, mode&0777)
	elif not os.listdir(fullpath):
            self.info('excluding empty directory %s', path)
	self.recipe.autopkg.delFile(path)


class ByDefault(policy.Policy):
    """
    NAME
    ====

    B{C{r.ByDefault()}} - Determines components to be installed by default
  
    SYNOPSIS
    ========

    C{r.ByDefault([I{exceptions},] [I{use},] [I{inclusions},] [I{subtrees}])}

    DESCRIPTION
    ===========

    The policy class C{r.ByDefault()} is called from within a Conary recipe
    to determine which components should be installed by default at the time 
    of package installation. By default, :debug, and :test packages are not 
    installed.
    
    PARAMETERS
    ==========

    The following parameters are recognized by C{r.ByDefault}:

    B{recipe} : Holds the recipe object, which is used for the macro set,
    and package objects.

    KEYWORDS
    ========

    The following keywords are recognized by C{r.ByDefault}:

    B{exceptions} : An optional argument comprised of regular expressions,
    which specifies files to ignore while enforcing the policy action. The
    content of C{exceptions} will be interpolated against recipe macros prior
    to being used.

    B{inclusions} : C{FileFilter} strings, C{FileFilter} tuples, or a 
    non-tuple list of C{FileFilter} strings, or C{FileFilter}s tuples used to
    limit the policy, or if it already is limited (invariantinclusion) then
    C{inclusions} provide additional FileFilters to include within the general
    limitation.
    
    B{subtrees} : Specifies a subtree to which to limit the policy, or it it
    already is limited (invariantsubtrees), then C{subtrees} provides 
    additional subtrees to consider.
    
    B{use} : An optional argument which specifies Use flag(s) instructing 
    whether to perform the action.
    
    EXAMPLES
    ========
    
    C{r.ByDefault(exceptions=[':manual'])}
    
    The above example uses C{r.ByDefault} to ignore C{:manual} components when 
    enforcing the policy.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )
    filetree = policy.NO_FILES

    invariantexceptions = [':test', ':debuginfo']

    def doProcess(self, recipe):
        if not self.inclusions:
            self.inclusions = []
        if not self.exceptions:
            self.exceptions = []
        recipe.setByDefaultOn(frozenset(self.inclusions))
        recipe.setByDefaultOff(frozenset(self.exceptions +
                                         self.invariantexceptions))


class _UserGroup(policy.Policy):
    """
    Abstract base class that implements marking owner/group dependencies.
    """
    bucket = policy.PACKAGE_CREATION
    # All classes that descend from _UserGroup must run before the
    # Requires policy, as they implicitly depend on it to set the
    # file requirements and union the requirements up to the package.
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Requires', policy.REQUIRED_SUBSEQUENT),
    )
    filetree = policy.PACKAGE

    def setUserGroupDep(self, path, info, depClass):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        if path not in pkg.requiresMap:
            # BuildPackage only fills in requiresMap for ELF files; we may
            # need to create a few more DependencySets.
            pkg.requiresMap[path] = deps.DependencySet()
        pkg.requiresMap[path].addDep(depClass, deps.Dependency(info, []))


class Ownership(_UserGroup):
    """
    NAME
    ====

    B{C{r.Ownership()}} - Set file ownership
    
    SYNOPSIS
    ========

    C{r.Ownership([I{username},] [I{groupname},] [I{filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.Ownership()} is typically called from within a Conary
    recipe to set user and group ownership of files when the default of
    C{root:root} is not appropriate.
    
    List the ownerships in order, most specific first, ending with least
    specific. The filespecs will be matched in the order that you provide them.
    
    PARAMETERS
    ==========

    None.

    KEYWORDS
    ========

    None.
    
    EXAMPLES
    ========
    
    C{r.Ownership('apache', 'apache', '%(localstatedir)s/lib/php/session')}
    
    The example above sets ownership of C{%(localstatedir)s/lib/php/session}
    to owner C{apache}, and group C{apache}.
    """

    def __init__(self, *args, **keywords):
	self.filespecs = []
        self.systemusers = ('root',)
        self.systemgroups = ('root',)
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	if args:
	    for filespec in args[2:]:
		self.filespecs.append((filespec, args[0], args[1]))
	policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
	# we must NEVER take ownership from the filesystem
	assert(not self.exceptions)
	self.rootdir = self.rootdir % recipe.macros
	self.fileFilters = []
	for (filespec, user, group) in self.filespecs:
	    self.fileFilters.append(
		(filter.Filter(filespec, recipe.macros),
                 user %recipe.macros,
                 group %recipe.macros))
	del self.filespecs
	policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
	for (f, owner, group) in self.fileFilters:
	    if f.match(path):
		self._markOwnership(path, owner, group)
		return
	self._markOwnership(path, 'root', 'root')

    def _markOwnership(self, filename, owner, group):
	pkgfile = self.recipe.autopkg.pathMap[filename]
	if owner:
	    pkgfile.inode.owner.set(owner)
            if owner not in self.systemusers:
                self.setUserGroupDep(filename, owner, deps.UserInfoDependencies)
	if group:
	    pkgfile.inode.group.set(group)
            if group not in self.systemgroups:
                self.setUserGroupDep(filename, group, deps.GroupInfoDependencies)


class _Utilize(_UserGroup):
    """
    Pure virtual base class for C{UtilizeUser} and C{UtilizeGroup}
    """
    def __init__(self, *args, **keywords):
	self.filespecs = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	call as::
	  UtilizeFoo(item, filespec(s)...)
	List them in order, most specific first, ending with most
	general; the filespecs will be matched in the order that
	you provide them.
	"""
	if args:
	    for filespec in args[1:]:
		self.filespecs.append((filespec, args[0]))
	policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
	self.rootdir = self.rootdir % recipe.macros
	self.fileFilters = []
	for (filespec, item) in self.filespecs:
	    self.fileFilters.append(
		(filter.Filter(filespec, recipe.macros), item))
	del self.filespecs
	policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
	for (f, item) in self.fileFilters:
	    if f.match(path):
		self._markItem(path, item)
        return

    def _markItem(self, path, item):
        # pure virtual
        assert(False)


class UtilizeUser(_Utilize):
    """
    NAME
    ====

    B{C{r.UtilizeUser()}} - Marks files as requiring a user definition to exist
    
    SYNOPSIS
    ========

    C{r.UtilizeUser([I{username}, I{filterexp}])}

    DESCRIPTION
    ===========

    The policy class C{r.UtilizeUser} is typically called from within a
    Conary recipe to mark files as requiring a user definition to exist even
    though the file is not owned by that user
    
    This is particularly useful for daemons that are setuid root
    but change their user id to a user id with no filesystem permissions
    after they start.
    
    EXAMPLES
    ========
    
    C{r.UtilizeUser('sshd', '%(sbindir)s/sshd')}

    The example above marks the file C{%(sbindir)s/sshd} as requiring the
    user definition 'sshd' although the file is not owned by the 'sshd' user.
    """
    def _markItem(self, path, user):
        self.info('user %s: %s' % (user, path))
        self.setUserGroupDep(path, user, deps.UserInfoDependencies)


class UtilizeGroup(_Utilize):
    """
    NAME
    ====

    B{C{r.UtilizeGroup()** - Marks files as requiring a user definition to
    exist}}
    
    SYNOPSIS
    ========

    C{r.UtilizeGroup([groupname, filterexp])}

    DESCRIPTION
    ===========

    The policy class C{r.UtilizeGroup} is typically called from within a
    Conary recipe to mark files as requiring a group definition to exist
    even though the file is not owned by that group.
    
    This is particularly useful for daemons that are setuid root
    but change their user id to a group id with no filesystem permissions
    after they start.
    
    EXAMPLES
    ========
    
    C{r.UtilizeGroup('users', '%(sysconfdir)s/default/useradd')}

    The example above marks the file C{%(sysconfdir)s/default/useradd} as
    requiring the group definition 'users' although the file is not owned
    by the 'users' group.
    """
    def _markItem(self, path, group):
        self.info('group %s: %s' % (group, path))
        self.setUserGroupDep(path, group, deps.GroupInfoDependencies)


class ComponentRequires(policy.Policy):
    """
    NAME
    ====

    B{C{r.ComponentRequires()}} - Create automatic, intra-package, 
    inter-component dependencies
    
    SYNOPSIS
    ========

    C{r.ComponentRequires([I{componentname: requiringComponentSet}] |
    [I{packagename: componentname: requiringComponentSet}])}

    DESCRIPTION
    ===========

    The policy class C{r.ComponentRequires()} is called from within a Conary
    recipe to create automatic, intra-package, inter-component dependencies,
    such as a corresponding dependency between C{:lib} and C{:data} components.
    
    Changes are passed in using dictionaries for both general, and top-level,
    package-specific changes.  For general changes, use this syntax:
    C{r.ComponentRequires(B{componentname: requiringComponentSet})}.
    For top-level, package-specific changes, the syntax is as such:
    C{r.ComponentRequires(B{packagename: componentname: requiringComponentSet})}.
    
    In  concept, a top-level example would be the default requirement of
    C{:data} by C{:lib} and C{:runtime}.
    Using C{r.ComponentRequires({'data': set(('lib',))})} would specify to all
    top-level packages only C{:lib} requires C{:data}, however.
   
    For the general use, such requirements might be affected on only one
    package, such as package C{foo}, for example, by using a syntax such
    as: C{r.ComponentRequires({'foo': 'data': set(('lib',))})}.
    
    Note that C{r.ComponentRequires} cannot require capability flags; use 
    C{r.Requires} if you need to specify requirements, including capability
    flags.
    

    EXAMPLES
    ========
    
    C{r.ComponentRequires({'openssl': {'config': set(('runtime', 'lib'))}})}
    
    The above example uses C{r.ComponentRequires} to create dependencies in a
    top-level manner for the C{:runtime} and C{:lib} component sets to require 
    the C{:config} component for the C{openssl} package.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    def __init__(self, *args, **keywords):
        self.depMap = {
            # component: components that require it if they both exist
            'data': frozenset(('lib', 'runtime', 'devellib')),
            'devellib': frozenset(('devel',)),
            'lib': frozenset(('devel', 'devellib', 'runtime')),
            'config': frozenset(('runtime', 'lib', 'devellib', 'devel')),
        }
        self.overridesMap = {}
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        d = args[0]
        if isinstance(d[d.keys()[0]], dict): # dict of dicts
            for packageName in d:
                if packageName not in self.overridesMap:
                    # start with defaults, then override them individually
                    o = {}
                    o.update(self.depMap)
                    self.overridesMap[packageName] = o
                self.overridesMap[packageName].update(d[packageName])
        else: # dict of sets
            self.depMap.update(d)

    def do(self):
        components = self.recipe.autopkg.components
        for packageName in [x.name for x in self.recipe.autopkg.packageMap]:
            if packageName in self.overridesMap:
                d = self.overridesMap[packageName]
            else:
                d = self.depMap
            for requiredComponent in d:
                for requiringComponent in d[requiredComponent]:
                    reqName = ':'.join((packageName, requiredComponent))
                    wantName = ':'.join((packageName, requiringComponent))
                    if (reqName in components and wantName in components and
                        components[reqName] and components[wantName]):
                        # Note: this does not add dependencies to files;
                        # these dependencies are insufficiently specific
                        # to attach to files.
                        ds = deps.DependencySet()
                        depClass = deps.TroveDependencies
                        ds.addDep(depClass, deps.Dependency(reqName))
                        p = components[wantName]
                        p.requires.union(ds)


class ComponentProvides(policy.Policy):
    """
    NAME
    ====

    B{C{r.ComponentProvides()}} - Causes each trove to explicitly provide
    itself.
    
    SYNOPSIS
    ========

    C{r.ComponentProvides([I{flags}] | [I{pkgname}, I{flags}])}

    DESCRIPTION
    ===========

    The policy class C{r.ComponentProvides()} is called from within a Conary
    recipe to cause each trove to provide itself explicitly, with optional
    capability flags consisting of a single string, or a list, tuple, or set
    of strings. It is impossible to provide a capability flag for one 
    component but not another within a single package.

    EXAMPLES
    ========
    
    C{r.ComponentProvides("addcolumn")}
    
    The above example uses C{r.ComponentProvides} in the context of the 
    sqlite recipe, and causes sqlite to provide itself explicitly with the
    capability flag C{addcolumn}.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
    )

    # frozenset to make sure we do not modify class data
    flags = frozenset()

    def updateArgs(self, *args, **keywords):
        if len(args) == 2:
            #pkgname = args[0]
            flags = args[1]
        else:
            flags = args[0]
        if not isinstance(flags, (list, tuple, set)):
            flags=(flags,)
        self.flags = frozenset(flags) | self.flags

    def do(self):
        if self.flags:
            flags = [ (x % self.macros, deps.FLAG_SENSE_REQUIRED)
                      for x in self.flags ]
        else:
            flags = []
        for component in self.recipe.autopkg.components.values():
            component.provides.addDep(deps.TroveDependencies,
                deps.Dependency(component.name, flags))



def _getmonodis(macros, recipe, path):
    # For bootstrapping purposes, prefer the just-built version if
    # it exists
    if os.access('%(destdir)s/%(monodis)s' %macros, os.X_OK):
        return ('MONO_PATH=%(destdir)s%(prefix)s/lib'
                ' LD_LIBRARY_PATH=%(destdir)s%(libdir)s'
                ' %(destdir)s/%(monodis)s' %macros)
    elif os.access('%(monodis)s' %macros, os.X_OK):
        return '%(monodis)s' %macros
    else:
        recipe.warn('%s not available for dependency discovery'
                    ' for path %s' %(macros.monodis, path))
    return None


def _getperlincpath(perl):
    """
    Fetch the perl @INC path, and sort longest first for removing
    prefixes from perl files that are provided.
    """
    if not perl:
        return []
    p = util.popen(r"""%s -e 'print join("\n", @INC)'""" %perl)
    perlIncPath = p.readlines()
    # make sure that the command completed successfully
    rc = p.close()
    perlIncPath = [x.strip() for x in perlIncPath if not x.startswith('.')]
    return perlIncPath

def _getperl(macros, recipe):
    """
    Find the preferred instance of perl to use, including setting
    any environment variables necessary to use that perl.
    Returns string for running it, and a separate string, if necessary,
    for adding to @INC.
    """
    perlDestPath = '%(destdir)s%(bindir)s/perl' %macros
    # not %(bindir)s so that package modifications do not affect
    # the search for system perl
    perlPath = '/usr/bin/perl' %macros

    def _perlDestInc(destdir, perlDestInc):
        return ' '.join(['-I' + destdir + x for x in perlDestInc])

    if os.access(perlDestPath, os.X_OK):
        # must use packaged perl if it exists
        m = recipe.magic[perlPath]
        if m and 'RPATH' in m.contents and m.contents['RPATH']:
            # we need to prepend the destdir to each element of the RPATH
            # in order to run perl in the destdir
            perl = ''.join((
                'export LD_LIBRARY_PATH=',
                ':'.join([macros.destdir+x
                          for x in m.contents['RPATH'].split(':')]),
                ';',
                perlDestPath
            ))
            perlDestInc = _getperlincpath(perl)
            perlDestInc = _perlDestInc(macros.destdir, perlDestInc)
            return [perl, perlDestInc]
        else:
            # perl that does not need rpath?
            perlDestInc = _getperlincpath(perlDestPath)
            perlDestInc = _perlDestInc(macros.destdir, perlDestInc)
            return [perlDestPath, perlDestInc]
    elif os.access(perlPath, os.X_OK):
        # system perl if no packaged perl, needs no @INC mangling
        return [perlPath, '']

    # must be no perl at all
    return ['', '']


class Provides(policy.Policy):
    """
    NAME
    ====

    B{C{r.Provides()}} - Drives provides mechanism
    
    SYNOPSIS
    ========

    C{r.Provides([I{provision}, I{filterexp}] || [I{exceptions=filterexp}])

    DESCRIPTION
    ===========

    The policy class C{r.Provides()} is called from within a Conary recipe to
    mark files as providing certain features, or characteristics, or to avoid
    marking a file as providing things, such as for package-private plugin
    modules installed in system library directories. 

    A C{provision} may be a file, soname or an ABI; Provisions that begin with
    'file' are files, those that start with 'soname:' are sonames, and those
    that start with 'abi:' are ABIs. Other prefixes are reserved. 
    
    Note: Use {Cr.ComponentProvides}, and not C{r.Provides} to add capability
    flags to components.
    
    EXAMPLES
    ========
    
    C{r.Provides('file', '/usr/sbin/sendmail')}
    
    The above example demonstrates using C{r.Provides} to specify the file
    provision C{/usr/sbin/sendmail}.
    """
    bucket = policy.PACKAGE_CREATION

    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('SharedLibrary', policy.REQUIRED),
        # _ELFPathProvide calls Requires to pass in discovered info
        ('Requires', policy.REQUIRED_SUBSEQUENT),
    )
    filetree = policy.PACKAGE

    invariantexceptions = (
	'%(docdir)s/',
    )

    def __init__(self, *args, **keywords):
	self.provisions = []
        self.sonameSubtrees = set()
        self.sysPath = None
        self.monodisPath = None
        self.perlIncPath = None
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	if args:
	    for filespec in args[1:]:
		self.provisions.append((filespec, args[0]))
        sonameSubtrees = keywords.pop('sonameSubtrees', None)
        if sonameSubtrees:
            if type(sonameSubtrees) in (list, tuple):
                self.sonameSubtrees.update(set(sonameSubtrees))
            else:
                self.sonameSubtrees.add(sonameSubtrees)
        policy.Policy.updateArgs(self, **keywords)

    def _generatePythonProvidesSysPath(self):
        """ Generate a correct sys.path based on both the installed 
            system (in case a buildreq affects the sys.path) and the
            destdir (for newly added sys.path directories).  Use site.py
            to generate a list of such dirs.  Note that this list of dirs
            should NOT have destdir in front.
        """
        oldSysPath = sys.path
        oldSysPrefix = sys.prefix
        oldSysExecPrefix = sys.exec_prefix
        destdir = self.macros.destdir

        try:
            # 1. determine python dir based on python version and sys.prefix,
            # just like site.py does
            pythonDir = os.path.dirname(sys.modules['os'].__file__)
            systemPaths = set([pythonDir])

            # 2. determine root system site-packages, and add them to the
            # list of acceptable provide paths
            sys.path = []
            site.addsitepackages(None)
            systemPaths.update(sys.path)

            # 3. determine created destdir site-packages, and add them to
            # the list of acceptable provide paths
            sys.path = []
            sys.prefix = destdir + sys.prefix
            sys.exec_prefix = destdir + sys.exec_prefix
            site.addsitepackages(None)

            destDirLen = len(destdir)
            systemPaths.update(x[destDirLen:] for x in sys.path
                                                    if x.startswith(destdir))

            # later, we will need to truncate paths using longest path first
            self.sysPath = sorted(systemPaths, key=len, reverse=True)
        finally:
            sys.path = oldSysPath
            sys.prefix = oldSysPrefix
            sys.exec_prefix = oldSysExecPrefix


    def preProcess(self):
	self.rootdir = self.rootdir % self.macros
	self.fileFilters = []
	for filespec, provision in self.provisions:
	    self.fileFilters.append(
		(filter.Filter(filespec, self.macros), provision % self.macros))
	del self.provisions
        self.legalCharsRE = re.compile('[.0-9A-Za-z_+-/]')

        # interpolate macros, using canonical path form with no trailing /
        self.sonameSubtrees = set(os.path.normpath(x % self.macros)
                                  for x in self.sonameSubtrees)

    def _fetchPerlIncPath(self):
        """
        Cache the perl @INC path, sorted longest first
        """
        if self.perlIncPath is not None:
            return

        perl = _getperl(self.recipe.macros, self.recipe)[0]
        self.perlIncPath = _getperlincpath(perl)
        self.perlIncPath.sort(key=len, reverse=True)

    def _ELFPathProvide(self, path, m, pkg):
        basedir = os.path.dirname(path)
        if basedir not in self.sonameSubtrees and path in pkg.providesMap:
            # path needs to be in the dependency, since the
            # provides is too broad otherwise, so add it.
            # We can only add characters from the path that are legal
            # in a dependency name
            basedir = ''.join(x for x in basedir if self.legalCharsRE.match(x))
            depSet = deps.DependencySet()
            for depClass, dep in pkg.providesMap[path].iterDeps():
                if depClass is deps.SonameDependencies:
                    oldname = dep.getName()[0]
                    elfclass, soname = oldname.split('/', 1)
                    if '/' not in soname:
                        # add it
                        name = '%s%s/%s' % (elfclass, basedir, soname)
                        newdep = deps.Dependency(name, dep.flags)
                        # tell Requires about this change
                        self.recipe.Requires(_privateDepMap=(oldname, newdep))
                        dep = newdep
                depSet.addDep(depClass, dep)
            pkg.providesMap[path] = depSet

    def _addPythonProvides(self, path, m, pkg, macros):

        if not (path.endswith('.py') or path.endswith('.so')):
            return

        if self.sysPath is None:
            self._generatePythonProvidesSysPath()

        depPath = None
        for sysPathEntry in self.sysPath:
            if path.startswith(sysPathEntry):
                newDepPath = path[len(sysPathEntry)+1:]
                if newDepPath not in ('__init__.py', '__init__'):
                    # we don't allow bare __init__ as a python import
                    # hopefully we'll find this init as a deeper import at some
                    # other point in the sysPath
                    depPath = newDepPath
                    break

        if not depPath:
            return

        depPath = depPath[:-3]
        if depPath.endswith('/__init__'):
            depPath = depPath.replace('/__init__', '')
        depPath = depPath.replace('/', '.')
        if depPath == '__future__':
            return
        
        dep = deps.Dependency(depPath)
        if path not in pkg.providesMap:
            pkg.providesMap[path] = deps.DependencySet()
        pkg.providesMap[path].addDep(deps.PythonDependencies, dep)


    def _addCILProvides(self, path, m, pkg, macros):
        if not m or m.name != 'CIL':
            return
        fullpath = macros.destdir + path
        if not self.monodisPath:
            self.monodisPath = _getmonodis(macros, self, path)
            if not self.monodisPath:
                return
        p = util.popen('%s --assembly %s' %(
                       self.monodisPath, fullpath))
        name = None
        ver = None
        for line in [ x.strip() for x in p.readlines() ]:
            if 'Name:' in line:
                name = line.split()[1]
            elif 'Version:' in line:
                ver = line.split()[1]
        p.close()
        # monodis did not give us any info
        if not name or not ver:
            return
        if path not in pkg.providesMap:
            pkg.providesMap[path] = deps.DependencySet()
        pkg.providesMap[path].addDep(deps.CILDependencies,
                deps.Dependency(name, [(ver, deps.FLAG_SENSE_REQUIRED)]))

    def _addJavaProvides(self, path, m, pkg):
        if not m.contents['provides']:
            return
        if path not in pkg.providesMap:
            pkg.providesMap[path] = deps.DependencySet()
        for prov in m.contents['provides']:
            pkg.providesMap[path].addDep(deps.JavaDependencies,
                deps.Dependency(prov, []))

    def _addPerlProvides(self, path, m, pkg):
        # do not call perl to get @INC unless we have something to do for perl
        self._fetchPerlIncPath()

        # It is possible that we'll want to allow user-specified
        # additions to the perl search path, but if so, we need
        # to path-encode those files, so we can't just prepend
        # those elements to perlIncPath.  We would need to end up
        # with something like "perl: /path/to/foo::bar" because
        # for perl scripts that don't modify @INC, they could not
        # find those scripts.  It is not clear that we need this
        # at all, because most if not all of those cases would be
        # intra-package dependencies that we do not want to export.

        depPath = None
        for pathPrefix in self.perlIncPath:
            if path.startswith(pathPrefix):
                depPath = path[len(pathPrefix)+1:]
                break
        if depPath is None:
            return

        if path not in pkg.providesMap:
            pkg.providesMap[path] = deps.DependencySet()
        # foo/bar/baz.pm -> foo::bar::baz
        prov = '::'.join(depPath.split('/')).rsplit('.', 1)[0]
        pkg.providesMap[path].addDep(deps.PerlDependencies,
            deps.Dependency(prov, []))

    def doFile(self, path):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        macros = self.recipe.macros
        m = None

        fullpath = macros.destdir + path

        if os.path.exists(fullpath):
            m = self.recipe.magic[path]
            mode = os.lstat(fullpath)[stat.ST_MODE]

        if os.path.exists(fullpath):
            if (m and m.name == 'ELF'
                and m.contents['provides']
                and m.contents['Type'] == elf.ET_EXEC):
                # unless specified manually, do not export dependencies
                # for ELF executables
                del pkg.providesMap[path]

        # Now add in the manual provisions, which may include sonames
        # that might need to have paths added
        for (filter, provision) in self.fileFilters:
            if filter.match(path):
                m = self._markProvides(path, fullpath, provision, pkg, m, f)

        if os.path.exists(fullpath):
            if m and m.name == 'ELF':
                self._ELFPathProvide(path, m, pkg)

            if path.endswith('.so') or path.endswith('.py'):
                self._addPythonProvides(path, m, pkg, macros)

            elif m and m.name == 'CIL':
                self._addCILProvides(path, m, pkg, macros)

            elif (m and (m.name == 'java' or m.name == 'jar')
                and m.contents['provides']):
                self._addJavaProvides(path, m, pkg)

            elif path.endswith('.pm') or path.endswith('.pl'):
                # Keep the extension list in sync with Requires.
                self._addPerlProvides(path, m, pkg)

        if path not in pkg.providesMap:
            return
        f.provides.set(pkg.providesMap[path])
        pkg.provides.union(f.provides())

        # Because paths can change, individual files do not provide their
        # paths.  However, within a trove, a file does provide its name.
        # Furthermore, non-regular files can be path dependency targets 
        # Therefore, we have to handle this case a bit differently.
        if f.flags.isPathDependencyTarget():
            pkg.provides.addDep(deps.FileDependencies, deps.Dependency(path))

    def _markProvides(self, path, fullpath, provision, pkg, m, f):
        if path not in pkg.providesMap:
            # BuildPackage only fills in providesMap for ELF files; we may
            # need to create a few more DependencySets.
            pkg.providesMap[path] = deps.DependencySet()

        if provision.startswith("file"):
            # can't actually specify what to provide, just that it provides...
            f.flags.isPathDependencyTarget(True)
            return m

        if provision.startswith("abi:"):
            abistring = provision[4:].strip()
            op = abistring.index('(')
            abi = abistring[:op]
            flags = abistring[op+1:-1].split()
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
            pkg.providesMap[path].addDep(deps.AbiDependency,
                deps.Dependency(abi, flags))
            return m

        if provision.startswith("soname:"):
            if os.path.islink(fullpath):
                # allow symlinks to provide sonames if necessary and if
                # they point to real files; paths encoded in sonames might
                # require the symlink path
                contents = os.readlink(fullpath)
                if contents.startswith('/'):
                    m = self.recipe.magic[os.path.normpath(contents)]
                else:
                    m = self.recipe.magic[os.path.normpath(
                                          os.path.dirname(path)+'/'+contents)]
            if m and m.name == 'ELF':
                # Only ELF files can provide sonames.
                # This is for libraries that don't really include a soname,
                # but programs linked against them require a soname
                main = provision[7:].strip()
                soflags = []
                if '(' in main:
                    # get list of arbitrary flags
                    main, rest = main.split('(')
                    soflags.extend(rest[:-1].split())
                main = ''.join(x for x in main if self.legalCharsRE.match(x))
                abi = m.contents['abi']
                soflags.extend(abi[1])
                flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in soflags ]
                # normpath removes extra / characters
                dep = deps.Dependency(
                    os.path.normpath('/'.join((abi[0], main))), flags)
                pkg.providesMap[path].addDep(deps.SonameDependencies, dep)
                if '/' in main:
                    # basename removes path, so that we can map
                    # requirements in Requires
                    plainname = '/'.join((abi[0], os.path.basename(main)))
                    self.recipe.Requires(_privateDepMap=(plainname, dep))
            return m


class Requires(_addInfo):
    """
    NAME
    ====

    B{C{r.Requires()}} - Drives requirements mechanism
    
    SYNOPSIS
    ========

    C{r.Requires([I{/path/to/file}, I{filterexp}] || [I{packagename:component[(FLAGS)]},] || [I{exceptions=filterexp)}])}

    DESCRIPTION
    ===========

    The policy class C{r.Requires()} is called from within a Conary recipe to
    avoid adding requirements for a file, such as example shell scripts
    outside of C{%(docdir)s}.
    
    Note: Components are the only troves which can be required.
    
    For executables executed only through wrappers that use C{LD_LIBRARY_PATH}
    to find the libraries instead of embedding an RPATH in the binary, you
    will need to provide a synthetic RPATH using C{r.Requires(rpath=RPATH)}
    or C{r.Requires(rpath=(filterExp, RPATH))} calls, which are tested in the
    order provided. 
    
    The RPATH is a standard Unix-style path string containing one or more
    directory names, separated only by colon characters, except for one
    significant change: Each path component is interpreted using shell-style
    globs, which are checked first in the C{%(destdir)s} and then on the
    installed system. (The globs are useful for cases like perl where
    statically determining the entire content of the path is difficult. Use
    globs only for variable parts of paths; be as specific as you can without
    using the glob feature any more than necessary.)
    
    Executables that use C{dlopen()} to open a shared library will not
    automatically have a dependency on that shared library. If the program
    unconditionally requires that it be able to C{dlopen()} the shared
    library, encode that requirement by manually creating the requirement
    by calling C{r.Requires('soname: libfoo.so', 'filterexp')} or
    C{r.Requires('soname: /path/to/libfoo.so', 'filterexp')} depending on
    whether the library is in a system library directory or not. (It should be
    the same as how the soname dependency is expressed by the providing
    package.)
    
    For unusual cases where a system library is not listed in C{ld.so.conf}
    but is instead found through a search through special subdirectories with
    architecture-specific names (such as C{i686} and C{tls}), you can pass in
    a string or list of strings specifying the directory or list of
    directories. with C{r.Requires(sonameSubtrees='/directoryname')}
    or C{r.Requires(sonameSubtrees=['/list', '/of', '/dirs'])} 
    
    Note: These are B{not} regular expressions. They will have macro
    expansion expansion performed on them.
    
    For unusual cases where Conary finds a false or misleading dependency,
    or in which you need to override a true dependency, you can specify
    C{r.Requires(exceptDeps='regexp')} to override all dependencies matching
    a regular expression, C{r.Requires(exceptDeps=('filterexp', 'regexp'))}
    to override dependencies matching a regular expression only for files
    matching filterexp, or
    C{r.Requires(exceptDeps=(('filterexp', 'regexp'), ...))} to specify
    multiple overrides.
    

    EXAMPLES
    ========
    
    C{r.Requires('mailbase:runtime', '%(sbindir)s/sendmail')}
    
    The above example demonstrates using C{r.Requires} to specify a manual
    requirement of the file C{%(sbindir)s/sendmail} to the  C{:runtime}
    component of package C{mailbase}.
    """

    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('SharedLibrary', policy.REQUIRED),
        # Requires depends on _ELFPathProvide having been run
        ('Provides', policy.REQUIRED_PRIOR),
    )
    filetree = policy.PACKAGE

    invariantexceptions = (
	'%(docdir)s/',
    )

    def __init__(self, *args, **keywords):
        self.sonameSubtrees = set()
        self._privateDepMap = {}
        self.rpathFixup = []
        self.exceptDeps = []
        self.sysPath = None
        self.monodisPath = None
        self.perlReqs = None
        self.perlPath = None
        self.perlIncPath = None
        policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
        # _privateDepMap is used only for Provides to talk to Requires
        privateDepMap = keywords.pop('_privateDepMap', None)
        if privateDepMap:
            self._privateDepMap.update([privateDepMap])
        sonameSubtrees = keywords.pop('sonameSubtrees', None)
        if sonameSubtrees:
            if type(sonameSubtrees) in (list, tuple):
                self.sonameSubtrees.update(set(sonameSubtrees))
            else:
                self.sonameSubtrees.add(sonameSubtrees)
        rpath = keywords.pop('rpath', None)
        if rpath:
            if type(rpath) is str:
                rpath = ('.*', rpath)
            assert(type(rpath) == tuple)
            self.rpathFixup.append(rpath)
        exceptDeps = keywords.pop('exceptDeps', None)
        if exceptDeps:
            if type(exceptDeps) is str:
                exceptDeps = ('.*', exceptDeps)
            assert(type(exceptDeps) == tuple)
            if type(exceptDeps[0]) is tuple:
                self.exceptDeps.extend(exceptDeps)
            else:
                self.exceptDeps.append(exceptDeps)
        _addInfo.updateArgs(self, *args, **keywords)

    def preProcess(self):
        macros = self.macros
        self.systemLibPaths = set(os.path.normpath(x % macros)
                                  for x in self.sonameSubtrees)
        # anything that any buildreqs have caused to go into ld.so.conf
        # is a system library by definition
        self.systemLibPaths |= set(os.path.normpath(x[:-1])
                                   for x in file('/etc/ld.so.conf').readlines())
        self.rpathFixup = [(filter.Filter(x, macros), y % macros)
                           for x, y in self.rpathFixup]
        self.exceptDeps = [(filter.Filter(x, macros), re.compile(y % macros))
                          for x, y in self.exceptDeps]

    def _ELFPathFixup(self, path, m, pkg):
        """
        Change requirements to have path in soname if appropriate
        """
        # common case is no likely modification, so make that fastest
        found = False
        rpathList = []
        for depClass, dep in pkg.requiresMap[path].iterDeps():
            if dep.getName()[0] in self._privateDepMap:
                found = True

        def appendUnique(ul, items):
            for item in items:
                if item not in ul:
                    ul.append(item)

        def _canonicalRPATH(rpath, glob=False):
            # normalize all elements of RPATH
            l = [ os.path.normpath(x) for x in rpath.split(':') ]
            # prune system paths and relative paths from RPATH
            l = [ x for x in l
                  if x not in self.systemLibPaths and x.startswith('/') ]
            if glob:
                destdir = self.macros.destdir
                dlen = len(destdir)
                gl = []
                for item in l:
                    # prefer destdir elements
                    paths = util.braceGlob(destdir + item)
                    paths = [ os.path.normpath(x[dlen:]) for x in paths ]
                    appendUnique(gl, paths)
                    # then look on system
                    paths = util.braceGlob(item)
                    paths = [ os.path.normpath(x) for x in paths ]
                    appendUnique(gl, paths)
                l = gl
            return l

        # fixup should come first so that its path elements can override
        # the included RPATH if necessary
        if self.rpathFixup:
            for f, rpath in self.rpathFixup:
                if f.match(path):
                    # synthetic RPATH items are globbed
                    rpathList = _canonicalRPATH(rpath, glob=True)
                    break

        if m and 'RPATH' in m.contents and m.contents['RPATH']:
            rpathList += _canonicalRPATH(m.contents['RPATH'])

        if not found and not rpathList:
            return

        def _findSonameInRpath(soname):
            for rpath in rpathList:
                destpath = '/'.join((self.macros.destdir, rpath, soname))
                if os.path.exists(destpath):
                    return rpath
                destpath = '/'.join((rpath, soname))
                if os.path.exists(destpath):
                    return rpath
            # didn't find anything
            return None

        # found at least one potential replacement, do the real work
        depSet = deps.DependencySet()
        for depClass, dep in pkg.requiresMap[path].iterDeps():
            oldName = dep.getName()[0]
            if oldName in self._privateDepMap:
                # we assume that if it is both provided and required,
                # we fix up the requirement to match the provision
                provided = self._privateDepMap[oldName]
                if set(dep.flags.keys()).issubset(set(provided.flags.keys())):
                    # we need the new name, but we need to keep the old flags
                    dep = deps.Dependency(provided.getName()[0], dep.flags)
            elif depClass.tag == deps.DEP_CLASS_SONAME:
                elfClass, soname = oldName.split('/', 1)
                rpath = _findSonameInRpath(soname)
                if rpath:
                    if rpath.startswith('/'):
                        # we need it without the leading /
                        rpath = rpath[1:]
                    # change the name to follow the rpath
                    newName = '/'.join((elfClass, rpath, soname))
                    dep = deps.Dependency(newName, dep.flags)
            depSet.addDep(depClass, dep)
        pkg.requiresMap[path] = depSet

    def _generatePythonRequiresSysPath(self):
        # Generate the correct sys.path for finding the required modules.
        # we use the built in site.py to generate a sys.path for the
        # current system and another one where destdir is the root. 
        # note the below code is similar to code in Provides, 
        # but it creates an ordered path list with and without destdir prefix,
        # while provides only needs a complete list without destdir prefix.

        oldSysPath = sys.path
        oldSysPrefix = sys.prefix
        oldSysExecPrefix = sys.exec_prefix

        try:
            destdir = self.macros.destdir

            # 1. determine python dir based on python version and sys.prefix,
            # just like site.py does
            pythonDir = os.path.dirname(sys.modules['os'].__file__)
            systemPaths = [pythonDir]


            # 2. generate site-packages list for /
            sys.path = []
            site.addsitepackages(None)

            systemPaths += sys.path

            # 2. generate site-packages list for destdir
            # (look in python base directory first)
            sys.path = [destdir + pythonDir]

            sys.prefix = destdir + sys.prefix
            sys.exec_prefix = destdir + sys.exec_prefix
            site.addsitepackages(None)

            destDirPaths = sys.path

            # when searching for modules, we search destdir first,
            # then system.
            self.sysPath = destDirPaths + systemPaths

            # make an unsorted copy for module finder
            sysPathForModuleFinder = list(self.sysPath)

            # later, we will need to truncate paths using longest path first
            self.sysPath.sort(key=len, reverse=True)
        finally:
            sys.path = oldSysPath
            sys.prefix = oldSysPrefix
            sys.exec_prefix = oldSysExecPrefix

        # load module finder after sys.path is restored
        # in case delayed importer is installed.
        self.pythonModuleFinder = pydeps.DirBasedModuleFinder(
                                            destdir, sysPathForModuleFinder)


    def _addPythonRequirements(self, path, fullpath, pkg, script=False):
        # FIXME: we really should check for python in destdir and shell
        # out to use that python to discover the dependencies if it exists.
        destdir = self.recipe.macros.destdir

        if not self.sysPath:
            self._generatePythonRequiresSysPath()

        try:
            if script:
                self.pythonModuleFinder.run_script(fullpath)
            else:
                self.pythonModuleFinder.load_file(fullpath)
        except:
            # not a valid python file
            self.info('File %s is not a valid python file', path)
            return

        for depPath in self.pythonModuleFinder.getDepsForPath(fullpath):
            for sysPathEntry in self.sysPath:
                if depPath.startswith(sysPathEntry):
                    newDepPath = depPath[len(sysPathEntry)+1:]
                    if newDepPath not in ('__init__', '__init__.py'):
                        # we don't allow bare __init__'s as dependencies.
                        # hopefully we'll find this at deeper level in 
                        # in the sysPath
                        depPath = newDepPath
                        break

            if depPath.startswith('/'):
                # a python file not found in sys.path will not have been
                # provided, so we must not depend on it either
                return
            if depPath.endswith('.py') or depPath.endswith('.so'):
                depPath = depPath[:-3]
            else:
                # Not something we provide, so not something we can
                # require either.  Drop it and go on.  We have seen
                # this when a script in /usr/bin has ended up in the
                # requires list.
                continue

            depPath = depPath.replace('/', '.')
            depPath = depPath.replace('.__init__', '')

            if depPath == '__future__':
                continue

            self._addRequirement(path, depPath, [], pkg,
                                 deps.PythonDependencies)

    def _fetchPerl(self):
        """
        Cache the perl path and @INC path with %(destdir)s prepended to
        each element if necessary
        """
        if self.perlPath is not None:
            return

        macros = self.recipe.macros
        self.perlPath, self.perlIncPath = _getperl(macros, self.recipe)

    def _getPerlReqs(self, path, fullpath):
        if self.perlReqs is None:
            self._fetchPerl()
            if not self.perlPath:
                # no perl == bootstrap, but print warning
                self.info('Unable to find perl interpreter,'
                           ' disabling perl: requirements')
                self.perlReqs = False
                return []
            # get the base directory where conary lives.  In a checked
            # out version, this would be .../conary/conary/build/package.py
            # chop off the last 3 directories to find where
            # .../conary/Scandeps and .../conary/scripts/perlreqs.pl live
            basedir = '/'.join(sys.modules[__name__].__file__.split('/')[:-3])
            scandeps = '/'.join((basedir, 'conary/ScanDeps'))
            if os.path.exists(scandeps):
                perlreqs = '%s/scripts/perlreqs.pl' % basedir
            else:
                # we assume that conary is installed in
                # $prefix/$libdir/python?.?/site-packages.  Use this
                # assumption to find the prefix for
                # /usr/lib/conary and /usr/libexec/conary
                regexp = re.compile(r'(.*)/lib(64){0,1}/python[1-9].[0-9]/site-packages')
                match = regexp.match(basedir)
                if not match:
                    # our regexp didn't work.  fall back to hardcoded
                    # paths
                    prefix = '/usr'
                else:
                    prefix = match.group(1)
                # ScanDeps is not architecture specific
                scandeps = '%s/lib/conary/ScanDeps' %prefix
                if not os.path.exists(scandeps):
                    # but it might have been moved to lib64 for multilib
                    scandeps = '%s/lib64/conary/ScanDeps' %prefix
                perlreqs = '%s/libexec/conary/perlreqs.pl' %prefix
            self.perlReqs = '%s -I%s %s %s' %(
                self.perlPath, scandeps, self.perlIncPath, perlreqs)
        if self.perlReqs is False:
            return []

        p = os.popen('%s %s' %(self.perlReqs, fullpath))
        reqlist = [x.strip().split('//') for x in p.readlines()]
        # make sure that the command completed successfully
        rc = p.close()
        if rc:
            # make sure that perl didn't blow up
            assert(os.WIFEXITED(rc))
            # Apparantly ScanDeps could not handle this input
            return []

        # we care only about modules right now
        # throwing away the filenames for now, but we might choose
        # to change that later
        reqlist = [x[2] for x in reqlist if x[0] == 'module']
        # foo/bar/baz.pm -> foo::bar::baz
        reqlist = ['::'.join(x.split('/')).rsplit('.', 1)[0] for x in reqlist]

        return reqlist

    def doFile(self, path):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        macros = self.recipe.macros
        fullpath = macros.destdir + path
        m = None
        if not isinstance(f, files.DeviceFile):
            # device is not in filesystem to look at with magic
            m = self.recipe.magic[path]

        # we may have some dependencies that need converting
        if (m and m.name == 'ELF'
            and path in pkg.requiresMap):
            self._ELFPathFixup(path, m, pkg)

        # now go through explicit requirements
	for info in self.included:
	    for filt in self.included[info]:
		if filt.match(path):
                    self._markManualRequirement(info, path, pkg, m)

        # now check for automatic dependencies besides ELF
        if f.inode.perms() & 0111 and m and m.name == 'script':
            interp = m.contents['interpreter']
            if len(interp.strip()) and self._checkInclusion(interp, path):
                # no interpreter string warning is in BadInterpreterPaths
                if not (os.path.exists(interp) or
                        os.path.exists(macros.destdir+interp)):
                    # this interpreter not on system, warn
                    # cannot be an error to prevent buildReq loops
                    self.warn('interpreter "%s" (referenced in %s) missing',
                        interp, path)
                    # N.B. no special handling for /{,usr/}bin/env here;
                    # if there has been an exception to
                    # NormalizeInterpreterPaths, then it is a
                    # real dependency on the env binary
                self._addRequirement(path, interp, [], pkg,
                                     deps.FileDependencies)

        if (f.inode.perms() & 0111 and m and m.name == 'script' and
            os.path.basename(m.contents['interpreter']).startswith('python')):
            self._addPythonRequirements(path, fullpath, pkg, script=True)
        elif path.endswith('.py'):
            self._addPythonRequirements(path, fullpath, pkg, script=False)

        if m and m.name == 'CIL':
            if not self.monodisPath:
                self.monodisPath = _getmonodis(macros, self, path)
                if not self.monodisPath:
                    return
            p = util.popen('%s --assemblyref %s' %(
                           self.monodisPath, fullpath))
            for line in [ x.strip() for x in p.readlines() ]:
                if ': Version=' in line:
                    ver = line.split('=')[1]
                elif 'Name=' in line:
                    name = line.split('=')[1]
                    self._addRequirement(path, name, [ver], pkg,
                                         deps.CILDependencies)
            p.close()

        if (m and (m.name == 'java' or m.name == 'jar')
            and m.contents['requires']):
            for req in m.contents['requires']:
                self._addRequirement(path, req, [], pkg,
                                     deps.JavaDependencies)

        if (path.endswith('.pl') or path.endswith('.pm') or
            (f.inode.perms() & 0111 and m and m.name == 'script'
             and '/bin/perl' in m.contents['interpreter'])):
            perlReqs = self._getPerlReqs(path, fullpath)
            for req in perlReqs:
                self._addRequirement(path, req, [], pkg,
                                     deps.PerlDependencies)

        # remove intentionally discarded dependencies
        if self.exceptDeps and path in pkg.requiresMap:
            depSet = deps.DependencySet()
            for depClass, dep in pkg.requiresMap[path].iterDeps():
                for filt, exceptRe in self.exceptDeps:
                    if filt.match(path):
                        matchName = '%s: %s' %(depClass.tagName, str(dep))
                        if exceptRe.match(matchName):
                            # found one to not copy
                            dep = None
                            break
                if dep is not None:
                    depSet.addDep(depClass, dep)
            pkg.requiresMap[path] = depSet

        # finally, package the dependencies up
        if path not in pkg.requiresMap:
            return
        f.requires.set(pkg.requiresMap[path])
        pkg.requires.union(f.requires())
    
    def _markManualRequirement(self, info, path, pkg, m):
        flags = []
        if self._checkInclusion(info, path):
            if info[0] == '/':
                depClass = deps.FileDependencies
            elif info.startswith('file:') and info[5:].strip()[0] == '/':
                info = info[5:].strip()
                depClass = deps.FileDependencies
            elif info.startswith('soname:'):
                if not m or m.name != 'ELF':
                    # only an ELF file can have a soname requirement
                    return
                # we need to synthesize a dependency that encodes the
                # same ABI as this binary
                depClass = deps.SonameDependencies
                for depType, dep, f in m.contents['requires']:
                    if depType == 'abi':
                        flags = f
                        info = '%s/%s' %(dep, info.split(None, 1)[1])
            else: # by process of elimination, must be a trove
                if info.startswith('group-'):
                    self.error('group dependency %s not allowed', info)
                    return
                if info.startswith('fileset-'):
                    self.error('fileset dependency %s not allowed', info)
                    return
                if ':' not in info:
                    self.error('package dependency %s not allowed', info)
                    return
                depClass = deps.TroveDependencies
            self._addRequirement(path, info, flags, pkg, depClass)

    def _checkInclusion(self, info, path):
        if info in self.excluded:
            for filt in self.excluded[info]:
                # exception handling is per-requirement,
                # so handled specially
                if filt.match(path):
                    self.info('ignoring requirement match for %s: %s',
                              path, info)
                    return False
        return True

    def _addRequirement(self, path, info, flags, pkg, depClass):
        if depClass == deps.FileDependencies:
            pathMap = self.recipe.autopkg.pathMap
            componentMap = self.recipe.autopkg.componentMap
            if info in pathMap and info not in componentMap[info].providesMap:
                # if a package requires a file, includes that file,
                # and does not provide that file, it should error out
                self.error('%s requires %s, which is included but not'
                           ' provided; use'
                           " r.Provides('file', '%s')", path, info, info)
                return
        if path not in pkg.requiresMap:
            # BuildPackage only fills in requiresMap for ELF files; we may
            # need to create a few more DependencySets.
            pkg.requiresMap[path] = deps.DependencySet()
        # in some cases, we get literal "(flags)" from the recipe
        if '(' in info:
            flagindex = info.index('(')
            flags = set(info[flagindex+1:-1].split() + list(flags))
            info = info.split('(')[0]
        if flags:
            flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags ]
        pkg.requiresMap[path].addDep(depClass, deps.Dependency(info, flags))


class Flavor(policy.Policy):
    """
    NAME
    ====

    B{C{r.Flavor()}} - Controls the Flavor mechanism
    
    SYNOPSIS
    ========

    C{r.Flavor([I{filterexp}] | [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========

    Mark a file's Flavor with the flavor mechanism.  To except a file's flavor
    from being marked, use:  C{r.Flavor(exceptions=filterexp)}.

    EXAMPLES
    ========
    
    C{r.Flavor(exceptions='%(crossprefix)s/lib/gcc-lib/.*')}
    
    In the above example, the files in the directory 
    C{%(crossprefix)s/lib/gcc-lib} are being excepted from having their Flavor
    marked.
    """
    bucket = policy.PACKAGE_CREATION
    requires = (
        ('PackageSpec', policy.REQUIRED_PRIOR),
        ('Requires', policy.REQUIRED_PRIOR),
        ('ExcludeDirectories', policy.REQUIRED_PRIOR),
    )
    filetree = policy.PACKAGE

    def doProcess(self, recipe):
	self.libRe = re.compile(
            '^(%(libdir)s'
            '|/%(lib)s'
            '|%(x11prefix)s/%(lib)s'
            '|%(krbprefix)s/%(lib)s)(/|$)' %recipe.macros)
	self.libReException = re.compile('^/usr/(lib|%(lib)s)/python.*$')

        self.baseIsnset = use.Arch.getCurrentArch()._name
        self.troveMarked = False
	policy.Policy.doProcess(self, recipe)

    def hasLib(self, path):
        return self.libRe.match(path) and not self.libReException.match(path)

    def doFile(self, path):
	componentMap = self.recipe.autopkg.componentMap
	if path not in componentMap:
	    return
	pkg = componentMap[path]
	f = pkg.getFile(path)
        if path in pkg.isnsetMap:
            isnset = pkg.isnsetMap[path]
        elif self.hasLib(path):
            # all possible paths in a %(lib)s-derived path get default
            # instruction set assigned if they don't have one already
            if f.hasContents:
                isnset = self.baseIsnset
            else:
                # this file can't be marked by arch, but the troves
                # and package must be
                if self.troveMarked:
                    return
                set = use.Arch.getCurrentArch()._toDependency()
                for pkg in componentMap.values():
                    pkg.flavor.union(set)
                self.troveMarked = True
                return
        else:
            return

	set = deps.DependencySet()
        set.addDep(deps.InstructionSetDependency, deps.Dependency(isnset, []))
        # get the Arch.* dependencies
        set.union(use.createFlavor(None, use.Arch._iterUsed()))
        f.flavor.set(set)
        # all troves need to share the same flavor so that we can
        # distinguish them later
        for pkg in componentMap.values():
            pkg.flavor.union(f.flavor())



class reportErrors(policy.Policy):
    """
    This class is used to report together all package errors.
    Do not call it directly; it is for internal use only.
    """
    bucket = policy.ERROR_REPORTING
    filetree = policy.NO_FILES

    def __init__(self, *args, **keywords):
	self.warnings = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	Called once, with printf-style arguments, for each warning.
	"""
	self.warnings.append(args[0] %tuple(args[1:]))

    def do(self):
	if self.warnings:
	    for warning in self.warnings:
		log.error(warning)
	    raise policy.PolicyError, 'Package Policy errors found:\n%s' %"\n".join(self.warnings)
