#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import os
import policy
import log
import stat
import buildpackage
import filter

"""
Module used by recipes to effect packaging policy; things like setting
hints, flags, and dependencies.
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

# bail out as soon as possible if there's an unrecoverable error
# so put error classes first

class NonBinariesInBindirs(policy.Policy):
    """
    Directories that are specifically for binaries should have only
    files that have some executable bit set.
    """
    invariantexceptions = [ ('.*', stat.S_IFDIR) ]
    invariantsubtrees = [
	'%(bindir)s/',
	'%(essentialbindir)s/',
	'%(krbprefix)s/bin/',
	'%(x11prefix)s/bin/',
	'%(sbindir)s/',
	'%(essentialsbindir)s/',
	'%(initdir)s/',
	'%(libexecdir)s/',
	'%(sysconfdir)s/profile.d/',
    ]

    def doFile(self, file):
	d = self.macros['destdir']
	mode = os.lstat(util.joinPaths(d, file))[stat.ST_MODE]
	if not mode & 0111:
	    raise PackagePolicyError(
		"%s has mode 0%o with no executable permission in bindir"
		%(file, mode))
	m = self.recipe.magic[file]
	if m and m.name == 'ltwrapper':
	    raise PackagePolicyError(
		"%s is a build-only libtool wrapper script" %file)


class FilesInMandir(policy.Policy):
    """
    The C{%(mandir)s} directory should only have files in it, normally.
    The main cause of files in C{%(mandir)s} is confusion in packages
    about whether "mandir" means /usr/share/man or /usr/share/man/man<n>.
    """
    invariantinclusions = [ ('%(mandir)s/[^/][^/]*$', None, stat.S_IFDIR) ]

    def doFile(self, file):
	raise PackagePolicyError("%s is non-directory file in mandir" %file)


class ImproperlyShared(policy.Policy):
    """
    The %(datadir)s directory (normally /usr/share) is intended for
    data that can be shared between architectures; therefore, no
    ELF files should be there.
    """
    invariantsubtrees = [ '/usr/share/' ]

    def doFile(self, file):
        m = self.recipe.magic[file]
	if m and m.name == "ELF":
	    raise PackagePolicyError(
		"Architecture-specific file %s in shared data directory" %file)


class DanglingSymlinks(policy.Policy):
    """
    Disallow dangling symbolic links (symbolic links which point to
    files which do not exist).  If you know that a dangling symbolic
    link created by your package is fulfilled by another package on
    which your package depends, you may set up an exception for that
    file for the C{DanglingSymlinks} policy.
    """
    targetexceptions = [
	'.*consolehelper'
    ]
    def doProcess(self, recipe):
	self.targetFilters = []
	self.macros = recipe.macros # for filterExpression
	for targetitem in self.targetexceptions:
	    filterargs = self.filterExpression(targetitem)
	    self.targetFilters.append(filter.Filter(*filterargs))
	policy.Policy.doProcess(self, recipe)

    def doFile(self, file):
	d = self.macros.destdir
	f = util.joinPaths(d, file)
	if os.path.islink(f):
	    contents = os.readlink(f)
	    if contents[0] == '/':
		log.warning('Absolute symlink %s points to %s, should probably be relative', file, contents)
		return
	    try:
		os.stat(f)
	    except OSError:
		for targetFilter in self.targetFilters:
		    if targetFilter.match(contents):
			# contents are an exception
			log.debug('allowing special dangling symlink %s -> %s',
				  file, contents)
			return
		raise PackagePolicyError(
		    "Dangling symlink: %s points to non-existant %s"
		    %(file, contents))


class CheckSonames(policy.Policy):
    """
    Make sure that .so -> SONAME -> fullname
    """
    invariantinclusions = [ (r'..*\.so', None, stat.S_IFDIR), ]
    def doFile(self, path):
	d = self.macros.destdir
	destlen = len(d)
	l = util.joinPaths(d, path)
	if not os.path.islink(l):
	    m = self.recipe.magic[path]
	    if m and m.name == 'ELF' and 'soname' in m.contents:
		if os.path.basename(path) == m.contents['soname']:
		    target = m.contents['soname']+'.something'
		else:
		    target = m.contents['soname']
		log.warning(
		    '%s is not a symlink but probably should be a link to %s',
		    path, target)
	    return

	# store initial contents
	sopath = util.joinPaths(os.path.dirname(l), os.readlink(l))
	so = util.normpath(sopath)
	# find final file
	while os.path.islink(l):
	    l = util.normpath(util.joinPaths(os.path.dirname(l),
					     os.readlink(l)))

	p = util.joinPaths(d, path)
	linkpath = l[destlen:]
	m = self.recipe.magic[linkpath]

	if m and m.name == 'ELF' and 'soname' in m.contents:
	    if so == linkpath:
		log.debug('%s is final path, soname is %s;'
		    ' soname usually is symlink to specific implementation',
		    linkpath, m.contents['soname'])
	    soname = util.normpath(util.joinPaths(
			os.path.dirname(sopath), m.contents['soname']))
	    s = soname[destlen:]
	    try:
		os.stat(soname)
		if not os.path.islink(soname):
		    log.warning('%s has soname %s; therefore should be a symlink',
			s, m.contents['soname'])
	    except:
		log.warning("%s implies %s, which does not exist --"
			    " use Ldconfig('%s')?", path, s,
			    os.path.dirname(path))


# now the packaging classes

class _filterSpec(policy.Policy):
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
	policy.Policy.updateArgs(self, [], **keywords)


class ComponentSpec(_filterSpec):
    """
    Determines which component each file is in.
    """
    baseFilters = (
	# automatic subpackage names and sets of regexps that define them
	# cannot be a dictionary because it is ordered; first match wins
	('runtime',   ('%(essentiallibdir)s/security/',
		       '/lib/security/',
		       '%(datadir)s/gnome/help/.*/C/')), # help menu stuff
	('python',    ('%(libdir)s/python.*/site-packages/')),
	('devel',     ('\.so',), stat.S_IFLNK),
	('devel',     ('\.a',
		       '.*/include/.*\.h',
		       '%(includedir)s/',
		       '%(mandir)s/man(2|3)/',
		       '%(datadir)s/aclocal/',
		       '%(libdir)s/pkgconfig/',
		       '%(bindir)s/..*-config')),
	('lib',       ('.*/lib/.*\.so.*')),
	# note that gtk-doc is not well-named; it is a shared system, like info,
	# and is used by unassociated tools (devhelp)
	('doc',       ('%(datadir)s/(gtk-doc|doc|man|info)/')),
	('locale',    ('%(datadir)s/locale/',
		       '%(datadir)s/gnome/help/.*/')),
	('emacs',     ('%(datadir)s/emacs/site-lisp/.*',)),
    )
    keywords = { 'catchall': 'runtime' }

    def __init__(self, *args, **keywords):
        """
        @keyword catchall: The component name which gets all otherwise
        unassigned files.  Default: C{runtime}
        """
        _filterSpec.__init__(self, *args, **keywords)

    def doProcess(self, recipe):
	compFilters = []
	self.macros = recipe.macros

	# the extras need to come first in order to override decisions
	# in the base subfilters
	for (filteritem) in self.extraFilters + list(self.baseFilters):
	    name = filteritem[0] % self.macros
	    assert(name != 'source')
	    filterargs = self.filterExpression(filteritem[1:], name=name)
	    compFilters.append(filter.Filter(*filterargs))
	# by default, everything that hasn't matched a filter pattern yet
	# goes in the catchall component ('runtime' by default)
	compFilters.append(filter.Filter('.*', self.macros, name=self.catchall))

	# pass these down to PackageSpec for building the package
	recipe.PackageSpec(compFilters=compFilters)

class PackageSpec(_filterSpec):
    """
    Determines which package each file is in.
    """
    keywords = { 'compFilters': None }

    def __init__(self, *args, **keywords):
        """
        @keyword compFilters: reserved for C{ComponentSpec} to pass information
        needed by C{PackageSpec}.
        """
        _filterSpec.__init__(self, *args, **keywords)
        

    def doProcess(self, recipe):
	pkgFilters = []
	self.macros = recipe.macros

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
	    pkgFilters, self.compFilters)

	# now walk the tree -- all policy classes after this require
	# that the initial tree is built
        recipe.autopkg.walk(self.macros['destdir'])


def _markConfig(recipe, filename):
    log.debug('config: %s', filename)
    recipe.autopkg.pathMap[filename].flags.isConfig(True)

class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files
    """
    invariantsubtrees = [ '%(sysconfdir)s' ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    _markConfig(self.recipe, file)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files
    """
    keywords = {
	'inclusions': []
    }

    def __init__(self, *args, **keywords):
        """
        @keyword inclusions: regexp(s) specifying files to be included.
        Do not mention files in /etc, which are already covered by the
        EtcConfig class.
        @type inclusions: None, regexp string, sequence of regexp strings.
        """
        policy.Policy.__init__(self, *args, **keywords)
        

    def updateArgs(self, *args, **keywords):
	"""
	Config(pathregex(s)...)
	"""
	if args:
	    self.inclusions.extend(args)
	inclusions = keywords.pop('inclusions', None)
	if inclusions:
	    self.inclusions.append(inclusions)
	policy.Policy.updateArgs(self, [], **keywords)

    def doProcess(self, recipe):
	self.configFilters = []
	if self.inclusions:
	    if not isinstance(self.inclusions, (tuple, list)):
		self.inclusions = (self.inclusions,)
	    for inclusion in self.inclusions:
		self.configFilters.append(
		    filter.Filter(inclusion, recipe.macros))
	policy.Policy.doProcess(self, recipe)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    for configFilter in self.configFilters:
		if configFilter.match(file):
		    _markConfig(self.recipe, file)


class InitScript(policy.Policy):
    """
    Mark initscripts as such so that chkconfig will be run.
    By default, every file in %(initdir)s is marked as an initscript.
    """
    invariantinclusions = [ '%(initdir)s/.[^/]*$' ]

    def _markInitScript(self, filename):
	log.debug('initscript: %s', filename)
	self.recipe.autopkg.pathMap[filename].flags.isInitScript(True)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    self._markInitScript(file)


class SharedLibrary(policy.Policy):
    """
    Mark system shared libaries as such so that ldconfig will be run.
    """
    # keep invariants in sync with ExecutableLibraries
    invariantsubtrees = [
	'%(libdir)s/',
	'%(essentiallibdir)s/',
	'%(krbprefix)s/%(lib)s/',
	'%(x11prefix)s/%(lib)s/',
	'%(prefix)s/local/%(lib)s/',
    ]
    invariantinclusions = [
	(r'..*\.so\..*', None, stat.S_IFDIR),
    ]

    def _markSharedLibrary(self, filename):
	log.debug('shared library: %s', filename)
	self.recipe.autopkg.pathMap[filename].flags.isShLib(True)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath) and \
	   self.recipe.magic[file].name == 'ELF':
	    self._markSharedLibrary(file)


class ParseManifest(policy.Policy):
    """
    Parse a file containing a manifest intended for RPM, finding the
    information that can't be represented by pure filesystem status
    with a non-root built: device files (%dev), directory responsibility
    (%dir), and ownership (%attr).  It translates these into the
    related SRS construct for each.  There is no equivalent to
    %defattr -- our default ownership is root:root, and permissions
    (except for setuid and setgid files) are collected from the filesystem.

    XXX I think this parsing may not be sufficient for all manifests,
    tested only with MAKEDEV output so far.
    """

    def __init__(self, *args, **keywords):
	self.paths = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	ParseManifest(path(s)...)
	"""
	if args:
	    self.paths.extend(args)
	policy.Policy.updateArgs(self, [], **keywords)

    def do(self):
	for path in self.paths:
	    self.processPath(path)

    def processPath(self, path):
	if not path.startswith('/'):
	    path = self.macros['builddir'] + os.sep + path
        f = open(path)
        for line in f:
            line = line.strip()
            fields = line.split(')')

            attr = fields[0].lstrip('%attr(').split(',')
            perms = attr[0].strip()
            owner = attr[1].strip()
            group = attr[2].strip()

            fields[1] = fields[1].strip()
            if fields[1].startswith('%dev('):
                dev = fields[1][5:].split(',')
                devtype = dev[0]
                major = dev[1]
                minor = dev[2]
                target = fields[2].strip()
                self.recipe.MakeDevices(target, devtype, int(major), int(minor),
                                        owner, group, int(perms, 0))
            elif fields[1].startswith('%dir '):
                target = fields[1][5:]
		# XXX not sure what we should do here...
                dironly = 1
            else:
		# XXX is this right?
                target = fields[1].strip()
		if int(perms, 0) & 06000:
		    self.recipe.AddModes(int(perms, 0), target)
		if owner != 'root' or group != 'root':
		    self.recipe.Ownership(owner, group, target)


class MakeDevices(policy.Policy):
    """
    Make device nodes
    """
    def __init__(self, *args, **keywords):
	self.devices = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	MakeDevices(path, devtype, major, minor, owner, group, perms=0400)
	"""
	if args:
	    l = len(args)
	    # perms is optional, all other arguments must be there
	    assert((l > 5) and (l < 8))
	    if l == 6:
		args.append(0400)
	    self.devices.append(args)
	policy.Policy.updateArgs(self, [], **keywords)

    def do(self):
        for device in self.devices:
            self.recipe.autopkg.addDevice(*device)


class AddModes(policy.Policy):
    """
    Apply suid/sgid modes -- use SetModes in recipes; this is just the
    combined back end to SetModes and ParseManifest.
    """
    def __init__(self, *args, **keywords):
	self.fixmodes = {}
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	AddModes(mode, path(s)...)
	"""
	if args:
	    for path in args[1:]:
		self.fixmodes[path] = args[0]
	policy.Policy.updateArgs(self, [], **keywords)

    def doFile(self, path):
	if path in self.fixmodes:
	    mode = self.fixmodes[path]
	    # set explicitly, do not warn
	    self.recipe.WarnWriteable(exceptions=path)
	    log.debug('suid/sgid: %s mode 0%o', path, mode & 07777)
	    self.recipe.autopkg.pathMap[path].inode.setPerms(mode)


class WarnWriteable(policy.Policy):
    """
    Unless a mode has been set explicitly (i.e. with SetModes), warn
    about group- or other-writeable files.
    """
    # Needs to run after AddModes because AddModes sets exceptions
    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.islink(fullpath):
	    return
	if file not in self.recipe.autopkg.pathMap:
	    # directory has been deleted
	    return
	mode = os.lstat(fullpath)[stat.ST_MODE]
	if mode & 022:
	    if stat.S_ISDIR(mode):
		type = "directory"
	    else:
		type = "file"
	    log.warning('Possibly inappropriately writeable permission'
			' 0%o for %s %s', mode & 0777, type, file)


class WarnIgnoredSetuid(policy.Policy):
    """
    Files/directories that are setuid/setgid in the filesystem
    but do not have that mode explicitly set in the recipe will
    be packaged without setuid/setgid bits set.  This might be
    a bug, so flag it with a warning.
    """
    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	mode = os.lstat(fullpath)[stat.ST_MODE]
	if mode & 06000 and \
	   not self.recipe.autopkg.pathMap[file].inode.perms() & 06000:
	    if stat.S_ISDIR(mode):
		type = "directory"
	    else:
		type = "file"
	    log.warning('%s %s has unpackaged set{u,g}id mode 0%o in filesystem'
			%(type, file, mode&06777))


class Ownership(policy.Policy):
    """
    Set user and group ownership of files.  The default is
    root:root
    """
    def __init__(self, *args, **keywords):
	self.filespecs = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	call as::
	  Ownership(user, group, filespec(s)...)
	List them in order, most specific first, ending with most
	general; the filespecs will be matched in the order that
	you provide them.
	"""
	if args:
	    for filespec in args[2:]:
		self.filespecs.append((filespec, args[0], args[1]))
	policy.Policy.updateArgs(self, [], **keywords)

    def doProcess(self, recipe):
	# we must NEVER take ownership from the filesystem
	assert(not self.exceptions)
	self.fileFilters = []
	for (filespec, user, group) in self.filespecs:
	    self.fileFilters.append(
		(filter.Filter(filespec, recipe.macros), user, group))
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
	    pkgfile.inode.setOwner(owner)
	if group:
	    pkgfile.inode.setGroup(group)


class ExcludeDirectories(policy.Policy):
    """
    In SRS, there are only two reasons to package a directory: the
    directory needs permissions other than 0755, or it must exist
    even if it is empty.

    In order to include a directory in a package, call
    C{self.SetMode(path, mode)} where C{mode} is not C{0755},
    or call C{ExcludeDirectories(exceptions=path)} if the
    directory mode is C{0755}
    """
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]

    def doFile(self, path):
	log.debug('excluding directory: %s', path)
	del self.recipe.autopkg.pkgMap[path][path]
	del self.recipe.autopkg.pkgMap[path]
	del self.recipe.autopkg.pathMap[path]


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	NonBinariesInBindirs(),
	FilesInMandir(),
	ImproperlyShared(),
	DanglingSymlinks(),
	CheckSonames(),
	ComponentSpec(),
	PackageSpec(),
	EtcConfig(),
	Config(),
	InitScript(),
	SharedLibrary(),
	ParseManifest(),
	MakeDevices(),
	AddModes(),
	WarnWriteable(),
	WarnIgnoredSetuid(),
	Ownership(),
	ExcludeDirectories(),
    ]


class PackagePolicyError(policy.PolicyError):
    pass
