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

import util
import os
import policy
import log
import stat
import tags
import buildpackage
import filter

"""
Module used by recipes to effect packaging policy; things like setting
hints, flags, and dependencies.
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

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
	    self.recipe.reportErrors(
		"%s has mode 0%o with no executable permission in bindir"
		%(file, mode))
	m = self.recipe.magic[file]
	if m and m.name == 'ltwrapper':
	    self.recipe.reportErrors(
		"%s is a build-only libtool wrapper script" %file)


class FilesInMandir(policy.Policy):
    """
    The C{%(mandir)s} directory should only have files in it, normally.
    The main cause of files in C{%(mandir)s} is confusion in packages
    about whether "mandir" means /usr/share/man or /usr/share/man/man<n>.
    """
    invariantinclusions = [ ('%(mandir)s/[^/][^/]*$', None, stat.S_IFDIR) ]

    def doFile(self, file):
	self.recipe.reportErrors("%s is non-directory file in mandir" %file)


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
	    self.recipe.reportErrors(
		"Architecture-specific file %s in shared data directory" %file)


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

class CheckDestDir(policy.Policy):
    """
    Look for the destdir path in files and symlink contents; it should
    not be there.
    """
    def doFile(self, file):
	d = self.macros.destdir
	if file.find(d) != -1:
	    self.recipe.reportErrors('Path %s contains destdir %s' %(file, d))
	fullpath = d+file
	if os.path.islink(fullpath):
	    contents = os.readlink(fullpath)
	    if contents.find(d) != -1:
		self.recipe.reportErrors(
		    'Symlink %s contains destdir %s in contents %s'
		    %(file, d, contents))


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
	policy.Policy.updateArgs(self, **keywords)


class ComponentSpec(_filterSpec):
    """
    Determines which component each file is in.
    """
    baseFilters = (
	# automatic subpackage names and sets of regexps that define them
	# cannot be a dictionary because it is ordered; first match wins
	('test',      ('%(testdir)s/')),
	('runtime',   ('%(essentiallibdir)s/security/',
		       '/lib/security/',
		       r'%(libdir)s/perl./vendor_perl/', # modules, not shlibs
		       '%(datadir)s/gnome/help/.*/C/')), # help menu stuff
	('python',    ('%(libdir)s/python.*/site-packages/')),
	('devel',     (r'\.so',), stat.S_IFLNK),
	('devel',     (r'\.a',
		       r'.*/include/.*\.h',
		       '%(includedir)s/',
		       '%(mandir)s/man(2|3)/',
		       '%(datadir)s/aclocal/',
		       '%(libdir)s/pkgconfig/',
		       '%(bindir)s/..*-config')),
	('lib',       (r'.*/lib/.*\.so.*')),
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
	self.rootdir = self.rootdir % recipe.macros

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
	    pkgFilters, self.compFilters)

	# now walk the tree -- all policy classes after this require
	# that the initial tree is built
        recipe.autopkg.walk(self.macros['destdir'])


def _markConfig(recipe, filename, fullpath):
    log.debug('config: %s', filename)
    f = file(fullpath)
    f.seek(0, 2)
    if f.tell():
	# file has contents
	f.seek(-1, 2)
	lastchar = f.read(1)
	f.close()
	if lastchar != '\n':
	    recipe.reportErrors("config file %s missing trailing newline" %filename)
    recipe.autopkg.pathMap[filename].flags.isConfig(True)

class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files
    """
    invariantsubtrees = [ '%(sysconfdir)s', '%(taghandlerdir)s']

    def doFile(self, file):
        m = self.recipe.magic[file]
	if m and m.name == "ELF":
	    # an ELF file cannot be a config file, some programs put
	    # ELF files under /etc (X, for example), and tag handlers
	    # can be ELF or shell scripts; we just want tag handlers
	    # to be config files if they are shell scripts.
	    # Just in case it was not intentional, warn...
	    log.debug('ELF file %s found in config directory', file)
	    return
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    _markConfig(self.recipe, file, fullpath)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files
    """

    keywords = policy.Policy.keywords.copy()
    keywords['inclusions'] = []

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    _markConfig(self.recipe, file, fullpath)


class Transient(policy.Policy):
    """
    Mark files that have transient contents as such.  Transient contents
    are contents that should be overwritten by a new version without
    question at update time; almost the opposite of configuration files.
    """
    invariantinclusions = [
	r'..*\.py(c|o)$',
    ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    log.debug('transient: %s', file)
	    self.recipe.autopkg.pathMap[file].flags.isConfig(True)


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

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath) and \
	   self.recipe.magic[file].name == 'ELF':
	    log.debug('shared library: %s', file)
	    self.recipe.autopkg.pathMap[file].tags.set("shlib")


class TagDescription(policy.Policy):
    """
    Mark tag description files as such so that conary handles the
    correctly.  By default, every file in %(sysconfdir)s/conary/tags/
    is marked as a tag description file.
    """
    invariantinclusions = [ '%(sysconfdir)s/conary/tags/.[^/]*$' ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and util.isregular(fullpath):
	    log.debug('conary tag file: %s', file)
	    self.recipe.autopkg.pathMap[file].tags.set("tagdescription")


class TagSpec(policy.Policy):
    """
    Apply tags defined by tag descriptions in both the current system
    and %(destdir)s to all the files in %(destdir)s.
    """
    keywords = {
	'included': {},
	'excluded': {}
    }

    def updateArgs(self, *args, **keywords):
	"""
	Call as::
	    C{TagSpec(I{tagname}, I{filterexp})}
	or::
	    C{TagSpec(I{tagname}, exceptions=I{filterexp})}
	where C{I{filterexp}} is either a regular expression or a
	tuple of C{(regexp[, setmodes[, unsetmodes]])}
	"""
	if args:
	    args = list(args)
	    tagname = args.pop(0)
	    if args:
		if tagname not in self.included:
		    self.included[tagname] = []
		self.included[tagname].extend(args)
	    if 'exceptions' in keywords:
		# not the usual exception handling
		if tagname not in self.excluded:
		    self.excluded[tagname] = []
		self.excluded[tagname].append(keywords.pop('exceptions'))
	policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
	self.rootdir = self.rootdir % recipe.macros
	self.tagList = []
	# read the system and %(destdir)s tag databases
	for directory in (recipe.macros.destdir+'/etc/conary/tags/',
			  '/etc/conary/tags/'):
	    if os.path.isdir(directory):
		for filename in os.listdir(directory):
		    path = util.joinPaths(directory, filename)
		    self.tagList.append(tags.TagFile(path, recipe.macros))

	# instantiate filters
	d = {}
	for tagname in self.included:
	    l = []
	    for item in self.included[tagname]:
		l.append(filter.Filter(item, recipe.macros))
	    d[tagname] = l
	self.included = d

	d = {}
	for tagname in self.excluded:
	    l = []
	    for item in self.excluded[tagname]:
		l.append(filter.Filter(item, recipe.macros))
	    d[tagname] = l
	self.excluded = d

	policy.Policy.doProcess(self, recipe)


    def markTag(self, name, tag, file):
	log.debug('%s: %s', name, file)
	self.recipe.autopkg.pathMap[file].tags.set(tag)

    def doFile(self, file):
	fullpath = self.recipe.macros.destdir+file
	if not util.isregular(fullpath) and not os.path.islink(fullpath):
	    return
	for tag in self.included:
	    for filt in self.included[tag]:
		if filt.match(file):
		    self.markTag(tag, tag, file)
	for tag in self.tagList:
	    if tag.match(file):
		if tag.name:
		    name = tag.name
		else:
		    name = tag.tag
		if tag.tag in self.excluded:
		    for filt in self.excluded[tag.tag]:
			# exception handling is per-tag, so handled specially
			if filt.match(file):
			    log.debug('ignoring tag match for %s: %s',
				      name, file)
			    return
		self.markTag(name, tag.tag, file)


class ParseManifest(policy.Policy):
    """
    Parse a file containing a manifest intended for RPM, finding the
    information that can't be represented by pure filesystem status
    with a non-root built: device files (%dev), directory responsibility
    (%dir), and ownership (%attr).  It translates these into the
    related Conary construct for each.  There is no equivalent to
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
	policy.Policy.updateArgs(self, **keywords)

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
	policy.Policy.updateArgs(self, **keywords)

    def do(self):
        for device in self.devices:
            self.recipe.autopkg.addDevice(*device)


class DanglingSymlinks(policy.Policy):
    # This policy must run after all modifications to the packging
    # are complete becuase it counts on self.recipe.autopkg.pathMap
    # being final
    """
    Disallow dangling symbolic links (symbolic links which point to
    files which do not exist).  If you know that a dangling symbolic
    link created by your package is fulfilled by another package on
    which your package depends, you may set up an exception for that
    file for the C{DanglingSymlinks} policy.
    """
    invariantexceptions = (
	'%(testdir)s/.*', )
    targetexceptions = [
	'.*consolehelper',
	'/proc/', # provided by the kernel, no package
    ]
    # XXX consider automatic file dependencies for dangling symlinks?
    # XXX if so, then we'll need exceptions for that too, for things
    # XXX like symlinks into /proc
    def doProcess(self, recipe):
	self.rootdir = self.rootdir % recipe.macros
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
	    abscontents = util.joinPaths(os.path.dirname(file), contents)
	    if abscontents in self.recipe.autopkg.pathMap:
		pkgMap = self.recipe.autopkg.pkgMap
		if pkgMap[abscontents] != pkgMap[file] and \
		   not file.endswith('.so') and \
		   not pkgMap[file].getName().endswith(':test'):
		    # warn about suspicious cross-component symlink
		    log.warning('symlink %s points from package %s to %s',
				file, pkgMap[file].getName(),
				pkgMap[abscontents].getName())
	    else:
		for targetFilter in self.targetFilters:
		    if targetFilter.match(abscontents):
			# contents are an exception
			log.debug('allowing special dangling symlink %s -> %s',
				  file, contents)
			return
		self.recipe.reportErrors(
		    "Dangling symlink: %s points to non-existant %s (%s)"
		    %(file, contents, abscontents))


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
	policy.Policy.updateArgs(self, **keywords)

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


class IgnoredSetuid(policy.Policy):
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
	policy.Policy.updateArgs(self, **keywords)

    def doProcess(self, recipe):
	# we must NEVER take ownership from the filesystem
	assert(not self.exceptions)
	self.rootdir = self.rootdir % recipe.macros
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
    In Conary, there are only two reasons to package a directory: the
    directory needs permissions other than 0755, or it must exist
    even if it is empty.  Packages do not need to explicitly include
    a directory just to ensure that there is a place to put a file;
    Conary will appropriately create the directory, and delete it later
    if the directory becomes empty.

    The ExcludeDirectories policy causes directories to be excluded
    from the package.  You can set exceptions to this policy with
    C{ExcludeDirectories(exceptions=I{regexp})} and the directories
    matching the regular expression will be included in the package.

    However, it should generally not be necessary to invoke this
    policy directly, because the most common reason to include a
    directory in a package is that it needs permissions other than
    0755, so simply call C{SetMode(I{path(s)}, I{mode})} where
    C{I{mode}} is not C{0755}, and the directory will automatically
    included.
    """
    invariantinclusions = [ ('.*', stat.S_IFDIR) ]

    def doFile(self, path):
	s = os.lstat(self.recipe.macros.destdir + os.sep + path)
	mode = s[stat.ST_MODE]
	if mode & 0777 != 0755:
	    log.debug('excluding directory %s with mode %o', path, mode&0777)
	elif s[stat.ST_NLINK] == 2:
	    log.debug('excluding empty directory %s', path)
	del self.recipe.autopkg.pkgMap[path][path]
	del self.recipe.autopkg.pkgMap[path]
	del self.recipe.autopkg.pathMap[path]


class _requirements(policy.Policy):
    def doFile(self, path):
	pkgMap = self.recipe.autopkg.pkgMap
	if path not in pkgMap:
	    return
	pkg = pkgMap[path]
	f = pkg.getFile(path)
	if f.hasContents:
	    self.addOne(path, pkg, f)
    def addOne(self, path, pkg, f):
	'pure virtual'
	pass

class Requires(_requirements):
    def addOne(self, path, pkg, f):
	pkg.requires.union(f.requires.value())

class Provides(_requirements):
    def addOne(self, path, pkg, f):
	pkg.provides.union(f.provides.value())

class Flavor(_requirements):
    def addOne(self, path, pkg, f):
	pkg.flavor.union(f.flavor.value())



class reportErrors(policy.Policy):
    """
    This class is used to pull together all package errors in the
    sanity-checking rules that come above it.  Do not call it
    directly; it is for internal use only!

    It must come after all the other package classes that report
    fatal errors, so might as well come last.
    """
    def __init__(self, *args, **keywords):
	self.warnings = []
	policy.Policy.__init__(self, *args, **keywords)
    def updateArgs(self, *args, **keywords):
	"""
	Called once, with printf-style arguments, for each warning.
	"""
	self.warnings.append(args[0] %args[1:])
    def do(self):
	if self.warnings:
	    for warning in self.warnings:
		log.error(warning)
	    raise PackagePolicyError, 'Package Policy errors found:\n%s' %"\n".join(self.warnings)



def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	NonBinariesInBindirs(),
	FilesInMandir(),
	ImproperlyShared(),
	CheckSonames(),
	CheckDestDir(),
	ComponentSpec(),
	PackageSpec(),
	EtcConfig(),
	Config(),
	Transient(),
	SharedLibrary(),
	TagDescription(),
	TagSpec(),
	ParseManifest(),
	MakeDevices(),
	DanglingSymlinks(),
	AddModes(),
	WarnWriteable(),
	IgnoredSetuid(),
	Ownership(),
	ExcludeDirectories(),
	Requires(),
	Provides(),
	Flavor(),
	reportErrors(),
    ]


class PackagePolicyError(policy.PolicyError):
    pass
