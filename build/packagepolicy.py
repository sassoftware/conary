#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import util
import re
import os
import policy
import log
import buildpackage

"""
Module used by recipes to effect packaging policy; things like setting
hints, flags, and dependencies.
Classes from this module are not used directly; instead, they are used
through eponymous interfaces in recipe.
"""

class _filterSpec(policy.Policy):
    def __init__(self, *args, **keywords):
	self.extraFilters = []
	policy.Policy.__init__(self, *args, **keywords)

    def updateArgs(self, *args, **keywords):
	"""
	ThisClass('<name>', 'regex1', 'regex2'...)
	"""
	if args:
	    self.extraFilters.append((args[0], args[1:]))
	policy.Policy.updateArgs(self, [], **keywords)


class ComponentSpec(_filterSpec):
    """
    Determines which component each file is in.
    """
    # XXX TEMPORARY - remove directories such as /usr/include from this
    # list when filesystem package is in place and we have install
    # ordering.
    baseFilters = (
	# automatic subpackage names and sets of regexps that define them
	# cannot be a dictionary because it is ordered; first match wins
	('python',    ('%(libdir)s/python.*/site-packages/.*')),
	('devel',     ('\.a',
		       '\.so',
		       '.*/include/.*\.h',
		       '%(includedir)s/',
		       '%(includedir)s',
		       '%(mandir)s/man(2|3)/',
		       '%(mandir)s/man(2|3)',
		       '%(datadir)s/aclocal/',
		       '%(datadir)s/aclocal')),
	('lib',       ('.*/lib/.*\.so\..*')),
	# note that gtk-doc is not well-named; it is a shared system, like info,
	# and is used by unassociated tools (devhelp)
	('doc',       ('%(datadir)s/(gtk-doc|doc|man|info)/',
		       '%(datadir)s/(gtk-doc|doc|man|info)')),
	('locale',    ('%(datadir)s/locale/',
		       '%(datadir)s/locale')),
	('runtime',   ('.*',)),
    )

    def doProcess(self, recipe):
	compFilters = []
	macros = recipe.macros

	# the extras need to come first in order to override decisions
	# in the base subfilters
	for (name, patterns) in self.extraFilters:
	    name = name %macros
	    assert(name != 'sources')
	    compFilters.append(buildpackage.Filter(name, patterns, macros))
	for (name, patterns) in self.baseFilters:
	    assert(name != 'sources')
	    compFilters.append(buildpackage.Filter(name, patterns, macros))

	# pass these down to PackageSpec for building the package
	recipe.PackageSpec(compFilters=compFilters)

class PackageSpec(_filterSpec):
    keywords = { 'compFilters': None }

    def doProcess(self, recipe):
	pkgFilters = []
	macros = recipe.macros

	for (name, patterns) in self.extraFilters:
	    pkgFilters.append(buildpackage.Filter(name %macros, patterns, macros))
	# by default, everything that hasn't matched a pattern in the
	# main package filter goes in the package named recipe.name
	pkgFilters.append(buildpackage.Filter(recipe.name, '.*', macros))

	# OK, all the filters exist, build an autopackage object that
	# knows about them
	recipe.autopkg = buildpackage.AutoBuildPackage(
	    recipe.namePrefix, recipe.fullVersion,
	    pkgFilters, self.compFilters)

	# now walk the tree -- all policy classes after this require
	# that the initial tree is built
        recipe.autopkg.walk(macros['destdir'])


def _markConfig(recipe, filename):
    packages = recipe.autopkg.packages
    for package in packages.keys():
	if filename in packages[package]:
            log.debug('config: %s', filename)
	    packages[package][filename].isConfig(True)

class EtcConfig(policy.Policy):
    """
    Mark all files below /etc as config files
    """
    invariantsubtree = [ '%(sysconfdir)s' ]

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    _markConfig(self.recipe, file)


class Config(policy.Policy):
    """
    Mark only explicit inclusions as config files
    """
    keywords = {
	'inclusions': None
    }

    def __init__(self, *args, **keywords):
        """
        @keyword inclusions: regexp(s) specifying files to be included.
        Do not mention files in /etc, which are already covered by the
        EtcConfig class.
        @type inclusions: None, regexp string, sequence of regexp strings.
        """
        policy.Policy.__init__(self, *args, **keywords)
        

    def doProcess(self, recipe):
	self.configREs = []
	if self.inclusions:
	    if not isinstance(self.inclusions, (tuple, list)):
		self.inclusions = (self.inclusions,)
	    for inclusion in self.inclusions:
		self.configREs.append(re.compile(inclusion %self.macros))
	policy.Policy.doProcess(self, recipe)

    def doFile(self, file):
	fullpath = ('%(destdir)s/'+file) %self.macros
	if os.path.isfile(fullpath) and not os.path.islink(fullpath):
	    for configRE in self.configREs:
		if configRE.search(file):
		    _markConfig(self.recipe, file)


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
	    packages = self.recipe.autopkg.packages
	    for package in packages.keys():
		if path in packages[package]:
		    log.debug('suid/sgid: %s', path)
		    packages[package][path].perms(mode)


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
	Ownership(user, group, filespec(s)...)
	"""
	if args:
	    for filespec in args[2:]:
		self.filespecs.append((filespec, args[0], args[1]))
	policy.Policy.updateArgs(self, [], **keywords)

    def doProcess(self, recipe):
	# we must NEVER take ownership from the filesystem
	assert(not self.exceptions)
	self.fileREs = []
	for (filespec, user, group) in self.filespecs:
	    self.fileREs.append((re.compile(filespec %recipe.macros),
	                        user, group))
	del self.filespecs
	policy.Policy.doProcess(self, recipe)

    def doFile(self, path):
	for (regex, owner, group) in self.fileREs:
	    if regex.search(path):
		self._markOwnership(path, owner, group)
		return
	self._markOwnership(path, 'root', 'root')

    def _markOwnership(self, filename, owner, group):
	packages = self.recipe.autopkg.packages
	for package in packages.keys():
	    if filename in packages[package]:
		pkgfile = packages[package][filename]
		if owner:
		    pkgfile.theOwner = owner
		if group:
		    pkgfile.theGroup = group


def DefaultPolicy():
    """
    Return a list of actions that expresses the default policy.
    A recipe can then modify this list if necessary.
    """
    return [
	ComponentSpec(),
	PackageSpec(),
	EtcConfig(),
	Config(),
	ParseManifest(),
	MakeDevices(),
	AddModes(),
	Ownership(),
    ]
