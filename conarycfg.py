#
# Copyright (c) 2004-2005 Specifix, Inc.
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
Provides Conary's generic config file format, and implements conaryrc
handling.
"""
import os
import sys

import deps
import deps.arch
import deps.deps
from build import use
from lib import util
import versions


(STRING, 
    BOOL, 
    LABEL, 
    LABELLIST,
    STRINGDICT, 
    STRINGLIST, 
    CALLBACK, 
    EXEC, 
    STRINGPATH, 
    FLAVOR,
    INT) = range(11)

BOOLEAN=BOOL

class ConfigFile:

    defaults = {}

    def read(self, path, exception=False):
	if os.path.exists(path):
	    f = open(path, "r")
	    lineno = 1
	    for line in f:
		self.configLine(line, path, lineno)
		lineno = lineno + 1
	    f.close()
	elif exception:
	    raise IOError, "No such file or directory: '%s'" % path

    def __getitem__(self, name):
	return self.__dict__[name]

    def configLine(self, line, file = "override", lineno = '<No line>'):
        self.lineno = lineno
	line = line.strip()
	if not line or line[0] == '#':
	    return
        parts = line.split(None, 1)
        if len(parts) == 1:
            key = parts[0]
            val = ''
        else:
            (key, val) = parts
	(key, type) = self.checkKey(key, file)
	if key:
	    if type == EXEC:
		self.execCmd(key, val, file)
	    else:
		self.setValue(key, val, type, file)
	
    def execCmd(self, key, val, file):
	if key == 'includeConfigFile':
	    for cfgfile in util.braceGlob(val):
		self.read(cfgfile, exception=True)

    def checkKey(self, key, file = "override"):
	lckey = key.lower()
	# XXX may have to generalize this some day
	if lckey == 'includeconfigfile':
	    return ('includeConfigFile', EXEC)
	if not self.lowerCaseMap.has_key(lckey):
	   raise ParseError, ("%s:%s: configuration value '%s' unknown" % (file, self.lineno, key))
	else:
	    return (self.lowerCaseMap[lckey], None)
	

    def setValue(self, key, val, type=None, file="override"):
	if type == None:
	    type = self.types[key]
	if type == STRING:
	    self.__dict__[key] = val
	if type == INT:
            try:
	        self.__dict__[key] = int(val)
	    except:
		raise ParseError, ("%s:%s: expected integer for configuration value '%s'" % (file, self.lineno, key))
	elif type == STRINGDICT:
	    (idx, val) = val.split(None, 1)
	    self.__dict__[key][idx] = val
	elif type == STRINGLIST:
	    self.__dict__[key].append(val)
	elif type == STRINGPATH:
	    self.__dict__[key] = val.split(":")
	elif type == CALLBACK:
	    self.__dict__[key]('set', key, val)
	elif type == LABEL:
	    try:
		self.__dict__[key] = versions.Label(val)
	    except versions.ParseError, e:
		raise versions.ParseError, str(e)
	elif type == LABELLIST:
            self.__dict__[key] = []
            for labelStr in val.split():
                try:
                    self.__dict__[key].append(versions.Label(labelStr))
                except versions.ParseError, e:
                    raise versions.ParseError, str(e)
	elif type == BOOL:
	    if isinstance(val, bool):
		self.__dict__[key] = val
	    if val.lower() in ('0', 'false'):
		self.__dict__[key] = False
	    elif val.lower() in ('1', 'true'):
		self.__dict__[key] = True
	    else:
		raise ParseError, ("%s:%s: expected True or False for configuration value '%s'" % (file, self.lineno, key))
        elif type == FLAVOR:
            self.__dict__[key] = deps.deps.parseFlavor(val)

    def display(self, out=None):
        if out is None:
            out = sys.stdout
	keys = self.defaults.keys()
	keys.sort()
	for item in keys:
	    if (type(self.defaults[item]) == list):
		t = self.defaults[item][0]
	    else:
		t = STRING

	    if t == STRING:
		out.write("%-25s %s\n" % (item, self.__dict__[item]))
	    elif t == LABEL:
		out.write("%-25s %s\n" % (item, self.__dict__[item].asString()))
	    elif t == LABELLIST:
		out.write("%-25s %s\n" % (item, " ".join([x.asString() for x in self.__dict__[item]])))
	    elif t == STRINGPATH:
		out.write("%-25s %s\n" % (item, ":".join(self.__dict__[item])))
	    elif t == STRINGDICT:
		d = self.__dict__[item]
		idxs = d.keys()
		idxs.sort()
		for idx in idxs:
		    out.write("%-25s %-25s %s\n" % (item, idx, d[idx]))
	    elif t == CALLBACK:
		self.__dict__[item]('display')
	    elif t == FLAVOR:
                flavorStr = deps.deps.formatFlavor(self.__dict__[item])
                flavorList = flavorStr.split(",")

                str = ""
                hdr = item
                for item in flavorList:
                    if len(str) + len(item) > 40:
                        out.write("%-25s %s\n" % (hdr, str))
                        str = ""
                        hdr = ""
                    str += item + ","

                # chop off the trailing ,
                str = str[:-1]
                out.write("%-25s %s\n" % (hdr, str))
	    elif t == BOOL:
		out.write("%-25s %s\n" % (item, bool(self.__dict__[item])))
	    else:
		out.write("%-25s (unknown type)\n" % (item))


    def __init__(self):
	self.types = {}
	for (key, value) in self.defaults.items():
	    if isinstance(value, (list, tuple)):
		self.types[key] = value[0]
		self.__dict__[key] = value[1]
	    else:
		self.types[key] = STRING
		self.__dict__[key] = value
            if isinstance(self.__dict__[key], (list, tuple)):
                self.__dict__[key] = self.__dict__[key][:]
            if isinstance(self.__dict__[key], dict):
                self.__dict__[key] = self.__dict__[key].copy()

        self.lowerCaseMap = {}
        for (key, value) in self.__dict__.items():
            self.lowerCaseMap[key.lower()] = key

class ConaryConfiguration(ConfigFile):

    defaults = {
	'autoResolve'	        : [ BOOL, False ],
        'buildFlavor'           : [ FLAVOR, deps.deps.DependencySet() ],
	'buildLabel'	        : [ LABEL, versions.Label('localhost@local:trunk') ],
	'buildPath'		: '/usr/src/conary/builds',
	'contact'		: None,
	'dbPath'		: '/var/lib/conarydb',
	'debugRecipeExceptions' : [ BOOL, False ], 
	'dumpStackOnError'      : [ BOOL, True ], 
        'flavor'                : [ FLAVOR, deps.deps.DependencySet() ],
	'installLabelPath'	: [ LABELLIST, [] ],
	'lookaside'		: '/var/cache/conary',
	'name'			: None,
	'repositoryMap'	        : [ STRINGDICT, {} ],
	'root'			: '/',
	'sourceSearchDir'	: '.',
	'tmpDir'		: '/var/tmp/',
        'useDir'                : '/etc/conary/use',
    }

    def __init__(self, readConfigFiles=True):
	ConfigFile.__init__(self)

	self.pkgflags = {}
	self.useflags = {}
	self.archflags = {}
	self.macroflags = {}
	if readConfigFiles:
	    self.readFiles()

    def readFiles(self):
	self.read("/etc/conaryrc")
	if os.environ.has_key("HOME"):
	    self.read(os.environ["HOME"] + "/" + ".conaryrc")
	self.read("conaryrc")

    def checkKey(self, key, file = ""):
	if key.find('.') != -1:
	    directive,arg = key.split('.', 1)
	    directive = directive.lower()
	    if directive in ('use', 'flags', 'arch', 'macros'):
		return self.checkFlagKey(directive, arg)
	return ConfigFile.checkKey(self, key, file)
	
    def checkFlagKey(self, directive, key):
	if directive == 'use':
	    if key not in use.Use:
		raise ParseError, ("%s:%s: Unknown Use flag %s" % (file, self.lineno, key))
	    else:
		self.useflags[key] = True
		return ('Use.' + key, BOOL)
	if directive == 'macros':
	    self.macroflags[key] = True
	    return ('macros.' + key, STRING)
	elif directive == 'arch':
	    dicts = key.split('.')
	    flag = dicts[-1]
	    dicts = dicts[:-1]
	    curdict = self.archflags
	    for subdict in dicts:
		if not subdict in curdict:
		    # flag value, subflags
		    curdict[subdict] = [ None, {} ]
		curdict = curdict[subdict][1]
	    if flag in curdict:
		curdict[flag][0] = True
	    else:
		curdict[flag] = [ True, {} ]
	    return ('Arch.' + key, BOOL)
	elif directive == 'flags':
	    if key.find('.') == -1:
		raise ParseError, ("%s:%s: Flag %s must be of form package.flag" % (file, self.lineno, key))
	    else:
		package, flag = key.split('.', 1)
		if package not in self.pkgflags:
		    self.pkgflags[package] = {}
		self.pkgflags[package][flag] = True
		return ('Flags.' + key, BOOL)

    def _archKeys(self, prefix, curdict):
	keylist = []
	for key in curdict.keys():
	    if curdict[key][0]:
		keylist.append(prefix + key)
	    if curdict[key][1]:
		keylist.extend(self._archKeys(''.join([prefix,key,'.']), curdict[key][1]))
	return keylist

    def archKeys(self):
	return self._archKeys('', self.archflags)

    def listPkgs(self):
	return self.pkgflags.keys()

    def pkgKeys(self, pkg):
	return self.pkgflags.get(pkg, {}).keys()

    def useKeys(self):
	return self.useflags.keys()

    def macroKeys(self):
	return self.macroflags.keys()

    def display(self, out=None):
        if out is None:
            out = sys.stdout
        ConfigFile.display(self, out=out)
        for key in sorted(self.archKeys()):
            out.write('Arch.%-20s %-25s\n' % (key, self['Arch.' + key]))
        for key in sorted(self.useKeys()):
            out.write('Use.%-21s %-25s\n' % (key, self['Use.' + key]))
        for pkg in sorted(self.listPkgs()):
            for flag in sorted(self.pkgKeys(pkg)):
                key = 'Flags.%s.%s' % (pkg, flag)
                out.write('%-25s %-25s\n' % (key, self[key]))
        for macro in sorted(self.macroflags.keys()):
            key = 'macros.' + macro
            out.write('%-25s %-25s\n' % (key, self[key]))


class ConaryCfgError(Exception):

    """
    Ancestor for all exceptions raised by the conarycfg module.
    """

    pass

class ParseError(ConaryCfgError):

    """
    Indicates that an error occured parsing the config file.
    """

    def __str__(self):
	return self.str

    def __init__(self, str):
	self.str = str

def UseFlagDirectory(path):
    """
    Returns a dependency set reflecting the use flags specified by 
    directory path.
    """

    flags = []
    useFlags = deps.deps.DependencySet()

    if not os.path.exists(path):
	return useFlags

    for flag in os.listdir(path):
        filePath = os.path.join(path, flag)
        size = os.stat(filePath).st_size
        if not size:
            sense = deps.deps.FLAG_SENSE_PREFERRED
        else:
            val = open(filePath).read().strip().lower()
            if val == "disallowed":
                sense = deps.deps.FLAG_SENSE_DISALLOWED
            elif val == "preferred":
                sense = deps.deps.FLAG_SENSE_PREFERRED
            elif val == "prefernot":
                sense = deps.deps.FLAG_SENSE_PREFERNOT
            elif val == "required":
                sense = deps.deps.FLAG_SENSE_REQUIRED
            else:
		raise ParseError, ("%s: unknown use value %s") % (filePath, val)
                
        flags.append((flag, sense))

    useFlags.addDep(deps.deps.UseDependency, 
                    deps.deps.Dependency("use", flags))

    return useFlags
