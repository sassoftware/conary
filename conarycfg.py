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
import re
import sre_constants
import sys

import deps
import deps.arch
import deps.deps
from build import use
from lib import log,util
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
    INT,
    REGEXPLIST
) = range(12)

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
	elif type == REGEXPLIST:
            self.__dict__[key] = []
            for regexpStr in val.split():
                try:
                    self.__dict__[key].append(
                                        (regexpStr, re.compile(regexpStr)))
                except sre_constants.error, e:
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

    def displayKey(self, key, value, type, out):
        if type == STRING:
            out.write("%-25s %s\n" % (key, value))
        elif type == LABEL:
            out.write("%-25s %s\n" % (key, value.asString()))
        elif type == LABELLIST:
            out.write("%-25s %s\n" % (key, " ".join(x.asString() for x in value)))
        elif type == REGEXPLIST:
            out.write("%-25s %s\n" % (key, " ".join([x[0] for x in value])))
        elif type == STRINGPATH:
            out.write("%-25s %s\n" % (key, ":".join(value)))
        elif type == STRINGDICT:
            idxs = value.keys()
            idxs.sort()
            for idx in idxs:
                out.write("%-25s %-25s %s\n" % (key, idx, value[idx]))
        elif type == CALLBACK:
            self.__dict__[key]('display')
        elif type == FLAVOR:
            flavorStr = deps.deps.formatFlavor(value)
            flavorList = flavorStr.split(",")

            str = ""
            hdr = key
            for key in flavorList:
                if len(str) + len(key) > 40:
                    out.write("%-25s %s\n" % (hdr, str))
                    str = ""
                    hdr = ""
                str += key + ","

            # chop off the trailing ,
            str = str[:-1]
            out.write("%-25s %s\n" % (hdr, str))
        elif type == BOOL:
            out.write("%-25s %s\n" % (key, bool(value)))
        else:
            out.write("%-25s (unknown type)\n" % (key))


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
            self.displayKey(item, self[item], t, out)

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

    def initializeFlavors(self):
        import flavorcfg
        self.flavorConfig = flavorcfg.FlavorConfig(self.useDir, self.archDir)

        self.flavor = self.flavorConfig.toDependency(override=self.flavor)

        if not deps.deps.DEP_CLASS_IS in self.flavor.getDepClasses():
            insSet = deps.deps.DependencySet()
            for dep in deps.arch.currentArch:
                insSet.addDep(deps.deps.InstructionSetDependency, dep)
            self.flavor.union(insSet)

        # buildFlavor is installFlavor + overrides
        self.buildFlavor = deps.deps.overrideFlavor(self.flavor, 
                                                    self.buildFlavor)
	self.flavorConfig.populateBuildFlags()


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
        'excludeTroves'         : [ REGEXPLIST, [] ],
        'flavor'                : [ FLAVOR, deps.deps.DependencySet() ],
	'installLabelPath'	: [ LABELLIST, [] ],
        'localRollbacks'        : [ BOOL, False ],
	'lookaside'		: '/var/cache/conary',
	'name'			: None,
	'repositoryMap'	        : [ STRINGDICT, {} ],
	'root'			: '/',
	'sourceSearchDir'	: '.',
	'tmpDir'		: '/var/tmp/',
        'useDir'                : '/etc/conary/use',
        'archDir'               : '/etc/conary/arch',
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
	    if directive in ('macros'):
		return self.checkFlagKey(directive, arg)
	return ConfigFile.checkKey(self, key, file)
	
    def checkFlagKey(self, directive, key):
	if directive == 'macros':
	    self.macroflags[key] = True
	    return ('macros.' + key, STRING)

    def macroKeys(self):
	return self.macroflags.keys()

    def display(self, out=None):
        if out is None:
            out = sys.stdout
        ConfigFile.display(self, out=out)
        for macro in sorted(self.macroflags.keys()):
            key = 'macros.' + macro
            out.write('%-25s %-25s\n' % (key, self[key]))

    def displayKey(self, key, value, type, out):
        # mask out username and password in repository map entries
        if key == 'repositoryMap':
            maskedMap = {}
            for host, map in value.iteritems():
                maskedMap[host] = re.sub('(https?://)[^:]*:[^@]*@(.*)', 
                                         r'\1<user>:<password>@\2',
                                         map)
            value = maskedMap

        ConfigFile.displayKey(self, key, value, type, out)


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
