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

import deps
import deps.arch
import deps.deps
import os
import versions

STRING, BOOL, LABEL, STRINGDICT, STRINGLIST, CALLBACK = range(6)

class ConfigFile:

    defaults = {}

    def read(self, file, exception=False):
	if os.path.exists(file):
	    f = open(file, "r")
	    self.lineno = 1
	    for line in f:
		self.configLine(line, file)
		self.lineno = self.lineno + 1
	    f.close()
	elif exception:
	    raise IOError, file

    def configLine(self, line, file = "override"):
	line = line.strip()
	if not line or line[0] == '#':
	    return
	(key, val) = line.split(None, 1)
        key = key.lower()
	if not self.lowerCaseMap.has_key(key):
	    raise ParseError, ("%s:%s: configuration value '%s' unknown" % (file, self.lineno, key))
	else:
	    key = self.lowerCaseMap[key]
	
	type = self.types[key]

	if type == STRING:
	    self.__dict__[key] = val
	elif type == STRINGDICT:
	    (idx, val) = val.split(None, 1)
	    self.__dict__[key][idx] = val
	elif type == STRINGLIST:
	    self.__dict__[key].append(val)
	elif type == CALLBACK:
	    self.__dict__[key]('set', key, val)
	elif type == LABEL:
	    try:
		self.__dict__[key] = versions.BranchName(val)
	    except versions.ParseError, e:
		raise versions.ParseError, str(e)
	elif type == BOOL:
	    if val.lower() in ('0', 'false'):
		self.__dict__[key] = False
	    elif val.lower() in ('1', 'true'):
		self.__dict__[key] = True
	    else:
		raise ParseError, ("%s:%s: expected True or False for configuration value '%s'" % (self.file, self.lineno, key))

    def display(self):
	keys = self.defaults.keys()
	keys.sort()
	for item in keys:
	    if (type(self.defaults[item]) == list):
		t = self.defaults[item][0]
	    else:
		t = STRING

	    if t == STRING:
		print "%-25s %s" % (item, self.__dict__[item])
	    elif t == LABEL:
		print "%-25s %s" % (item, self.__dict__[item].asString())
	    elif t == STRINGDICT:
		d = self.__dict__[item]
		idxs = d.keys()
		idxs.sort()
		for idx in idxs:
		    print "%-25s %-25s %s" % (item, idx, d[idx])
	    elif t == CALLBACK:
		self.__dict__[item]('display')
	    elif t == BOOL:
		print "%-25s %s" % (item, bool(self.__dict__[item]))
	    else:
		print "%-25s (unknown type)" % (item)

    def __init__(self):
	self.types = {}
	for (key, value) in self.defaults.items():
	    if isinstance(value, (list, tuple)):
		self.types[key] = value[0]
		self.__dict__[key] = value[1]
	    else:
		self.types[key] = STRING
		self.__dict__[key] = value

        self.lowerCaseMap = {}
        for (key, value) in self.__dict__.items():
            self.lowerCaseMap[key.lower()] = key

class ConaryConfiguration(ConfigFile):

    defaults = {
	'buildLabel'	        : [ LABEL, versions.BranchName('localhost@local:trunk') ],
	'buildPath'		: '/usr/src/conary/builds',
	'contact'		: None,
	'dbPath'		: '/var/lib/conarydb',
	'debugRecipeExceptions' : [ BOOL, False ], 
	'installLabel'		: [ LABEL,	 None ],
	'instructionSet'	: deps.arch.current(),
	'lookaside'		: '/var/cache/conary',
	'name'			: None,
	'repositoryMap'	        : [ STRINGDICT, {} ],
	'root'			: '/',
	'sourceSearchDir'	: '.',
	'tmpDir'		: '/var/tmp/',
    }
   
    def __init__(self):
	ConfigFile.__init__(self)

	self.flavor = deps.deps.DependencySet()
	self.flavor.addDep(deps.deps.InstructionSetDependency, 
			   self.instructionSet)

	self.read("/etc/conaryrc")
	if os.environ.has_key("HOME"):
	    self.read(os.environ["HOME"] + "/" + ".conaryrc")
	self.read("conaryrc")

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
