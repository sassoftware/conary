#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import deps
import deps.arch
import deps.deps
import os
import versions

STRING, BOOL, LABEL = range(3)

class ConaryConfiguration:
   
    def read(self, file):
	if os.path.exists(file):
	    f = open(file, "r")
	    self.file = file
	    self.lineno = 1
	    for line in f:
		self.configLine(line)
		self.lineno = self.lineno + 1
	    f.close()

    def configLine(self, line):
	line = line.strip()
	if not line or line[0] == '#':
	    return
	(key, val) = line.split(None, 1)
        key = key.lower()
	if not self.lowerCaseMap.has_key(key):
	    raise ParseError, ("%s:%s: configuration value '%s' unknown" % (self.file, self.lineno, key))
	else:
	    key = self.lowerCaseMap[key]
	
	type = self.types[key]

	if type == STRING:
	    self.__dict__[key] = val
	elif type == LABEL:
	    try:
		self.__dict__[key] = versions.BranchName(val)
	    except versions.ParseError, e:
		raise ParseError, str(e)

	elif type == BOOL:
	    if val.lower() in ('0', 'false'):
		self.__dict__[key] = False
	    elif val.lower() in ('1', 'true'):
		self.__dict__[key] = True
	    else:
		raise ParseError, ("%s:%s: expected True or False for configuration value '%s'" % (self.file, self.lineno, key))

    def display(self):
	keys = self.__dict__.keys()
	keys.sort()
	for item in keys:
	    if type(self.__dict__[item]) is str:
		print "%-20s %s" % (item, self.__dict__[item])
	    elif isinstance(self.__dict__[item], versions.Version):
		print "%-20s %s" % (item, self.__dict__[item].asString())
	    elif isinstance(self.__dict__[item], versions.BranchName):
		print "%-20s %s" % (item, self.__dict__[item].asString())
	    elif isinstance(self.__dict__[item], deps.deps.Dependency):
		print "%-20s %s" % (item, self.__dict__[item])
	    elif item == "flavor":
		pass
            elif item == "lowerCaseMap":
                pass
	    else:
		print "%-20s (unknown type)" % (item)

    def __init__(self):
	
	defaults = {
	    'repPath'	     : '/var/lib/conary-rep',
	    'root'	     : '/',
	    'sourcePath'     : '/usr/src/conary/sources',
	    'buildPath'	     : '/usr/src/conary/builds',
	    'installLabel'   : [ LABEL,	 None ],
	    'buildLabel'     : [ LABEL,	 None ],
	    'lookaside'	     : '/var/cache/conary',
	    'dbPath'	     : '/var/lib/conarydb',
	    'tmpDir'	     : '/var/tmp/',
	    'name'	     : None,
	    'contact'	     : None,
	    'instructionSet' : deps.arch.current(),
	    'debugRecipeExceptions' : [ BOOL, False ] }
	
	self.types = {}
	for (key, value) in defaults.items():
	    if isinstance(value, (list, tuple)):
		self.types[key] = value[0]
		self.__dict__[key] = value[1]
	    else:
		self.types[key] = STRING
		self.__dict__[key] = value

        self.lowerCaseMap = {}
        for (key, value) in self.__dict__.items():
            self.lowerCaseMap[key.lower()] = key

	self.flavor = deps.deps.DependencySet()
	self.flavor.addDep(deps.deps.InstructionSetDependency, 
			   self.instructionSet)

	self.read("/etc/conaryrc")
	if os.environ.has_key("HOME"):
	    self.read(os.environ["HOME"] + "/" + ".conaryrc")

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
