# -*- mode: python -*-
#
# Copyright (c) 2005 Specifix, Inc.
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

import os.path

#conary
from build import use
from conarycfg import ConfigFile, STRING, STRINGDICT, BOOL, ParseError
import deps.deps

# XXX hack -- need a better way to add to list of config types
FLAGSENSE = 2222
BOOLDICT = 2223

class SubArchConfig(ConfigFile):
    defaults = {
	'name'	                : [ STRING, None ],
	'buildName'	        : [ STRING, None ],
        'subsumes'              : [ STRING, ''   ],
	'buildRequired'	        : [ BOOL,   True ],
	'shortDoc'	        : [ STRING, '' ],
	'longDoc'	        : [ STRING, '' ],
	'march'	                : [ STRING, None ],
    } 


class ArchConfig(ConfigFile):

    requiredArchProps = ['bits32', 'bits64', 'LE', 'BE'] 

    defaults = {
	'name'	                : [ STRING, None ],
	'march'	                : [ STRING, None ],
	'buildName'	        : [ STRING, None ],
	'shortDoc'	        : [ STRING, '' ],
	'longDoc'	        : [ STRING, '' ],
        'archProp'              : [ BOOLDICT, {} ],
    } 


    def configLine(self, line, file = "override", lineno = '<No line>'):
	line = line.strip()
        if line and line[0] == '[' and line[-1] == ']':
            self.setSection(line[1:-1])
            return
        if self.section:
            self.sections[self.section].configLine(line, file, lineno)
        else:
            ConfigFile.configLine(self, line, file, lineno)


    def setValue(self, key, val, type=None, file="override"):
	if type == None:
	    type = self.types[key]
        if type == BOOLDICT:
	    (idx, val) = val.split(None, 1)
	    if val.lower() in ('0', 'false'):
		self.__dict__[key][idx] = False
	    elif val.lower() in ('1', 'true'):
		self.__dict__[key][idx] = True
	    else:
		raise ParseError, ("%s:%s: expected True or False for configuration value '%s'" % (file, self.lineno, key))
        else:
            ConfigFile.setValue(self, key, val, type, file)


    def setSection(self, sectionName):
        if sectionName not in self.sections:
            self.sections[sectionName] = SubArchConfig()
        self.section = sectionName

    def __init__(self, name, path):
	ConfigFile.__init__(self)
        self.section = ''
        self.sections = {}
        filePath = os.path.join(path, name) 
        self.read(filePath)
        self.name = name
	if sorted(self.archProp.iterkeys()) != sorted(self.requiredArchProps):
	    raise RuntimeError, \
		    ('Arch %s must specify arch properties %s using the'
		     ' archProp directive' % (self.name,
		     ', '.join(sorted(self.requiredArchProps))))


    def addArchFlags(self):
        if not self.march:
            self.march = self.name
        use.Arch._addFlag(self.name, archProps = self.archProp, 
                                            march=self.march)
                                     
        for subArchName in self.sections:
            subArch = self.sections[subArchName]
            subArch.name = subArchName
            newSubsumes = []
            for item in subArch.subsumes.split(','):
                item = item.strip()
                # skip past empty items, say because of a trailing comma
                if not item:
                    continue
                newSubsumes.append(item)
            subArch.subsumes = newSubsumes

            use.Arch[self.name]._addFlag(subArch.name,
                                         subsumes=subArch.subsumes, 
                                         march=subArch.march)
            if subArch.buildName and subArch.buildName != subArch.name:
                use.Arch[self.name]._addAlias(subArch.name, subArch.buildName)



class UseFlagConfig(ConfigFile):
    defaults = {
	'name'	                : [ STRING, None ],
        'sense'                 : [ FLAGSENSE, deps.deps.FLAG_SENSE_PREFERRED ],
	'buildName'	        : [ STRING, None ],
	'buildRequired'	        : [ BOOL,   True ],
	'shortDoc'	        : [ STRING, '' ],
	'longDoc'	        : [ STRING, '' ],
    }

    def __init__(self, name, path):
	ConfigFile.__init__(self)
        filePath = os.path.join(path, name)
	# Hack to allow old-style config files to be parsed
        contents = open(filePath).read().strip()
        if contents.strip() in ('disallowed', 'preferred', 'prefernot', 
                                                           'required'):
            self.configLine('sense %s' % contents, filePath, 1)
        else:
            self.read(filePath)
        if self.name is None:
            self.name = name
        assert(self.name == name)

    def setValue(self, key, val, type=None, filePath="override"):
	if type == None:
	    type = self.types[key]
        if type == FLAGSENSE:
            val = val.lower()
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
            self.__dict__[key] = sense
        else:
            ConfigFile.setValue(self, key, val, type, filePath)

    def displayKey(self, key, value, type, out):
        if type == FLAGSENSE:
            if value == deps.deps.FLAG_SENSE_DISALLOWED:
                out.write('%s: %s\n' % (key, "disallowed"))
            elif value == deps.deps.FLAG_SENSE_PREFERRED:
                out.write('%s: %s\n' % (key, "preferred"))
            elif value == deps.deps.FLAG_SENSE_PREFERNOT:
                out.write('%s: %s\n' % (key, "prefernot"))
            elif value == deps.deps.FLAG_SENSE_REQUIRED:
                out.write('%s: %s\n' % (key, "required"))
        else:
            ConfigFile.displayKey(self, key, value, type, out)

    def toDepFlag(self):
        return (self.name, self.sense)

    def addUseFlag(self):
        use.Use._addFlag(self.name, required=self.buildRequired) 
        if self.buildName and self.buildName != self.name:
            use.Use._addAlias(self.name, self.buildName)

class FlavorConfig:
    """
    contains information reflecting the use flags specified by the 
    use and arch paths
    """

    def __init__(self, useDir, archDir):
        self.flags = {}
        self.arches = {}
        if useDir and os.path.exists(useDir):
            for flag in os.listdir(useDir):
		if os.path.isfile(os.path.join(useDir, flag)):
		    self.flags[flag] = UseFlagConfig(flag, useDir)
        if archDir and os.path.exists(archDir):
            for arch in os.listdir(archDir):
		if os.path.isfile(os.path.join(archDir, arch)):
		    self.arches[arch] = ArchConfig(arch, archDir)

    def toDependency(self, override=None):
        useFlags = deps.deps.DependencySet()
        flags = [x.toDepFlag() for x in self.flags.values() ] 

        useFlags.addDep(deps.deps.UseDependency, 
                        deps.deps.Dependency("use", flags))
        if override:
            useFlags.union(override,
                           mergeType = deps.deps.DEP_MERGE_TYPE_OVERRIDE)
        return useFlags

    def populateBuildFlags(self):
        for flag in self.flags.itervalues():
            flag.addUseFlag()
        for arch in self.arches.itervalues():
            arch.addArchFlags()

	# These are the required arch properties, every architecture
        # must specify these values
	use.Arch._setArchProps('bits32', 'bits64', 'LE', 'BE')
