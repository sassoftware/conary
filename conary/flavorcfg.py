# -*- mode: python -*-
#
# Copyright (c) 2005 rPath, Inc.
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
from conary.deps import deps
from conary.lib import log

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
	'macro'	                : [ STRINGDICT, {} ],
    } 

class ArchConfig(ConfigFile):

    requiredArchProps = ['bits32', 'bits64', 'LE', 'BE'] 

    defaults = {
	'name'	                : [ STRING, None ],
	'buildName'	        : [ STRING, None ],
	'shortDoc'	        : [ STRING, '' ],
	'longDoc'	        : [ STRING, '' ],
        'archProp'              : [ BOOLDICT, {} ],
	'macro'	                : [ STRINGDICT, {} ],
    } 


    def configLine(self, line, file = "override", lineno = '<No line>'):
	line = line.strip()
        if line and line[0] == '[' and line[-1] == ']':
            self.setSection(line[1:-1])
            return
        if line:
            # handle old targetarch, optflags, and unamearch defs
            parts = line.split(None, 1)
            parts[0] = parts[0].lower()
            if parts[0] in ('targetarch', 'optflags', 'unamearch'):
                line = ' '.join(('macro', parts[0], parts[1]))
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

    def __init__(self, name):
	ConfigFile.__init__(self)
        self.section = ''
        self.sections = {}
        self.name = name

    def read(self, path):
        ConfigFile.read(self, path)
	if sorted(self.archProp.iterkeys()) != sorted(self.requiredArchProps):
	    raise RuntimeError, \
		    ('Arch %s must specify arch properties %s using the'
		     ' archProp directive' % (self.name,
		     ', '.join(sorted(self.requiredArchProps))))

    def addArchFlags(self):
        if 'unamearch' not in self.macro:
            self.macro['unamearch'] = self.name

        if 'targetarch' not in self.macro:
            self.macro['targetarch'] = self.name
        use.Arch._addFlag(self.name, archProps = self.archProp, 
                          macros=self.macro)
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
                                         macros=subArch.macro)
            if subArch.buildName and subArch.buildName != subArch.name:
                use.Arch[self.name]._addAlias(subArch.name, subArch.buildName)



class UseFlagConfig(ConfigFile):
    defaults = {
	'name'	                : [ STRING, None ],
        'sense'                 : [ FLAGSENSE, deps.FLAG_SENSE_PREFERRED ],
	'buildName'	        : [ STRING, None ],
	'buildRequired'	        : [ BOOL,   True ],
	'shortDoc'	        : [ STRING, '' ],
	'longDoc'	        : [ STRING, '' ],
    }

    def __init__(self, name):
	ConfigFile.__init__(self)
        self.name = name

    def read(self, path):
        # Hack to allow old-style config files to be parsed
        contents = open(path).read().strip()
        if contents.strip() in ('disallowed', 'preferred', 'prefernot',
                                                           'required'):
            self.configLine('sense %s' % contents, path, 1)
        else:
            ConfigFile.read(self, path)

    def setValue(self, key, val, type=None, filePath="override"):
        if key == 'name':
            assert(val == self.name)
	if type == None:
	    type = self.types[key]
        if type == FLAGSENSE:
            val = val.lower()
            if val == "disallowed":
                sense = deps.FLAG_SENSE_DISALLOWED
            elif val == "preferred":
                sense = deps.FLAG_SENSE_PREFERRED
            elif val == "prefernot":
                sense = deps.FLAG_SENSE_PREFERNOT
            elif val == "required":
                sense = deps.FLAG_SENSE_REQUIRED
            else:
                raise ParseError, ("%s: unknown use value %s") % (filePath, val)
            self.__dict__[key] = sense
        else:
            ConfigFile.setValue(self, key, val, type, filePath)

    def displayKey(self, key, value, type, out):
        if type == FLAGSENSE:
            if value == deps.FLAG_SENSE_DISALLOWED:
                out.write('%s: %s\n' % (key, "disallowed"))
            elif value == deps.FLAG_SENSE_PREFERRED:
                out.write('%s: %s\n' % (key, "preferred"))
            elif value == deps.FLAG_SENSE_PREFERNOT:
                out.write('%s: %s\n' % (key, "prefernot"))
            elif value == deps.FLAG_SENSE_REQUIRED:
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

    def __init__(self, useDirs, archDirs):
        self.flags = {}
        self.arches = {}

        if useDirs and not isinstance(useDirs, (list, tuple)):
            useDirs = [useDirs]
        if archDirs and not isinstance(archDirs, (list, tuple)):
            archDirs = [archDirs]

        for useDir in useDirs:
            useDir = os.path.expanduser(useDir)
            if not useDir or not os.path.exists(useDir):
                continue
            for flag in os.listdir(useDir):
                if (os.path.isfile(os.path.join(useDir, flag)) 
                    and not flag.startswith('.')):
                    if flag not in self.flags:
                        self.flags[flag] = UseFlagConfig(flag)
                    self.flags[flag].read(os.path.join(useDir, flag))
        for archDir in archDirs:
            useDir = os.path.expanduser(useDir)
            if archDir and os.path.exists(archDir):
                for arch in os.listdir(archDir):
                    if (os.path.isfile(os.path.join(archDir, arch)) and 
                       not arch.startswith('.')):
                       if arch not in self.arches:
                            self.arches[arch] = ArchConfig(arch)
                       self.arches[arch].read(os.path.join(archDir, arch))

    def toDependency(self, override=None):
        useFlags = deps.DependencySet()
        flags = [x.toDepFlag() for x in self.flags.values() ] 

        useFlags.addDep(deps.UseDependency, 
                        deps.Dependency("use", flags))
        if override:
            if isinstance(override, list):
                newUseFlags = []
                for flavor in override:
                    useFlagsCopy = useFlags.copy()
                    useFlagsCopy.union(flavor,
                                 mergeType = deps.DEP_MERGE_TYPE_OVERRIDE)
                    newUseFlags.append(useFlagsCopy)
                return newUseFlags
            else:
                useFlags.union(override,
                               mergeType = deps.DEP_MERGE_TYPE_OVERRIDE)
        return useFlags

    def populateBuildFlags(self):
        for flag in self.flags.itervalues():
            flag.addUseFlag()
        for arch in self.arches.itervalues():
            arch.addArchFlags()

	# These are the required arch properties, every architecture
        # must specify these values
	use.Arch._setArchProps('bits32', 'bits64', 'LE', 'BE')
