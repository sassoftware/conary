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
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import os.path

#conary
from conary.build import use
from conary.deps import deps
from conary.lib.cfg import *

class CfgFlagSense(CfgType):
    @staticmethod
    def parseString(val):
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
            raise ParseError, ("unknown use value '%s'") % val
        return sense

    @staticmethod
    def format(val, displayOptions={}):
        if val == deps.FLAG_SENSE_DISALLOWED:
            return "disallowed"
        elif val == deps.FLAG_SENSE_PREFERRED:
            return "preferred"
        elif val == deps.FLAG_SENSE_PREFERNOT:
            return "prefernot"
        elif val == deps.FLAG_SENSE_REQUIRED:
            return "required"
  
class SubArchConfig(ConfigFile):
    name             = None
    buildName        = None
    subsumes         = ''
    buildRequired    = CfgBool
    shortDoc         = CfgString
    longDoc          = CfgString
    macro            = CfgDict(CfgString)
  
class ArchConfig(ConfigFile):
  
    _requiredArchProps = ['bits32', 'bits64', 'LE', 'BE'] 
  
    name             = CfgString
    buildName        = CfgString
    shortDoc         = CfgString
    longDoc          = CfgString
    archProp         = CfgDict(CfgBool)
    macro            = CfgDict(CfgString)
 
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
        if self._section:
            self._sections[self._section].configLine(line, file, lineno)
        else:

            ConfigFile.configLine(self, line, file, lineno)


    def setSection(self, sectionName):
        if sectionName not in self._sections:
            self._sections[sectionName] = SubArchConfig()
        self._section = sectionName

    def __init__(self, name):
	ConfigFile.__init__(self)
        self._section = ''
        self._sections = {}
        self.name = name

    def read(self, path):
        ConfigFile.read(self, path)
	if sorted(self.archProp.iterkeys()) != sorted(self._requiredArchProps):
	    raise RuntimeError, \
		    ('Arch %s must specify arch properties %s using the'
		     ' archProp directive' % (self.name,
		     ', '.join(sorted(self._requiredArchProps))))

    def addArchFlags(self):
        if 'unamearch' not in self.macro:
            self.macro['unamearch'] = self.name

        if 'targetarch' not in self.macro:
            self.macro['targetarch'] = self.name
        use.Arch._addFlag(self.name, archProps = self.archProp, 
                          macros=self.macro)
        for subArchName in self._sections:
            subArch = self._sections[subArchName]
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

    name             = CfgString
    buildName        = CfgString
    buildRequired    = (CfgBool, True)
    sense            = (CfgFlagSense, deps.FLAG_SENSE_PREFERRED)
    shortDoc         = CfgString
    longDoc          = CfgString

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
        useFlags = deps.Flavor()
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
