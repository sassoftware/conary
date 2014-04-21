#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os.path

#conary
from conary.build import use
from conary.deps import deps
from conary.lib import cfg

class CfgFlagSense(cfg.CfgType):
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
            raise cfg.ParseError, ("unknown use value '%s'") % val
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

class SubArchConfig(cfg.ConfigFile):
    name             = None
    buildName        = None
    subsumes         = ''
    buildRequired    = cfg.CfgBool
    shortDoc         = cfg.CfgString
    longDoc          = cfg.CfgString
    macro            = cfg.CfgDict(cfg.CfgString)

class ArchConfig(cfg.ConfigFile):

    _requiredArchProps = ['bits32', 'bits64', 'LE', 'BE']

    name             = cfg.CfgString
    buildName        = cfg.CfgString
    shortDoc         = cfg.CfgString
    longDoc          = cfg.CfgString
    archProp         = cfg.CfgDict(cfg.CfgBool)
    macro            = cfg.CfgDict(cfg.CfgString)

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

            cfg.ConfigFile.configLine(self, line, file, lineno)


    def setSection(self, sectionName):
        if sectionName not in self._sections:
            self._sections[sectionName] = SubArchConfig()
        self._section = sectionName

    def __init__(self, name):
        cfg.ConfigFile.__init__(self)
        self._section = ''
        self._sections = {}
        self.name = name

    def read(self, path):
        cfg.ConfigFile.read(self, path)
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
                          macros=self.macro, platform=True)
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



class UseFlagConfig(cfg.ConfigFile):

    name             = cfg.CfgString
    buildName        = cfg.CfgString
    buildRequired    = (cfg.CfgBool, True)
    sense            = (CfgFlagSense, deps.FLAG_SENSE_PREFERRED)
    shortDoc         = cfg.CfgString
    longDoc          = cfg.CfgString
    platform         = (cfg.CfgBool, False)

    def __init__(self, name):
        cfg.ConfigFile.__init__(self)
        self.name = name
        self.path = None

    def read(self, path):
        # Hack to allow old-style config files to be parsed
        self.path = path
        contents = open(path).read().strip()
        if contents.strip() in ('disallowed', 'preferred', 'prefernot',
                                                           'required'):
            self.configLine('sense %s' % contents, path, 1)
        else:
            cfg.ConfigFile.read(self, path)

    def toDepFlag(self):
        return (self.name, self.sense)

    def addUseFlag(self):
        if '.' in self.name:
            packageName, flagName = self.name.split('.', 1)
            use.PackageFlags
            flagLoc = use.PackageFlags[packageName]
        else:
            flagName = self.name
            flagLoc = use.Use
        flagLoc._addFlag(flagName, required=self.buildRequired,
                         path=self.path, platform=self.platform)
        if self.buildName and self.buildName != self.name:
            flagLoc._addAlias(self.name, self.buildName)

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
                flagPath = os.path.join(useDir, flag)
                if (os.path.isfile(flagPath)
                    and not flag.startswith('.')):
                    if flag not in self.flags:
                        self.flags[flag] = UseFlagConfig(flag)
                    self.flags[flag].read(flagPath)
        for archDir in archDirs:
            archDir = os.path.expanduser(archDir)
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
        if flags:
            useFlags.addDep(deps.UseDependency, deps.Dependency("use", flags))
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
