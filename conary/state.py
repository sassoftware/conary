#
# Copyright (c) 2004-2005 rPath, Inc.
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
"""
CONARY state files - stores directory-specific context and source trove info
for a particular directory
"""
import copy
import itertools
import os

from conary import errors, trove
from conary.deps import deps
from conary.lib import sha1helper
from conary import versions

class FileInfo(object):

    __slots__ = ( 'isConfig', 'refresh' )

    # container for the extra information we keep on files for SourceStates
    # this has no access methods; it is meant to be accessed directly

    def __init__(self, isConfig = 0, refresh = 0):
        self.isConfig = isConfig
        self.refresh = refresh

class ConaryState:

    stateVersion = 1

    def __init__(self, context=None, source=None):
        self.context = context
        self.source = source

    def write(self, filename):
	f = open(filename, "w")
        self._write(f)
        if self.hasSourceState():
            self.source._write(f)
            
    def _write(self, f):
        f.write("stateversion %d\n" % self.stateVersion)
        if self.getContext():
            f.write("context %s\n" % self.getContext())

    def hasContext(self):
        return bool(self.context )

    def getContext(self):
        return self.context 

    def setContext(self, name):
        self.context = name

    def getSourceState(self):
        if not self.source:
            raise ConaryStateError, 'No source state defined in CONARY'
        return self.source

    def setSourceState(self, sourceState):
        self.source = sourceState

    def hasSourceState(self):
        return bool(self.source)

    def copy(self):
        if self.hasSourceState():
            sourceState = self.getSourceState().copy()
        else:
            sourceState = None
        return ConaryState(self.context, sourceState)
        
class SourceState(trove.Trove):

    __slots__ = [ "branch", "pathMap", "lastMerged", "fileInfo" ]

    def setPathMap(self, map):
        self.pathMap = map

    def removeFile(self, pathId):
        trove.Trove.removeFile(self, pathId)
        del self.fileInfo[pathId]

    def addFile(self, pathId, path, version, fileId, isConfig):
        trove.Trove.addFile(self, pathId, path, version, fileId)
        self.fileInfo[pathId] = FileInfo(isConfig = isConfig)

    def removeFilePath(self, file):
	for (pathId, path, fileId, version) in self.iterFileList():
	    if path == file: 
		self.removeFile(pathId)
		return True

	return False

    def _write(self, f):
        """
	Returns a string representing file information for this trove
	trove, which can later be read by the read() method. This is
	only used to create the Conary control file when dealing with
	:source component checkins, so things like trove dependency
	information is not needed.  The format of the string is:

        name <name>
        version <version>
        branch <branch>
        (lastmerged <version>)?
	<file count>
	PATHID1 PATH1 FILEID1 ISCONFIG1 REFRESH1 VERSION1
	PATHID2 PATH2 FILEID2 ISCONFIG2 REFRESH2 VERSION2
	.
	.
	.
	PATHIDn PATHn FILEIDn ISCONFIGn REFRESHn VERSIONn
	"""
        assert(len(self.strongTroves) == 0)
        assert(len(self.weakTroves) == 0)

        f.write("name %s\n" % self.getName())
        f.write("version %s\n" % self.getVersion().freeze())
        f.write("branch %s\n" % self.getBranch().freeze())
        if self.getLastMerged() is not None:
            f.write("lastmerged %s\n" % self.getLastMerged().freeze())

        rc = []
        rc.append("%d\n" % (len(self.idMap)))

        rc += [ "%s %s %s %d %d %s\n" % (sha1helper.md5ToString(x[0]), x[1][0],
                                sha1helper.sha1ToString(x[1][1]),
                                self.fileInfo[x[0]].isConfig,
                                self.fileInfo[x[0]].refresh,
                                x[1][2].asString())
                for x in self.idMap.iteritems() ]

	f.write("".join(rc))


    def changeBranch(self, branch):
	self.branch = branch

    def getBranch(self):
        return self.branch

    def setLastMerged(self, ver = None):
        self.lastMerged = ver

    def getLastMerged(self):
        return self.lastMerged

    def getRecipeFileName(self):
        # XXX this is not the correct way to solve this problem
        # assumes a fully qualified trove name
        name = self.getName().split(':')[0]
        return os.path.join(os.getcwd(), name + '.recipe')

    def expandVersionStr(self, versionStr):
	if versionStr[0] == "@":
	    # get the name of the repository from the current branch
	    repName = self.getVersion().getHost()
	    return repName + versionStr
	elif versionStr[0] != "/" and versionStr.find("@") == -1:
	    # non fully-qualified version; make it relative to the current
	    # branch
	    return self.getVersion().branch().asString() + "/" + versionStr

	return versionStr

    def copy(self, classOverride = None):
        new = trove.Trove.copy(self, classOverride = classOverride)
        new.branch = self.branch.copy()
        new.pathMap = copy.copy(self.pathMap)
        new.fileInfo = copy.copy(self.fileInfo)
        if self.lastMerged:
            new.lastMerged = self.lastMerged.copy()
        else:
            new.lastMerged = None
        return new

    def fileIsConfig(self, pathId, set = None):
        if set is None:
            return self.fileInfo[pathId].isConfig
        self.fileInfo[pathId].isConfig = set

    def fileNeedsRefresh(self, pathId, set = None):
        if set is None:
            return self.fileInfo[pathId].refresh
        self.fileInfo[pathId].refresh = set

    def __init__(self, name, version, branch, changeLog = None,
                 lastmerged = None, isRedirect = False, **kw):
        assert(not isRedirect)
        assert(not changeLog)

	trove.Trove.__init__(self, name, version, deps.Flavor(),
                             None, **kw)
        self.branch = branch
        self.pathMap = {}
        self.lastMerged = lastmerged
        self.fileInfo = {}

class ConaryStateFromFile(ConaryState):

    def parseFile(self, filename, repos):
	f = open(filename)
        lines = f.readlines()

        stateVersion = 0
        if lines[0].startswith('stateversion '):
            stateVersion = int(lines[0].split(None, 1)[1].strip())
            lines.pop(0)

        contextList = [ x for x in lines if x.startswith('context ') ]
        if contextList:
            contextLine = contextList[-1]
            self.context = contextLine.split(None, 1)[1].strip()
            lines = [ x for x in lines if not x.startswith('context ')]
        else:
            self.context = None

        if lines:
            try:
                self.source = SourceStateFromLines(lines, stateVersion, repos)
            except ConaryStateError, err:
                raise ConaryStateError('Cannot parse state file %s: %s' % (filename, err))
        else:
            self.source = None

    def __init__(self, file, repos):
	if not os.path.isfile(file):
	    raise CONARYFileMissing

	self.parseFile(file, repos)
        

class SourceStateFromLines(SourceState):

    # name : (isVersion, required)
    fields = { 'name'       : (False, True ),
               'version'    : (True,  True ),
               'branch'     : (True,  True ),
               'lastmerged' : (True,  False) }

    def _readFileList(self, lines, stateVersion, repos):
	fileCount = int(lines[0][:-1])
        configFlagNeeded = []

        for line in lines[1:]:
            # chop
            line = line[:-1]
	    fields = line.split()
	    pathId = sha1helper.md5FromString(fields.pop(0))
            version = versions.VersionFromString(fields.pop(-1))

            if stateVersion >= 1:
                refresh = int(fields.pop(-1))
                isConfig = int(fields.pop(-1))
            else:
                isConfig = 0
                refresh = 0

	    fileId = sha1helper.sha1FromString(fields.pop(-1))

            if stateVersion == 0:
                if not isinstance(version, versions.NewVersion):
                    configFlagNeeded.append((pathId, fileId, version))

	    path = " ".join(fields)

	    self.addFile(pathId, path, version, fileId, isConfig = isConfig)
            self.fileNeedsRefresh(pathId, set = refresh)

        if configFlagNeeded:
            assert(stateVersion == 0)
            fileObjs = repos.getFileVersions(configFlagNeeded)
            for (pathId, fileId, version), fileObj in \
                            itertools.izip(configFlagNeeded, fileObjs):
                self.fileIsConfig(pathId, set = fileObj.flags.isConfig())

    def parseLines(self, lines, stateVersion, repos):
        kwargs = {}

        while lines:
	    fields = lines[0][:-1].split()

            # the file count ends the list of fields
            if len(fields) == 1: break
	    assert(len(fields) == 2)
            del lines[0]

	    what = fields[0]
            assert(not kwargs.has_key(what))
            if what not in self.fields:
                raise ConaryStateError('Invalid field "%s"' % what)
                
            isVer = self.fields[what][0]

	    if isVer:
                kwargs[what] = versions.ThawVersion(fields[1])
	    else:
                kwargs[what] = fields[1]

        required = set([ x[0] for x in self.fields.items() if x[1][1] ])
        assert((set(kwargs.keys()) & required) == required)

	SourceState.__init__(self, **kwargs)

	self._readFileList(lines, stateVersion, repos)

    def __init__(self, lines, stateVersion, repos):
        self.parseLines(lines, stateVersion, repos )

    def copy(self):
        return SourceState.copy(self, classOverride = SourceState)

class ConaryStateError(errors.ConaryError):
    pass

class CONARYFileMissing(ConaryStateError):
    """
    This exception is raised when the CONARY file specified does not
    exist
    """
    def __str__(self):
        return 'CONARY state file does not exist.'
