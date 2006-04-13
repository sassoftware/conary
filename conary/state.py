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
import os

from conary import errors, trove
from conary.deps import deps
from conary.lib import sha1helper
from conary import versions

class ConaryState:

    def __init__(self, context=None, source=None):
        self.context = context
        self.source = source

    def write(self, filename):
	f = open(filename, "w")
        self._write(f)
        if self.hasSourceState():
            self.source._write(f)
            
    def _write(self, f):
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

    __slots__ = [ "branch", "pathMap", "lastMerged" ]

    def setPathMap(self, map):
        self.pathMap = map

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

	<file count>
	PATHID1 PATH1 FILEID1 VERSION1
	PATHID2 PATH2 FILEID2 VERSION2
	.
	.
	.
	PATHIDn PATHn FILEIDn VERSIONn
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

        rc += [ "%s %s %s %s\n" % (sha1helper.md5ToString(x[0]), x[1][0], 
                                sha1helper.sha1ToString(x[1][1]),
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
        if self.lastMerged:
            new.lastMerged = self.lastMerged.copy()
        else:
            new.lastMerged = None
        return new

    def __init__(self, name, version, branch, changeLog = None, 
                 lastmerged = None, isRedirect = False):
        assert(not isRedirect)
        assert(not changeLog)

	trove.Trove.__init__(self, name, version, 
                             deps.DependencySet(), None)
        self.branch = branch
        self.pathMap = {}
        self.lastMerged = lastmerged

class ConaryStateFromFile(ConaryState):

    def parseFile(self, filename):
	f = open(filename)
        lines = f.readlines()

	fields = lines[0][:-1].split()
        if fields[0] == 'context':
            self.context = fields[1]
            lines = lines[1:]
        else:
            self.context = None

        if lines:
            self.source = SourceStateFromLines(lines)
        else:
            self.source = None

    def __init__(self, file):
	if not os.path.isfile(file):
	    raise CONARYFileMissing

	self.parseFile(file)
        

class SourceStateFromLines(SourceState):

    # name : (isVersion, required)
    fields = { 'name'       : (False, True ),
               'version'    : (True,  True ),
               'branch'     : (True,  True ),
               'lastmerged' : (True,  False) }

    def readFileList(self, lines):
	fileCount = int(lines[0][:-1])

        for line in lines[1:]:
            # chop
            line = line[:-1]
	    fields = line.split()
	    pathId = sha1helper.md5FromString(fields.pop(0))
	    version = fields.pop(-1)
	    fileId = sha1helper.sha1FromString(fields.pop(-1))
	    path = " ".join(fields)

	    version = versions.VersionFromString(version)
	    self.addFile(pathId, path, version, fileId)

    def parseLines(self, lines):
        kwargs = {}

        while lines:
	    fields = lines[0][:-1].split()

            # the file count ends the list of fields
            if len(fields) == 1: break
	    assert(len(fields) == 2)
            del lines[0]

	    what = fields[0]
            assert(not kwargs.has_key(what))
            isVer = self.fields[what][0]

	    if isVer:
                kwargs[what] = versions.ThawVersion(fields[1])
	    else:
                kwargs[what] = fields[1]

        required = set([ x[0] for x in self.fields.items() if x[1][1] ])
        assert((set(kwargs.keys()) & required) == required)

	SourceState.__init__(self, **kwargs)

	self.readFileList(lines)

    def __init__(self, lines):
        self.parseLines(lines)

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
