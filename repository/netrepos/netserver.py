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

import base64
from repository import changeset
from repository import repository
import fsrepos
from lib import log
import files
import os
import re
from lib import sha1helper
import sqlite3
import tempfile
from lib import util
from repository import xmlshims
from repository import repository
from local import idtable
from local import sqldb
from local import versiontable
from netauth import NetworkAuthorization
from netauth import InsufficientPermission

SERVER_VERSIONS=[6,7,8,9,10]
CACHE_SCHEMA_VERSION=10

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    # lets the following exceptions pass:
    #
    # 1. Internal server error (unknown exception)
    # 2. netserver.InsufficientPermission

    def callWrapper(self, methodname, authToken, args):

        def condRollback():
            if self.repos.troveStore.db.inTransaction:
                self.repos.rollback()

	# reopens the sqlite db if it's changed
	self.repos.reopen()

        try:
            # try and get the method to see if it exists
            method = self.__getattribute__(methodname)
        except AttributeError:
            return (True, ("MethodNotSupported", methodname, ""))
        try:
            # the first argument is a version number
	    r = method(authToken, *args)
	    return (False, r)

            # the first argument is a version number
	    r = self.__getattribute__(method)(authToken, *args)
	    return (False, r)
	except repository.TroveMissing, e:
            condRollback()
	    if not e.troveName:
		return (True, ("TroveMissing", "", ""))
	    elif not e.version:
		return (True, ("TroveMissing", e.troveName, ""))
	    else:
		return (True, ("TroveMissing", e.troveName, 
			self.fromVersion(e.version)))
	except repository.CommitError, e:
            condRollback()
	    return (True, ("CommitError", str(e)))
	except ClientTooOld, e:
            condRollback()
	    return (True, ("ClientTooOld", str(e)))
	except repository.DuplicateBranch, e:
            condRollback()
	    return (True, ("DuplicateBranch", str(e)))
	#except Exception:
        #    self.condRollback()
	#    import traceback, sys, string
        #    import lib.epdb
	#    excInfo = sys.exc_info()
	#    lines = traceback.format_exception(*excInfo)
	#    print string.joinfields(lines, "")
	#    if sys.stdout.isatty() and sys.stdin.isatty():
	#	lib.epdb.post_mortem(excInfo[2])
	#    raise

    def allTroveNames(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

	return [ x for x in self.iterAllTroveNames(authToken, clientVersion) ]

    def createBranch(self, authToken, clientVersion, newBranch, kind, 
                     frozenLocation, troveList):
	if not self.auth.check(authToken, write = True):
	    raise InsufficientPermission

	newBranch = self.toLabel(newBranch)

	if kind == 'v':
	    location = self.toVersion(frozenLocation)
	elif kind == 'l':
	    location = self.toLabel(frozenLocation)
	else:
	    return 0

	ret = self.repos.createBranch(newBranch, location, troveList)
        ret = [ (x[0], self.fromVersion(x[1])) for x in ret ]
	return ret

    def updateMetadata(self, authToken, clientVersion,
                       troveName, branch, shortDesc, longDesc,
                       urls, categories, licenses, language):
        branch = self.toBranch(branch)
        retval = self.repos.troveStore.updateMetadata(troveName, branch, shortDesc, longDesc,
                                                      urls, categories, licenses, language)
        self.repos.troveStore.commit()
        return retval

    def getMetadata(self, authToken, clientVersion,
                    troveName, branch, language, version):
        branch = self.toBranch(branch)
        if version:
            version = self.toVersion(version)

        return self.repos.troveStore.getMetadata(troveName, branch, version)
    
    def hasPackage(self, authToken, clientVersion, pkgName):
	if not self.auth.check(authToken, write = False, trove = pkgName):
	    raise InsufficientPermission

	return self.repos.troveStore.hasTrove(pkgName)

    def hasTrove(self, authToken, clientVersion, pkgName, version, flavor):
	if not self.auth.check(authToken, write = False, trove = pkgName):
	    raise InsufficientPermission

	return self.repos.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTroveVersionList(self, authToken, clientVersion, troveNameList):
	d = {}
	for troveName in troveNameList:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	    d[troveName] = [ self.freezeVersion(x) for x in
		    self.repos.troveStore.iterTroveVersions(troveName) ]

	return d

    def getFilesInTrove(self, authToken, clientVersion, troveName, versionStr, 
                        flavor, sortByPath = False, withFiles = False):
        version = self.toVersion(versionStr)
	if not self.auth.check(authToken, write = False, trove = troveName,
			       label = version.branch().label()):
	    raise InsufficientPermission

        gen = self.repos.troveStore.iterFilesInTrove(troveName,
					       version,
                                               self.toFlavor(flavor),
                                               sortByPath, 
                                               withFiles) 
        verDict = {}
        verList = []
        dirDict = {}
        dirList = []
        l = []

        for tup in gen:
            (fileId, filePath, fileVersion) = tup[0:3]
            if withFiles:
                fileStream = tup[3]
                if fileStream is None:
                    fileObj = self.repos.getFileVersion(fileId, fileVersion)
                    fileStream = fileObj.freeze()

            dir = os.path.dirname(filePath)
            fileName = os.path.basename(filePath)

            dirNum = dirDict.get(dir, None)
            if dirNum is None:
                dirNum = len(dirDict)
                dirDict[dir] = dirNum
                dirList.append(dir)

            verNum = verDict.get(fileVersion, None)
            if verNum is None:
                verNum = len(verDict)
                verDict[fileVersion] = verNum
                verList.append(self.fromVersion(fileVersion))

            if clientVersion in [6,7]:
                if withFiles:
                    fileObj = files.ThawFile(fileStream, fileId)
                    l.append((self.fromFileId(fileId), filePath, 
                              self.fromVersion(fileVersion), 
                              self.fromFile(fileObj)))
                else:
                    l.append((self.fromFileId(fileId), filePath, 
                              self.fromVersion(fileVersion)))
            else:
                if withFiles:
                    l.append((base64.encodestring(fileId), dirNum, fileName, 
                              verNum, base64.encodestring(fileStream)))
                else:
                    l.append((base64.encodestring(fileId), dirNum, fileName, 
                              verNum))

        if clientVersion in [6,7]:
            return l

	return l, verList, dirList

    def getFileContents(self, authToken, clientVersion, troveName, 
			troveVersion, troveFlavor, fileId, fileVersion = None):
        if clientVersion <= 8:
            path = fileId
            fileId = None
	elif clientVersion <= 9:
            fileId = self.toFileId(fileId)
        else:
            fileVersion = fileId
            fileId = troveFlavor
            troveFlavor = None
            path = None
            fileId = self.toFileId(fileId)

	troveVersion = self.toVersion(troveVersion)
	fileVersion = self.toVersion(fileVersion)

	if not self.auth.check(authToken, write = False, trove = troveName,
			       label = troveVersion.branch().label()):
	    raise InsufficientPermission

        fileObj = self.repos.findFileVersion(troveName, troveVersion,
                                             fileId, fileVersion)

        filePath = self.repos.contentsStore.hashToPath(
                        sha1helper.sha1ToString(fileObj.contents.sha1()))
        size = os.stat(filePath).st_size

        (fd, path) = tempfile.mkstemp(dir = self.tmpPath, 
                                      suffix = '.cf-out')
        os.write(fd, "%s %d\n" % (filePath, size))
        os.close(fd)

        url = os.path.join(self.urlBase, 
                           "changeset?%s" % os.path.basename(path)[:-4])
        return url

    def getAllTroveLeafs(self, authToken, clientVersion, troveNames):
	for troveName in troveNames:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	d = {}
	for (name, leafList) in \
			self.repos.troveStore.iterAllTroveLeafs(troveNames):
            if name != None:
                d[name] = leafList
	
	return d

    def getTroveLeavesByLabel(self, authToken, clientVersion, troveNameList, 
                              labelStr):
	d = {}
	for troveName in troveNameList:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	rd = {}

	if len(troveNameList) == 1:
	  for troveName in troveNameList:
	    rd[troveName] = [ self.freezeVersion(x) for x in
			self.repos.troveStore.iterTroveLeafsByLabel(troveName,
								   labelStr) ]
	else:
	    d = self.repos.troveStore.iterTroveLeafsByLabelBulk(troveNameList,
								labelStr)
	    for name in troveNameList:
		if d.has_key(name):
		    rd[name] = [ self.freezeVersion(x) for x in d[name] ]
		else:
		    rd[name] = []

	return rd

    def getTroveVersionsByLabel(self, authToken, clientVersion, troveNameList, 
                                labelStr):
	d = {}
	for troveName in troveNameList:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	    d[troveName] = [ self.freezeVersion(x) for x in
		    self.repos.troveStore.iterTroveVersionsByLabel(troveName,
								   labelStr) ]

	return d

    def getTroveVersionFlavors(self, authToken, clientVersion, troveDict):
	inD = {}
	vMap = {}
	for (troveName, versionList) in troveDict.iteritems():
	    inD[troveName] = []
	    for versionStr in versionList:
		v = self.toVersion(versionStr)
		vMap[v] = versionStr
		inD[troveName].append(v)

	outD = self.repos.troveStore.getTroveFlavors(inD)

	retD = {}
	for troveName in outD.iterkeys():
	    retD[troveName] = {}
	    for troveVersion in outD[troveName]:
		verStr = vMap[troveVersion]
		retD[troveName][verStr] = outD[troveName][troveVersion]

	return retD

    def getTroveLatestVersion(self, authToken, clientVersion, pkgName, 
                              branchStr):
	branch = self.toBranch(branchStr)

	if not self.auth.check(authToken, write = False, trove = pkgName,
			       label = branch.label()):
	    raise InsufficientPermission

        try:
            return self.freezeVersion(
			self.repos.troveStore.troveLatestVersion(pkgName, 
						     self.toBranch(branchStr)))
        except KeyError:
            return 0

    def getTroveFlavorsLatestVersion(self, authToken, clientVersion, troveName, 
                                     branchStr):
	branch = self.toBranch(branchStr)

	if not self.auth.check(authToken, write = False, trove = troveName,
			       label = branch.label()):
	    raise InsufficientPermission

	return self.repos.troveStore.iterTrovePerFlavorLeafs(troveName, branchStr)

    def getChangeSet(self, authToken, clientVersion, chgSetList, recurse, 
                     withFiles, withFileContents = None):

        def _cvtTroveList(l):
            new = []
            for (name, (oldV, oldF), (newV, newF), absolute) in l:
                if oldV:
                    oldV = self.fromVersion(oldV)
                    oldF = self.fromFlavor(oldF)
                else:
                    oldV = '0'
                    oldF = '0'

                newV = self.fromVersion(newV)
                newF = self.fromFlavor(newF)

                new.append((name, (oldV, oldF), (newV, newF), absolute))

            return new

        def _cvtFileList(l):
            new = []
            for (fileId, troveName, (oldTroveV, oldTroveF, oldFileV), 
                                    (newTroveV, newTroveF, newFileV)) in l:
                if oldTroveV:
                    oldTroveV = self.fromVersion(oldTroveV)
                    oldFileV = self.fromVersion(oldFileV)
                    oldTroveF = self.fromFlavor(oldTroveF)
                else:
                    oldTroveV = 0
                    oldFileV = 0
                    oldTroveF = 0

                newTroveV = self.fromVersion(newTroveV)
                newFileV = self.fromVersion(newFileV)
                newTroveF = self.fromFlavor(newTroveF)

                fileId = self.fromFileId(fileId)

                new.append((fileId, troveName, 
                               (oldTroveV, oldTroveF, oldFileV),
                               (newTroveV, newTroveF, newFileV)))

            return new

        if clientVersion == 6:
            withFileContents = withFiles
            withFiles = True

        urlList = []
        newChgSetList = []
        allFilesNeeded = []

        # XXX all of these cache lookups should be a single operation through a 
        # temporary table
	for (name, (old, oldFlavor), (new, newFlavor), absolute) in chgSetList:
	    newVer = self.toVersion(new)

	    if not self.auth.check(authToken, write = False, trove = name,
				   label = newVer.branch().label()):
		raise InsufficientPermission

	    if old == 0:
		l = (name, (None, None),
			   (self.toVersion(new), self.toFlavor(newFlavor)),
			   absolute)
	    else:
		l = (name, (self.toVersion(old), self.toFlavor(oldFlavor)),
			   (self.toVersion(new), self.toFlavor(newFlavor)),
			   absolute)

            path = self.cache.getEntry(l, withFiles, withFileContents)
            if path is None:
                (cs, trovesNeeded, filesNeeded) = \
                            self.repos.createChangeSet([ l ], 
                                        recurse = recurse, 
                                        withFiles = withFiles,
                                        withFileContents = withFileContents)
                path = self.cache.addEntry(l, withFiles, withFileContents)

                newChgSetList += _cvtTroveList(trovesNeeded)
                allFilesNeeded += _cvtFileList(filesNeeded)
                cs.writeToFile(path)

            fileName = os.path.basename(path)

            urlList.append(os.path.join(self.urlBase, 
                                        "changeset?%s" % fileName[:-4]))

        if clientVersion < 10:
            return urlList
        else:
            return urlList, newChgSetList, allFilesNeeded

    def iterAllTroveNames(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

	return self.repos.iterAllTroveNames()

    def getDepSuggestions(self, authToken, clientVersion, label, requiresList):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

	requires = {}
	for dep in requiresList:
	    requires[self.toDepSet(dep)] = dep

        label = self.toLabel(label)

	sugDict = self.repos.resolveRequirements(label, requires.keys())

	result = {}
	for (key, val) in sugDict.iteritems():
            result[requires[key]] = val

        return result

    def prepareChangeSet(self, authToken, clientVersion):
	# make sure they have a valid account and permission to commit to
	# *something*
	if not self.auth.check(authToken, write = True):
	    raise InsufficientPermission

	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-in')
	os.close(fd)
	fileName = os.path.basename(path)

        return os.path.join(self.urlBase, "?%s" % fileName[:-3])

    def commitChangeSet(self, authToken, clientVersion, url):
	assert(url.startswith(self.urlBase))
	# +1 strips off the ? from the query url
	fileName = url[len(self.urlBase) + 1:] + "-in"
	path = "%s/%s" % (self.tmpPath, fileName)

	try:
	    cs = changeset.ChangeSetFromFile(path)
	finally:
	    #print path
	    os.unlink(path)

	# walk through all of the branches this change set commits to
	# and make sure the user has enough permissions for the operation
	items = {}
	for pkgCs in cs.iterNewPackageList():
	    items[(pkgCs.getName(), pkgCs.getNewVersion())] = True
	    if not self.auth.check(authToken, write = True, 
			       label = pkgCs.getNewVersion().branch().label(),
			       trove = pkgCs.getName()):
		raise InsufficientPermission

	self.repos.commitChangeSet(cs)

	if not self.commitAction:
	    return True

	for pkgCs in cs.iterNewPackageList():
	    d = { 'reppath' : self.urlBase,
	    	  'trove' : pkgCs.getName(),
		  'version' : pkgCs.getNewVersion().asString() }
	    cmd = self.commitAction % d
	    os.system(cmd)

	return True

    def getFileVersions(self, authToken, clientVersion, fileList):
	# XXX needs to authentication against the trove the file is part of,
	# which is unfortunate, though you have to wonder what could be so
        # special in an inode...
        r = []
        for (fileId, version) in fileList:
            f = self.repos.troveStore.getFile(self.toFileId(fileId), 
                                              self.toVersion(version))
            r.append(self.fromFile(f))

        return r

    def getFileVersion(self, authToken, clientVersion, fileId, version, 
                       withContents = 0):
	# XXX needs to authentication against the trove the file is part of,
	# which is unfortunate, though you have to wonder what could be so
        # special in an inode...
	f = self.repos.troveStore.getFile(self.toFileId(fileId), 
					  self.toVersion(version))
	return self.fromFile(f)

    def checkVersion(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

        if clientVersion not in SERVER_VERSIONS:
            raise ClientTooOld

        return SERVER_VERSIONS[-1]

    def cacheChangeSets(self):
        return isinstance(self.cache, CacheSet)

    def __init__(self, path, tmpPath, urlBase, authDbPath, name,
		 repositoryMap, commitAction = None, cacheChangeSets = False):
	self.repos = fsrepos.FilesystemRepository(name, path, repositoryMap)
	self.map = repositoryMap
	self.repPath = path
	self.tmpPath = tmpPath
	self.urlBase = urlBase
	self.name = name
	self.auth = NetworkAuthorization(authDbPath, name, 
                                         anonymousReads = True)
	self.commitAction = commitAction

        if cacheChangeSets:
            self.cache = CacheSet(path + "/cache.sql", tmpPath, 
                                  CACHE_SCHEMA_VERSION)
        else:
            self.cache = NullCacheSet(tmpPath)

class NullCacheSet:
    def getEntry(self, item, withFiles, withFileContents):
        return None 

    def addEntry(self, item, withFiles, withFileContents):
        (fd, path) = tempfile.mkstemp(dir = self.tmpPath, 
                                      suffix = '.ccs-out')
        os.close(fd)
        return path

    def __init__(self, tmpPath):
        self.tmpPath = tmpPath

class CacheSet:

    filePattern = "%s/cache-%s.ccs-out"

    def getEntry(self, item, withFiles, withFileContents):
        (name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute) = \
            item

        oldVersionId = 0
        oldFlavorId = 0
        newFlavorId = 0

        if oldVersion:
            oldVersionId = self.versions.get(oldVersion, None)
            if oldVersionId is None:
                return None

        if oldFlavor:
            oldFlavorId = self.flavors.get(oldFlavor, None)
            if oldFlavorId is None: 
                return None

        if newFlavor:
            newFlavorId = self.flavors.get(newFlavor, None)
            if newFlavorId is None: 
                return None
        
        newVersionId = self.versions.get(newVersion, None)
        if newVersionId is None:
            return None

        cu = self.db.cursor()
        cu.execute("""
            SELECT row FROM CacheContents WHERE
                troveName=? AND
                oldFlavorId=? AND oldVersionId=? AND
                newFlavorId=? AND newVersionId=? AND
                absolute=? AND withFiles=? AND withFileContents=?
            """, name, oldFlavorId, oldVersionId, newFlavorId, 
            newVersionId, absolute, withFiles, withFileContents)

        row = None
        for (row,) in cu:
            path = self.filePattern % (self.tmpDir, row)
            try:
                fd = os.open(path, os.O_RDONLY)
                os.close(fd)
                return path
            except OSError:
                cu.execute("DELETE FROM CacheContents WHERE row=?", row)
                self.db.commit()

        return None

    def addEntry(self, item, withFiles, withFileContents):
        (name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute) = \
            item

        oldVersionId = 0
        oldFlavorId = 0
        newFlavorId = 0

        if oldVersion:
            oldVersionId = self.versions.get(oldVersion, None)
            if oldVersionId is None:
                oldVersionId = self.versions.addId(oldVersion)

        if oldFlavor:
            oldFlavorId = self.flavors.get(oldFlavor, None)
            if oldFlavorId is None: 
                oldFlavorId = self.flavors.addId(oldFlavor)

        if newFlavor:
            newFlavorId = self.flavors.get(newFlavor, None)
            if newFlavorId is None: 
                newFlavorId = self.flavors.addId(newFlavor)

        newVersionId = self.versions.get(newVersion, None)
        if newVersionId is None:
            newVersionId = self.versions.addId(newVersion)

        cu = self.db.cursor()
        cu.execute("""
            INSERT INTO CacheContents VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?)
        """, name, oldFlavorId, oldVersionId, newFlavorId, newVersionId, 
        absolute, withFiles, withFileContents)

        row = cu.lastrowid
        path = self.filePattern % (self.tmpDir, row)

        self.db.commit()

        return path
        
    def createSchema(self, dbpath, schemaVersion):
	self.db = sqlite3.connect(dbpath, timeout = 30000)
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "CacheContents" in tables:
            cu.execute("SELECT version FROM CacheVersion")
            version = cu.next()[0]
            if version != schemaVersion:
                cu.execute("SELECT row from CacheContents")
                for (row,) in cu:
                    fn = self.tmpDir + "/cache-%s.ccs-out"
                    if os.path.exists(fn):
                        os.unlink(fn)

                self.db.close()
                os.unlink(dbpath)
                self.db = sqlite3.connect(dbpath, timeout = 30000)
                tables = []

        if "CacheContents" not in tables:
            cu.execute("""
                CREATE TABLE CacheContents(
                    row INTEGER PRIMARY KEY,
                    troveName STRING,
                    oldFlavorId INTEGER,
                    oldVersionId INTEGER,
                    newFlavorId INTEGER,
                    newVersionId INTEGER,
                    absolute BOOLEAN,
                    withFiles BOOLEAN,
                    withFileContents BOOLEAN)
            """)
            cu.execute("""
                CREATE INDEX CacheContentsIdx ON 
                        CacheContents(troveName, oldFlavorId, oldVersionId, 
                                      newFlavorId, newVersionId)
            """)

            cu.execute("CREATE TABLE CacheVersion(version INTEGER)")
            cu.execute("INSERT INTO CacheVersion VALUES(?)", schemaVersion)
            self.db.commit()

    def __init__(self, dbpath, tmpDir, schemaVersion):
	self.tmpDir = tmpDir
        self.createSchema(dbpath, schemaVersion)
        self.db._begin()
        self.flavors = sqldb.DBFlavors(self.db)
        self.versions = versiontable.VersionTable(self.db)
        self.db.commit()

class ClientTooOld(Exception):

    def __str__(self):
	return "download a new client from www.specifixinc.com"

    pass
