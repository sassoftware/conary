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

from repository import changeset
from repository import repository
from localrep import fsrepos
from lib import log
import md5
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

SERVER_VERSION=5

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    # lets the following exceptions pass:
    #
    # 1. Internal server error (unknown exception)
    # 2. netserver.InsufficientPermission

    def callWrapper(self, methodname, authToken, args):
	# reopens the sqlite db if it's changed
	self.repos.reopen()

        try:
            # try and get the method to see if it exists
            method = self.__getattribute__(methodname)
        except:
            return (True, ("MethodNotSupported", methodname, ""))
        try:
            # the first argument is a version number
	    r = method(authToken, *args)
	    return (False, r)

            # the first argument is a version number
	    r = self.__getattribute__(method)(authToken, *args)
	    return (False, r)
	except repository.TroveMissing, e:
	    if not e.troveName:
		return (True, ("TroveMissing", "", ""))
	    elif not e.version:
		return (True, ("TroveMissing", e.troveName, ""))
	    else:
		return (True, ("TroveMissing", e.troveName, 
			self.fromVersion(e.version)))
	except repository.CommitError, e:
	    return (True, ("CommitError", str(e)))
	except ClientTooOld, e:
	    return (True, ("ClientTooOld", str(e)))
	except repository.DuplicateBranch, e:
	    return (True, ("DuplicateBranch", str(e)))
	except Exception:
	    import traceback, pdb,sys, string
	    excInfo = sys.exc_info()
	    lines = traceback.format_exception(*excInfo)
	    print string.joinfields(lines, "")
	    if sys.stdout.isatty() and sys.stdin.isatty():
		pdb.post_mortem(excInfo[2])
	    raise

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

	return self.repos.createBranch(newBranch, location, troveList)

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
        if withFiles:
	    l = []
	    for (fileId, filePath, fileVersion, fileObj) in gen:
		if fileObj is None:
		    fileObj = self.repos.getFileVersion(fileId, fileVersion)

		l.append((self.fromFileId(fileId), filePath, 
			  self.fromVersion(fileVersion), 
			  self.fromFile(fileObj)))
        else:
            l = [ (self.fromFileId(x[0]), x[1], self.fromVersion(x[2])) 
			    for x in gen ]

	return l

    def getFileContents(self, authToken, clientVersion, troveName, 
                            troveVersion, troveFlavor, path, fileVersion):
	troveVersion = self.toVersion(troveVersion)
	fileVersion = self.toVersion(fileVersion)
	troveFlavor = self.toFlavor(troveFlavor)

	if not self.auth.check(authToken, write = False, trove = troveName,
			       label = troveVersion.branch().label()):
	    raise InsufficientPermission

	# this could be much more efficient; iterating over the files is
	# just silly
	for (fileId, tpath, tversion, fileObj) in \
		self.repos.iterFilesInTrove(troveName, troveVersion, 
					    troveFlavor, withFiles = True):
	    if tpath != path or tversion != fileVersion: continue

	    inF = self.repos.contentsStore.openRawFile(
			    sha1helper.sha1ToString(fileObj.contents.sha1()))

	    (fd, path) = tempfile.mkstemp(dir = self.tmpPath, 
					  suffix = '.cf-out')
	    outF = os.fdopen(fd, "w")
	    util.copyfileobj(inF, outF)

	    url = "%s?%s" % ( self.urlBase, os.path.basename(path)[:-4] )
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
                     withFiles):
        urlList = []

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

            path = self.cache.getEntry(l, withFiles)
            if path is None:
                cs = self.repos.createChangeSet([ l ], recurse = recurse, 
                                                withFiles = withFiles)
                path = self.cache.addEntry(l, withFiles)
                cs.writeToFile(path)

            fileName = os.path.basename(path)

            urlList.append("%s?%s" % (self.urlBase, fileName[:-4]))

        return urlList

    def iterAllTroveNames(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

	return self.repos.iterAllTroveNames()

    def prepareChangeSet(self, authToken, clientVersion):
	# make sure they have a valid account and permission to commit to
	# *something*
	if not self.auth.check(authToken, write = True):
	    raise InsufficientPermission

	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-in')
	os.close(fd)
	fileName = os.path.basename(path)
	return "%s?%s" % (self.urlBase, fileName[:-3])

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

    def getFileVersion(self, authToken, clientVersion, fileId, version, 
                       withContents = 0):
	# XXX needs to authentication against the trove the file is part of,
	# which is unfortunate
	f = self.repos.troveStore.getFile(self.toFileId(fileId), 
					  self.toVersion(version))
	return self.fromFile(f)

    def checkVersion(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

        if clientVersion != SERVER_VERSION:
            raise ClientTooOld

        return SERVER_VERSION

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
            self.cache = CacheSet(path + "/cache.sql", tmpPath, SERVER_VERSION)
        else:
            self.cache = NullCacheSet(tmpPath)

class NullCacheSet:
    def getEntry(self, item, withFiles):
        return None 

    def addEntry(self, item, withFiles):
        (fd, path) = tempfile.mkstemp(dir = self.tmpPath, 
                                      suffix = '.ccs-out')
        os.close(fd)
        return path

    def __init__(self, tmpPath):
        self.tmpPath = tmpPath

class CacheSet:

    filePattern = "%s/cache-%s.ccs-out"

    def getEntry(self, item, withFiles):
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
                absolute=? AND withFiles=?
            """, name, oldFlavorId, oldVersionId, newFlavorId, 
            newVersionId, absolute, withFiles)

        row = None
        for (row,) in cu:
            path = self.filePattern % (self.tmpDir, row)
            try:
                fd = os.open(path, os.O_RDONLY)
                os.close(fd)
                return path
            except OSError:
                cu.execute("DELETE FROM CacheContents WHERE row=?", row)
                db.commit()

        return None

    def addEntry(self, item, withFiles):
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
            INSERT INTO CacheContents VALUES(NULL, ?, ?, ?, ?, ?, ?, ?)
        """, name, oldFlavorId, oldVersionId, newFlavorId, newVersionId, 
        absolute, withFiles)

        row = cu.lastrowid
        path = self.filePattern % (self.tmpDir, row)

        self.db.commit()

        return path
        
    def createSchema(self, dbpath, protocolVersion):
	self.db = sqlite3.connect(dbpath)
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "CacheContents" in tables:
            cu.execute("SELECT version FROM CacheVersion")
            version = cu.next()[0]
            if version != protocolVersion:
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
                    withFiles BOOLEAN)
            """)
            cu.execute("""
                CREATE INDEX CacheContentsIdx ON 
                        CacheContents(troveName, oldFlavorId, oldVersionId, 
                                      newFlavorId, newVersionId)
            """)

            cu.execute("CREATE TABLE CacheVersion(version INTEGER)")
            cu.execute("INSERT INTO CacheVersion VALUES(?)", protocolVersion)
            self.db.commit()

    def __init__(self, dbpath, tmpDir, protocolVersion):
	self.tmpDir = tmpDir
        self.createSchema(dbpath, protocolVersion)
        self.db._begin()
        self.flavors = sqldb.DBFlavors(self.db)
        self.versions = versiontable.VersionTable(self.db)
        self.db.commit()

class NetworkAuthorization:

    def check(self, authToken, write = False, label = None, trove = None):
	if label and label.getHost() != self.name:
	    log.error("repository name mismatch")
	    return False

	if not write and self.anonReads:
	    return True

	if not authToken[0]:
	    log.error("no authtoken received")
	    return False

	stmt = """
	    SELECT troveName FROM
	       (SELECT userId as uuserId FROM Users WHERE user=? AND 
		    password=?) 
	    JOIN Permissions ON uuserId=Permissions.userId
	    LEFT OUTER JOIN TroveNames ON Permissions.troveNameId = TroveNames.troveNameId
	""" 
	m = md5.new()
	m.update(authToken[1])
	params = [authToken[0], m.hexdigest()]

	where = []
	if label:
	    where.append(" labelId=(SELECT labelId FROM Labels WHERE " \
			    "label=?) OR labelId is Null")
	    params.append(label.asString())

	if write:
	    where.append("write=1")

	if where:
	    stmt += "WHERE " + " AND ".join(where)

	cu = self.db.cursor()
	cu.execute(stmt, params)

	for (troveName, ) in cu:
	    if not troveName or not trove:
		return True

	    regExp = self.reCache.get(troveName, None)
	    if regExp is None:
		regExp = re.compile(troveName)
		self.reCache[troveName] = regExp

	    if regExp.match(trove):
		return True

	log.error("no permissions match for (%s, %s)" % authToken)

	return False

    def __init__(self, dbpath, name, anonymousReads = False):
	self.name = name
	self.db = sqlite3.connect(dbpath)
	self.anonReads = anonymousReads
	self.reCache = {}

class InsufficientPermission(Exception):

    pass

class ClientTooOld(Exception):

    def __str__(self):
	return "download a new client from www.specifixinc.com"

    pass
