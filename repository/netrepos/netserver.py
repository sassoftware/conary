from repository import changeset
from localrep import fsrepos
import filecontainer
import md5
import os
import sqlite
import tempfile
import xmlshims

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    def allTroveNames(self, authToken):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

	return [ x for x in self.iterAllTroveNames(authToken) ]

    def createBranch(self, authToken, newBranch, kind, frozenLocation, 
		     troveList):
	if not self.auth.check(authToken, write = True):
	    raise InsufficientPermission

	newBranch = self.toLabel(newBranch)
	if kind == 'v':
	    location = self.toVersion(frozenLocation)
	elif kind == 'l':
	    location = self.toLabel(frozenLocation)
	else:
	    return 0

	self.repos.createBranch(newBranch, location, troveList)
	return 1

    def hasPackage(self, authToken, pkgName):
	if not self.auth.check(authToken, write = False, trove = pkgName):
	    raise InsufficientPermission

	return self.repos.troveStore.hasTrove(pkgName)

    def hasTrove(self, authToken, pkgName, version, flavor):
	if not self.auth.check(authToken, write = False, trove = pkgName):
	    raise InsufficientPermission

	return self.repos.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTroveVersionList(self, authToken, troveNameList):
	d = {}
	for troveName in troveNameList:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	    d[troveName] = [ x for x in
			    self.repos.troveStore.iterTroveVersions(troveName) ]

	return d

    def getFilesInTrove(self, authToken, troveName, versionStr, flavor,
                        sortByPath = False, withFiles = False):
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
            return [ (x[0], x[1], self.fromVersion(x[2]), self.fromFile(x[3]))
                     for x in gen ]
        else:
            return [ (x[0], x[1], self.fromVersion(x[2])) for x in gen ]

    def getFileContents(self, authToken, sha1list):
	# XXX X this isn't properly checked!
	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.cfc-out')
	f = os.fdopen(fd, "w")

	fc = filecontainer.FileContainer(f)
	del f
	d = self.repos.getFileContents(sha1list)

	for sha1 in sha1list:
	    fc.addFile(sha1, d[sha1], "", d[sha1].fullSize)
	fc.close()

	fileName = os.path.basename(path)
	return "%s?%s" % (self.urlBase, fileName[:-4])

    def getAllTroveLeafs(self, authToken, troveNames):
	for troveName in troveNames:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	d = {}
	for (name, leafList) in \
			self.repos.troveStore.iterAllTroveLeafs(troveNames):
            if name != None:
                d[name] = leafList
	
	return d

    def getTroveLeavesByLabel(self, authToken, troveNameList, labelStr):
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

    def getTroveVersionsByLabel(self, authToken, troveNameList, labelStr):
	d = {}
	for troveName in troveNameList:
	    if not self.auth.check(authToken, write = False, trove = troveName):
		raise InsufficientPermission

	    d[troveName] = [ self.freezeVersion(x) for x in
		    self.repos.troveStore.iterTroveVersionsByLabel(troveName,
								   labelStr) ]

	return d

    def getTroveVersionFlavors(self, authToken, troveDict):
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

    def getTroveLatestVersion(self, authToken, pkgName, branchStr):
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

    def getTroveFlavorsLatestVersion(self, authToken, troveName, branchStr):
	branch = self.toBranch(branchStr)

	if not self.auth.check(authToken, write = False, trove = troveName,
			       label = branch.label()):
	    raise InsufficientPermission

	return self.repos.troveStore.iterTrovePerFlavorLeafs(troveName, branchStr)

    def getChangeSet(self, authToken, chgSetList, recurse, withFiles):
	l = []
	for (name, flavor, old, new, absolute) in chgSetList:
	    newVer = self.toVersion(new)

	    if not self.auth.check(authToken, write = False, trove = name,
				   label = newVer.branch().label()):
		raise InsufficientPermission

	    if old == 0:
		l.append((name, self.toFlavor(flavor), None,
			 self.toVersion(new), absolute))
	    else:
		l.append((name, self.toFlavor(flavor), self.toVersion(old),
			 self.toVersion(new), absolute))

	cs = self.repos.createChangeSet(l, recurse = recurse, 
					withFiles = withFiles)
	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-out')
	os.close(fd)
	cs.writeToFile(path)
	fileName = os.path.basename(path)
	return "%s?%s" % (self.urlBase, fileName[:-4])

    def iterAllTroveNames(self, authToken):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

	return self.repos.iterAllTroveNames()

    def prepareChangeSet(self, authToken):
	# make sure they have a valid account and permission to commit to
	# *something*
	if not self.auth.check(authToken, write = True):
	    raise InsufficientPermission

	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-in')
	os.close(fd)
	fileName = os.path.basename(path)
	return "%s?%s" % (self.urlBase, fileName[:-3])

    def commitChangeSet(self, authToken, url):
	assert(url.startswith(self.urlBase))
	# +1 strips off the ? from the query url
	fileName = url[len(self.urlBase) + 1:] + "-in"
	path = "%s/%s" % (self.tmpPath, fileName)

	try:
	    cs = changeset.ChangeSetFromFile(path)
	finally:
	    pass
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
	    if not pkgCs.getName().endswith(":source"): continue

	    d = { 'reppath' : self.urlBase,
	    	  'trove' : pkgCs.getName(),
		  'version' : pkgCs.getNewVersion().asString() }
	    cmd = self.commitAction % d
	    os.system(cmd)

	return True

    def getFileVersion(self, authToken, fileId, version, withContents = 0):
	# XXX needs to authentication against the trove the file is part of,
	# which is unfortunate
	f = self.repos.troveStore.getFile(fileId, self.toVersion(version))
	return self.fromFile(f)

    def checkVersion(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise InsufficientPermission

        if clientVersion < 0:
            raise RuntimeError, "client is too old"
        return 0

    def __init__(self, path, tmpPath, urlBase, authDbPath,
		 commitAction = None):
	self.repos = fsrepos.FilesystemRepository(path)
	self.repPath = path
	self.tmpPath = tmpPath
	self.urlBase = urlBase
	self.auth = NetworkAuthorization(authDbPath, anonymousReads = True)
	self.commitAction = commitAction

class NetworkAuthorization:

    def check(self, authToken, write = False, label = None, trove = None):
	if not write and self.anonReads:
	    return True

	if not authToken[0]:
	    return False

	stmt = """
	    SELECT count(*) FROM
	       (SELECT userId as uuserId FROM Users WHERE user=%s AND 
		    password=%s) 
	    JOIN Permissions ON uuserId=Permissions.userId
	""" 
	m = md5.new()
	m.update(authToken[1])
	params = [authToken[0], m.hexdigest()]

	where = []
	if label:
	    where.append(" labelId=(SELECT labelId FROM Labels WHERE " \
			    "label=%s) OR labelId is Null")
	    params.append(label.asString())

	if trove:
	    where.append(" troveNameId=(SELECT troveNameId FROM TroveNames "
			        "WHERE troveName=%s) OR troveNameId is Null" )
	    params.append(trove)

	if write:
	    where.append("write=1")

	if where:
	    stmt += "WHERE " + " AND ".join(where)

	cu = self.db.cursor()
	cu.execute(stmt, params)
	result = cu.fetchone()[0]

	return result != 0

    def __init__(self, dbpath, anonymousReads = False):
	self.db = sqlite.connect(dbpath)
	self.anonReads = anonymousReads

class InsufficientPermission(Exception):

    pass
