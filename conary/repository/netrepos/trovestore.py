#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import os
import itertools

from conary import files, metadata, trove, versions, changelog
from conary.deps import deps
from conary.lib import util, tracelog
from conary.local import deptable
from conary.local.sqldb import VersionCache, FlavorCache
from conary.local import versiontable
from conary.repository import errors
from conary.repository.netrepos import instances, items, keytable, flavors,\
     troveinfo, versionops, cltable, accessmap
from conary.server import schema


class LocalRepVersionTable(versiontable.VersionTable):

    def getId(self, theId, itemId):
        cu = self.db.cursor()
        cu.execute("""
        SELECT Versions.version, Nodes.timeStamps
        FROM Nodes
        JOIN Versions USING (versionId)
        WHERE Nodes.versionId=? AND Nodes.itemId=?
        """, theId, itemId)
	try:
	    (s, t) = cu.next()
	    v = self._makeVersion(s, t)
	    return v
	except StopIteration:
            raise KeyError, theId

    def getTimeStamps(self, version, itemId):
        cu = self.db.cursor()
        cu.execute("""
        SELECT timeStamps
        FROM Nodes
        WHERE versionId = (SELECT versionId from Versions WHERE version=?)
          AND itemId=?""", version.asString(), itemId)
	try:
	    (t,) = cu.next()
	    return [ float(x) for x in t.split(":") ]
	except StopIteration:
            raise KeyError, itemId

class TroveStore:
    def __init__(self, db, log = None):
	self.db = db

	self.items = items.Items(self.db)
	self.flavors = flavors.Flavors(self.db)
        self.branchTable = versionops.BranchTable(self.db)
        self.changeLogs = cltable.ChangeLogTable(self.db)

	self.versionTable = LocalRepVersionTable(self.db)
	self.versionOps = versionops.SqlVersioning(
            self.db, self.versionTable, self.branchTable)
	self.instances = instances.InstanceTable(self.db)
        self.latest = versionops.LatestTable(self.db)
        self.ri = accessmap.RoleInstances(self.db)
        
        self.keyTable = keytable.OpenPGPKeyTable(self.db)
        self.depTables = deptable.DependencyTables(self.db)
        self.metadataTable = metadata.MetadataTable(self.db, create = False)
        self.troveInfoTable = troveinfo.TroveInfoTable(self.db)

        self.needsCleanup = False
        self.log = log or tracelog.getLog(None)
        self.LATEST_TYPE_ANY = versionops.LATEST_TYPE_ANY
        self.LATEST_TYPE_NORMAL = versionops.LATEST_TYPE_NORMAL
        self.LATEST_TYPE_PRESENT = versionops.LATEST_TYPE_PRESENT

        self.versionIdCache = {}
        self.seenFileId = set()
        self.itemIdCache = {}

    def __del__(self):
        self.db = self.log = None

    def getLabelId(self, label):
        self.versionOps.labels.getOrAddId(label)

    def getItemId(self, item):
        itemId = self.itemIdCache.get(item, None)
        if itemId is not None:
            return itemId
        itemId = self.items.getOrAddId(item)
        self.itemIdCache[item] = itemId
        return itemId

    def getInstanceId(self, itemId, versionId, flavorId, clonedFromId,
                      troveType, isPresent = instances.INSTANCE_PRESENT_NORMAL):
 	theId = self.instances.get((itemId, versionId, flavorId), None)
	if theId == None:
	    theId = self.instances.addId(itemId, versionId, flavorId,
                                         clonedFromId,
					 troveType, isPresent = isPresent)
        # XXX we shouldn't have to do this unconditionally
        if isPresent != instances.INSTANCE_PRESENT_MISSING:
	    self.instances.update(theId, isPresent=isPresent, clonedFromId=clonedFromId)
            self.items.setTroveFlag(itemId, 1)
 	return theId

    def getVersionId(self, version):
	theId = self.versionIdCache.get(version, None)
	if theId:
	    return theId

	theId = self.versionTable.get(version, None)
	if theId == None:
	    theId = self.versionTable.addId(version)

	self.versionIdCache[version] = theId
	return theId

    def getFullVersion(self, item, version):
	"""
	Updates version with full timestamp information.
	"""
	cu = self.db.cursor()
	cu.execute("""
        SELECT timeStamps
        FROM Nodes
        WHERE itemId=(SELECT itemId FROM Items WHERE item=?)
          AND versionId=(SELECT versionId FROM Versions WHERE version=?)
	""", item, version.asString())

	timeStamps = cu.fetchone()[0]
	version.setTimeStamps([float(x) for x in timeStamps.split(":")])

    def createTroveBranch(self, troveName, branch):
	itemId = self.getItemId(troveName)
	self.versionOps.createBranch(itemId, branch)

    def getTroveFlavors(self, troveDict):
	cu = self.db.cursor()
	vMap = {}
	outD = {}
	# I think we might be better of intersecting subqueries rather
	# then using all of the and's in this join

        schema.resetTable(cu, 'tmpIVF')
        for troveName in troveDict.keys():
            outD[troveName] = {}
            for version in troveDict[troveName]:
                outD[troveName][version] = []
                versionStr = version.asString()
                vMap[versionStr] = version
                cu.execute("INSERT INTO tmpIVF VALUES (?, ?, ?)",
                           (troveName, versionStr, versionStr),
                           start_transaction = False)
        self.db.analyze("tmpIVF")
        cu.execute("""
        SELECT Items.item, fullVersion, Flavors.flavor
        FROM tmpIVF
        JOIN Items on tmpIVF.item = Items.item
        JOIN Versions on tmpIVF.version = Versions.version
        JOIN Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId
        JOIN Flavors using (flavorId)
        ORDER BY Items.item, fullVersion
        """)

        for (item, verString, flavor) in cu:
            ver = vMap[verString]
            outD[item][ver].append(flavor)

	return outD

    def iterTroveNames(self):
        cu = self.db.cursor()
        cu.execute("""
        SELECT DISTINCT Items.item as item
        FROM Instances
        JOIN Items USING(itemId)
        WHERE Instances.isPresent = ?
        ORDER BY item
        """, instances.INSTANCE_PRESENT_NORMAL)
        for (item,) in cu:
            yield item

    # refresh the latest for all the recently presented troves
    def presentHiddenTroves(self):
        cu = self.db.cursor()
        cu.execute("""
        select i.instanceId, i.itemId, n.branchId, i.flavorId
        from Instances as i join Nodes as n using(itemId, versionId)
        where i.isPresent = ?""", instances.INSTANCE_PRESENT_HIDDEN)
        for (instanceId, itemId, branchId, flavorId) in cu.fetchall():
            cu.execute("UPDATE Instances SET isPresent=? WHERE instanceId=?",
                       (instances.INSTANCE_PRESENT_NORMAL, instanceId))
            self.latest.update(cu, itemId, branchId, flavorId)

    def addTrove(self, trv, hidden = False):
	cu = self.db.cursor()
        schema.resetTable(cu, 'tmpNewFiles')
        schema.resetTable(cu, 'tmpNewRedirects')
	return (cu, trv, hidden, [])

    # tmpNewFiles can contain duplicate entries for the same fileId,
    # (with stream being NULL or not), so the FileStreams update
    # is happening in three steps:
    # 1. Update existing fileIds
    # 2. Insert new fileIds with non-NULL streams
    # 3. Insert new fileIds that might have NULL streams.
    def _addTroveNewFiles(self, cu, troveInstanceId):
        # We split these steps into separate queries because most DB
        # backends I have tried will get the optimization of a single
        # query wrong --gafton

        # In the extreme case of binary shadowing this might require a
        # bit of of memory for larger troves, but it is preferable to
        # constant full table scans in the much more common cases
        cu.execute("""
        SELECT tmpNewFiles.fileId, tmpNewFiles.stream
        FROM tmpNewFiles
        JOIN FileStreams USING(fileId)
        WHERE FileStreams.stream IS NULL
        AND tmpNewFiles.stream IS NOT NULL
        """)
        for (fileId, stream) in cu.fetchall():
            cu.execute("UPDATE FileStreams SET stream = ? WHERE fileId = ?",
                       (cu.binary(stream), cu.binary(fileId)))
        # select the new non-NULL streams out of tmpNewFiles and Insert
        # them in FileStreams
        cu.execute("""
        INSERT INTO FileStreams (fileId, stream, sha1)
        SELECT DISTINCT NF.fileId, NF.stream, NF.sha1
        FROM tmpNewFiles AS NF
        LEFT JOIN FileStreams AS FS USING(fileId)
        WHERE FS.fileId IS NULL
          AND NF.stream IS NOT NULL
          """)
        # now insert the other fileIds. select the new non-NULL streams
        # out of tmpNewFiles and insert them in FileStreams
        cu.execute("""
        INSERT INTO FileStreams (fileId, stream, sha1)
        SELECT DISTINCT NF.fileId, NF.stream, NF.sha1
        FROM tmpNewFiles AS NF
        LEFT JOIN FileStreams AS FS USING(fileId)
        WHERE FS.fileId IS NULL
        """)
        # need to keep a list of new dirnames we're inserting
        cu.execute(""" select distinct tnf.dirname from tmpNewFiles as tnf
        where not exists (
            select 1 from Dirnames as d where d.dirname = tnf.dirname )""")
        newDirnames = cu.fetchall()
        schema.resetTable(cu, "tmpItems")
        self.db.bulkload("tmpItems", newDirnames, ["item"])
        self.db.bulkload("Dirnames", newDirnames, ["dirname"])
        prefixList = []
        # all the new dirnames need to be processed for Prefixes links
        def _getPrefixes(dirname):
            # note: we deliberately do not insert '/' as a prefix for
            # all directories, since not walking through the Prefixes
            # is faster than looping through the entire Dirnames set
            d, b = os.path.split(dirname)
            if d == '/':
                return [dirname]
            if d == '':
                return []
            ret = _getPrefixes(d)
            ret.append(dirname)
            return ret
        cu.execute("select dirnameId, dirname from Dirnames "
                   "join tmpItems on dirname = item ")
        for dirnameId, dirname in cu.fetchall():
            prefixList += [ (dirnameId, x) for x in _getPrefixes(dirname) ]
        if prefixList:
            schema.resetTable(cu, "tmpItems")
            self.db.bulkload("tmpItems", prefixList, ["itemId", "item"])
            # insert any new prefix strings into Dirnames
            cu.execute(""" insert into Dirnames(dirname)
            select distinct item from tmpItems where not exists (
                select 1 from Dirnames as d where d.dirname = tmpItems.item ) """)
            # now populate Prefixes
            cu.execute(""" insert into Prefixes (dirnameId, prefixId)
            select tmpItems.itemId, d.dirnameId from tmpItems
            join Dirnames as d on tmpItems.item = d.dirname """)
        # done with processing the Prefixes for new Dirnames
        cu.execute(""" insert into Basenames(basename)
        select distinct tnf.basename from tmpNewFiles as tnf
        where not exists (
            select 1 from Basenames as b where b.basename = tnf.basename ) """)
        cu.execute(""" insert into FilePaths (pathId, dirnameId, basenameId)
        select tnf.pathId, d.dirnameId, b.basenameId
        from tmpNewFiles as tnf
        join Dirnames as d on tnf.dirname = d.dirname
        join Basenames as b on tnf.basename = b.basename
        where not exists (
            select 1 from FilePaths as fp
            where fp.pathId = tnf.pathId
              and fp.dirnameId = d.dirnameId
              and fp.basenameId = b.basenameId ) """)

        # create the TroveFiles links for this trove's files.
        cu.execute(""" insert into TroveFiles (instanceId, streamId, versionId, filePathId)
        select %d, fs.streamId, tnf.versionId, fp.filePathId
        from tmpNewFiles as tnf
        join Dirnames as d on tnf.dirname = d.dirname
        join Basenames as b on tnf.basename = b.basename
        join FilePaths as fp on
            tnf.pathId = fp.pathId and
            fp.dirnameId = d.dirnameId and
            fp.basenameId = b.basenameId
        join FileStreams as fs on tnf.fileId = fs.fileId
        """ % (troveInstanceId,))

    def _addTroveNewTroves(self, cu, troveInstanceId, troveType):
        # need to use self.items.addId to keep the CheckTroveCache in
        # sync for any new items we might add
        cu.execute("""
        SELECT DISTINCT tmpTroves.item
        FROM tmpTroves
        LEFT JOIN Items USING (item)
        WHERE Items.itemId is NULL
        """)
        for (newItem,) in cu.fetchall():
            self.items.addId(newItem)

        # look for included troves with no instances yet; we make those
        # entries manually here
        cu.execute("""
        SELECT Items.itemId, tmpTroves.frozenVersion, Flavors.flavorId
        FROM tmpTroves
        JOIN Items USING (item)
        JOIN Flavors ON Flavors.flavor = tmpTroves.flavor
        LEFT JOIN Versions ON Versions.version = tmpTroves.version
        LEFT JOIN Instances ON
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        WHERE Instances.instanceId is NULL
        """)

        for (itemId, version, flavorId) in cu.fetchall():
	    # make sure the versionId and nodeId exists for this (we need
	    # a nodeId, or the version doesn't get timestamps)
            version = versions.ThawVersion(version)
	    versionId = self.getVersionId(version)

            # sourcename = None for now.
            # will be fixed up when the real trove is comitted
	    if versionId is not None:
		nodeId = self.versionOps.nodes.getRow(itemId, versionId, None)
		if nodeId is None:
		    (nodeId, versionId) = self.versionOps.createVersion(
                        itemId, version, flavorId, sourceName = None)
		del nodeId
            else:
                (nodeId, versionId) = self.versionOps.createVersion(
                    itemId, version, flavorId, sourceName = None)
            # create the new instanceId entry.
            # cloneFromId = None for now.
            # will get fixed when the trove is comitted.
            # We actually don't quite care about the exact instanceId value we get back...
            self.getInstanceId(itemId, versionId, flavorId,
                               clonedFromId = None, troveType =  troveType,
                               isPresent = instances.INSTANCE_PRESENT_MISSING)

        cu.execute("""
        INSERT INTO TroveTroves (instanceId, includedId, flags)
        SELECT %d, Instances.instanceId, tmpTroves.flags
        FROM tmpTroves
        JOIN Items USING (item)
        JOIN Versions ON Versions.version = tmpTroves.version
        JOIN Flavors ON Flavors.flavor = tmpTroves.flavor
        JOIN Instances ON
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        """ %(troveInstanceId,))

    def addTroveDone(self, troveInfo, mirror=False):
        (cu, trv, hidden, newFilesInsertList) = troveInfo

        self.log(3, trv)

	troveVersion = trv.getVersion()
	troveItemId = self.getItemId(trv.getName())
        sourceName = trv.troveInfo.sourceName()

        # Pull out the clonedFromId
        clonedFrom = trv.troveInfo.clonedFrom()
        clonedFromId = None
        if clonedFrom:
            clonedFromId = self.versionTable.get(clonedFrom, None)
            if clonedFromId is None:
                clonedFromId = self.versionTable.addId(clonedFrom)

        isPackage = (not trv.getName().startswith('group') and
                     not trv.getName().startswith('fileset') and
                     ':' not in trv.getName())

	troveVersionId = self.versionTable.get(troveVersion, None)
	if troveVersionId is not None:
	    nodeId = self.versionOps.nodes.getRow(
                troveItemId, troveVersionId, None)

	troveFlavor = trv.getFlavor()

	# start off by creating the flavors we need; we could combine this
	# to some extent with the file table creation below, but there are
	# normally very few flavors per trove so this probably better
	flavorsNeeded = {}
	if troveFlavor is not None:
	    flavorsNeeded[troveFlavor] = True

	for (name, version, flavor) in trv.iterTroveList(strongRefs = True,
                                                           weakRefs = True):
	    if flavor is not None:
		flavorsNeeded[flavor] = True

        for (name, branch, flavor) in trv.iterRedirects():
            if flavor is not None:
                flavorsNeeded[flavor] = True

	flavorIndex = {}
        schema.resetTable(cu, "tmpItems")
	for flavor in flavorsNeeded.iterkeys():
	    flavorIndex[flavor.freeze()] = flavor
	    cu.execute("INSERT INTO tmpItems(item) VALUES(?)", flavor.freeze(),
                       start_transaction=False)
	del flavorsNeeded
        self.db.analyze("tmpItems")

	# it seems like there must be a better way to do this, but I can't
	# figure it out. I *think* inserting into a view would help, but I
	# can't with sqlite.
	cu.execute("""
        select tmpItems.item as flavor
        from tmpItems
        where not exists ( select flavor from Flavors
                           where Flavors.flavor = tmpItems.item ) """)
        # make a list of the flavors we're going to create.  Add them
        # after we have retrieved all of the rows from this select
        l = []
	for (flavorStr,) in cu:
            l.append(flavorIndex[flavorStr])
        for flavor in l:
	    self.flavors.createFlavor(flavor)

	flavors = {}
	cu.execute("""
        SELECT Flavors.flavor, Flavors.flavorId
        FROM tmpItems
        JOIN Flavors ON tmpItems.item = Flavors.flavor""")
	for (flavorStr, flavorId) in cu:
	    flavors[flavorIndex[flavorStr]] = flavorId

	del flavorIndex

	if troveFlavor is not None:
	    troveFlavorId = flavors[troveFlavor]
	else:
	    troveFlavorId = 0

	if troveVersionId is None or nodeId is None:
	    (nodeId, troveVersionId) = self.versionOps.createVersion(
                troveItemId, troveVersion, troveFlavorId, sourceName)
	    if trv.getChangeLog() and trv.getChangeLog().getName():
		self.changeLogs.add(nodeId, trv.getChangeLog())
        elif sourceName: # make sure the sourceItemId matches for the trove we are comitting
            sourceItemId = self.items.getOrAddId(sourceName)
            self.versionOps.nodes.updateSourceItemId(nodeId, sourceItemId, mirrorMode=mirror)

	# the instance may already exist (it could be referenced by a package
	# which has already been added)
        if hidden:
            presence = instances.INSTANCE_PRESENT_HIDDEN
        else:
            presence = instances.INSTANCE_PRESENT_NORMAL

	troveInstanceId = self.getInstanceId(troveItemId, troveVersionId,
                         troveFlavorId, clonedFromId, trv.getType(),
                         isPresent = presence)
        assert(cu.execute("SELECT COUNT(*) from TroveTroves WHERE "
                          "instanceId=?", troveInstanceId).next()[0] == 0)

        troveBranchId = self.branchTable[troveVersion.branch()]
        self.depTables.add(cu, trv, troveInstanceId)
        self.ri.addInstanceId(troveInstanceId)
        self.latest.update(cu, troveItemId, troveBranchId, troveFlavorId)

        # Fold tmpNewFiles into FileStreams
        if len(newFilesInsertList):
            self.db.bulkload("tmpNewFiles", newFilesInsertList,
                             [ "pathId", "versionId", "fileId", "stream",
                               "dirname", "basename", "sha1" ])
            self.db.analyze("tmpNewFiles")
            self._addTroveNewFiles(cu, troveInstanceId)

        # iterate over both strong and weak troves, and set weakFlag to
        # indicate which kind we're looking at when
        insertList = []
        for ((name, version, flavor), weakFlag) in itertools.chain(
                itertools.izip(trv.iterTroveList(strongRefs = True,
                                                   weakRefs   = False),
                               itertools.repeat(0)),
                itertools.izip(trv.iterTroveList(strongRefs = False,
                                                   weakRefs   = True),
                               itertools.repeat(schema.TROVE_TROVES_WEAKREF))):

            flags = weakFlag
            if trv.includeTroveByDefault(name, version, flavor):
                flags |= schema.TROVE_TROVES_BYDEFAULT

            # sanity check - version/flavor of components must match the
            # version/flavor of the package
            assert(not isPackage or version == trv.getVersion())
            assert(not isPackage or flavor == trv.getFlavor())
            insertList.append((name, str(version), version.freeze(),
                               flavor.freeze(), flags))
        if len(insertList):
            schema.resetTable(cu, 'tmpTroves')
            self.db.bulkload("tmpTroves", insertList, [
                "item", "version", "frozenVersion", "flavor", "flags" ])
            self.db.analyze("tmpTroves")
            self._addTroveNewTroves(cu, troveInstanceId, trv.getType())

        # process troveInfo and metadata...
        self.troveInfoTable.addInfo(cu, trv, troveInstanceId)

        # now add the redirects
        for (name, branch, flavor) in trv.iterRedirects():
            if flavor is None:
                frz = None
            else:
                frz = flavor.freeze()
            cu.execute("INSERT INTO tmpNewRedirects (item, branch, flavor) "
                       "VALUES (?, ?, ?)", (name, str(branch), frz),
                       start_transaction=False)
        self.db.analyze("tmpNewRedirects")
        
        # again need to pay attention to CheckTrovesCache and use items.addId()
        cu.execute("""
        SELECT tmpNewRedirects.item
        FROM tmpNewRedirects
        LEFT JOIN Items USING (item)
        WHERE Items.itemId is NULL
        """)
        for (newItem,) in cu.fetchall():
            self.items.addId(newItem)
        
        cu.execute("""
        INSERT INTO Branches (branch)
        SELECT tmpNewRedirects.branch
        FROM tmpNewRedirects
        LEFT JOIN Branches USING (branch)
        WHERE Branches.branchId is NULL
        """)

        cu.execute("""
        INSERT INTO Flavors (flavor)
        SELECT tmpNewRedirects.flavor
        FROM tmpNewRedirects
        LEFT JOIN Flavors USING (flavor)
        WHERE 
            Flavors.flavor is not NULL 
            AND Flavors.flavorId is NULL
        """)

        cu.execute("""
        INSERT INTO TroveRedirects (instanceId, itemId, branchId, flavorId)
        SELECT %d, Items.itemId, Branches.branchId, Flavors.flavorId
        FROM tmpNewRedirects
        JOIN Items USING (item)
        JOIN Branches ON tmpNewRedirects.branch = Branches.branch
        LEFT JOIN Flavors ON tmpNewRedirects.flavor = Flavors.flavor
        """ % troveInstanceId)

    def updateMetadata(self, troveName, branch, shortDesc, longDesc,
                    urls, licenses, categories, source, language):
        itemId = self.getItemId(troveName)
        branchId = self.branchTable[branch]

        # if we're updating the default language, always create a new version
        # XXX we can remove one vesionTable.get call from here...
        # XXX this entire mass of code can probably be improved.
        #     surely someone does something similar someplace else...
        latestVersion = self.metadataTable.getLatestVersion(itemId, branchId)
        if language == "C":
            if latestVersion: # a version exists, increment it
                version = versions.VersionFromString(latestVersion).copy()
                version.incrementSourceCount()
            else: # otherwise make a new version
                version = versions._VersionFromString("1-1", defaultBranch=branch)

            if not self.versionTable.get(version, None):
                self.versionTable.addId(version)
        else: # if this is a translation, update the current version
            if not latestVersion:
                raise KeyError, troveName
            version = versions.VersionFromString(latestVersion)

        versionId = self.versionTable.get(version, None)
        return self.metadataTable.add(itemId, versionId, branchId, shortDesc, longDesc,
                                      urls, licenses, categories, source, language)

    def getMetadata(self, troveName, branch, version=None, language="C"):
        itemId = self.items.get(troveName, None)
        if not itemId:
            return None

        # follow the branch tree up until we find metadata
        md = None
        while not md:
            # make sure we're on the same server
            if self.branchTable.has_key(branch):
                branchId = self.branchTable[branch]
            else:
                return None

            if not version:
                latestVersion = self.metadataTable.getLatestVersion(itemId, branchId)
            else:
                latestVersion = version.asString()
            cu = self.db.cursor()
            cu.execute("SELECT versionId FROM Versions WHERE version=?", latestVersion)

            versionId = cu.fetchone()
            if versionId:
                versionId = versionId[0]
            else:
                if branch.hasParentBranch():
                    branch = branch.parentBranch()
                else:
                    return None

            md = self.metadataTable.get(itemId, versionId, branchId, language)

        md["version"] = versions.VersionFromString(latestVersion).asString()
        md["language"] = language
        return metadata.Metadata(md)

    def hasTrove(self, troveName, troveVersion = None, troveFlavor = None):
        self.log(3, troveName, troveVersion, troveFlavor)

	if not troveVersion:
	    return self.items.has_key(troveName)

	assert(troveFlavor is not None)

        # if we can not find the ids for the troveName, troveVersion
        # or troveFlavor in their respective tables, than this trove
        # can't possibly exist...
	troveItemId = self.items.get(troveName, None)
	if troveItemId is None:
	    return False
	troveVersionId = self.versionTable.get(troveVersion, None)
	if troveVersionId is None:
            return False
	troveFlavorId = self.flavors.get(troveFlavor, None)
	if troveFlavorId is None:
            return False

	return self.instances.isPresent((troveItemId, troveVersionId,
					 troveFlavorId))

    def getTrove(self, troveName, troveVersion, troveFlavor, withFiles = True,
                 hidden = False):
	troveIter = self.iterTroves(( (troveName, troveVersion, troveFlavor), ),
                                    withFiles = withFiles, hidden = hidden)
        trv = [ x for x in troveIter ][0]
        if trv is None:
	    raise errors.TroveMissing(troveName, troveVersion)
        return trv

    def iterTroves(self, troveInfoList, withFiles = True, withFileStreams = False,
                   hidden = False):
        self.log(3, troveInfoList, "withFiles=%s withFileStreams=%s hidden=%s" % (
                        withFiles, withFileStreams, hidden))

        cu = self.db.cursor()
        schema.resetTable(cu, 'tmpNVF')
        schema.resetTable(cu, 'tmpInstanceId')

        for idx, (n,v,f) in enumerate(troveInfoList):
            cu.execute("INSERT INTO tmpNVF VALUES (?, ?, ?, ?)",
                       (idx, n, v.asString(), f.freeze()),
                       start_transaction = False)
        self.db.analyze("tmpNVF")
        args = [instances.INSTANCE_PRESENT_NORMAL]
        d = dict(presence = "Instances.isPresent = ?")
        if hidden:
            args.append(instances.INSTANCE_PRESENT_HIDDEN)
            d = dict(presence = "Instances.isPresent in (?,?)")
        d.update(self.db.keywords)
        cu.execute("""
        SELECT %(STRAIGHTJOIN)s tmpNVF.idx, Instances.instanceId, Instances.troveType,
               Nodes.timeStamps,
               Changelogs.name, ChangeLogs.contact, ChangeLogs.message
        FROM tmpNVF
        JOIN Items on tmpNVF.name = Items.item
        JOIN Versions on tmpNVF.version = Versions.version
        JOIN Flavors on tmpNVF.flavor = Flavors.flavor
        JOIN Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        JOIN Nodes on
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
        LEFT JOIN ChangeLogs using(nodeId)
        WHERE %(presence)s
        ORDER BY tmpNVF.idx
        """ % d, args)
        troveIdList = [ x for x in cu ]
        # short-circuit for cases when nothing matches
        if not troveIdList:
            for i in xrange(len(troveInfoList)):
                yield None
            return
        for singleTroveIds in troveIdList:
            cu.execute("INSERT INTO tmpInstanceId VALUES (?, ?)",
                       singleTroveIds[0], singleTroveIds[1],
                       start_transaction = False)
        self.db.analyze("tmpInstanceId")
        
        # unfortunately most cost-based optimizers will get the
        # following troveTrovesCursor queries wrong. Details in CNY-2695

        # for PostgreSQL we have to force an execution plan that
        # uses the join order as coded in the query
        if self.db.driver == 'postgresql':
            cu.execute("set join_collapse_limit to 2")
            cu.execute("set enable_seqscan to off")

        troveTrovesCursor = self.db.cursor()
        # the STRAIGHTJOIN hack will ask MySQL to execute the query as written
        troveTrovesCursor.execute("""
        SELECT %(STRAIGHTJOIN)s tmpInstanceId.idx, Items.item, Versions.version,
            Flavors.flavor, TroveTroves.flags, Nodes.timeStamps
        FROM tmpInstanceId
        JOIN TroveTroves using(instanceId)
        JOIN Instances on TroveTroves.includedId = Instances.instanceId
        JOIN Items on Instances.itemId = Items.itemId
        JOIN Versions on Instances.versionId = Versions.versionId
        JOIN Flavors on Instances.flavorId = Flavors.flavorId
        JOIN Nodes on
            Instances.itemId = Nodes.itemId and
            Instances.versionId = Nodes.versionId
        ORDER BY tmpInstanceId.idx
        """ % self.db.keywords)
        troveTrovesCursor = util.PeekIterator(troveTrovesCursor)

        # revert changes we forced on the postgresql optimizer
        if self.db.driver == 'postgresql':
            cu.execute("set join_collapse_limit to default")
            cu.execute("set enable_seqscan to default")

        troveFilesCursor = iter(())
        if withFileStreams or withFiles:
            troveFilesCursor = self.db.cursor()
            streamSel = "NULL"
            if withFileStreams:
                streamSel = "FileStreams.stream"
            troveFilesCursor.execute("""
            SELECT tmpInstanceId.idx, FilePaths.pathId,
                   Dirnames.dirname, Basenames.basename,
                   Versions.version, FileStreams.fileId, %s
            FROM tmpInstanceId
            JOIN TroveFiles using(instanceId)
            JOIN FileStreams using(streamId)
            JOIN FilePaths ON TroveFiles.filePathId = FilePaths.filePathId
            JOIN Dirnames ON FilePaths.dirnameId = Dirnames.dirnameId
            JOIN Basenames ON FilePaths.basenameId = Basenames.basenameId
            JOIN Versions ON TroveFiles.versionId = Versions.versionId
            ORDER BY tmpInstanceId.idx """ % (streamSel,))
        troveFilesCursor = util.PeekIterator(troveFilesCursor)

        troveRedirectsCursor = self.db.cursor()
        troveRedirectsCursor.execute("""
        SELECT tmpInstanceId.idx, Items.item, Branches.branch, Flavors.flavor 
        FROM tmpInstanceId 
        JOIN TroveRedirects using (instanceId)
        JOIN Items using (itemId)
        JOIN Branches ON TroveRedirects.branchId = Branches.branchId
        LEFT JOIN Flavors ON TroveRedirects.flavorId = Flavors.flavorId
        ORDER BY tmpInstanceId.idx
        """)
        troveRedirectsCursor = util.PeekIterator(troveRedirectsCursor)

        neededIdx = 0
        versionCache = VersionCache()
        flavorCache = FlavorCache()
        while troveIdList:
            (idx, troveInstanceId, troveType, timeStamps,
             clName, clVersion, clMessage) =  troveIdList.pop(0)

            # make sure we've returned something for everything up to this
            # point
            while neededIdx < idx:
                neededIdx += 1
                yield None

            # we need the one after this next time through
            neededIdx += 1

            singleTroveInfo = troveInfoList[idx]

            if clName is not None:
                changeLog = changelog.ChangeLog(clName, clVersion, clMessage)
            else:
                changeLog = None

            v = singleTroveInfo[1]
            key = (v, timeStamps)
            if versionCache.has_key(key):
                v = versionCache(key)
            else:
                v = v.copy()
                v.setTimeStamps([ float(x) for x in timeStamps.split(":") ])

            trv = trove.Trove(singleTroveInfo[0], v,
                              singleTroveInfo[2], changeLog,
                              type = troveType,
                              setVersion = False)

            try:
                while troveTrovesCursor.peek()[0] == idx:
                    idxA, name, version, flavor, flags, timeStamps = \
                                                troveTrovesCursor.next()
                    version = versionCache.get(version, timeStamps)
                    flavor = flavorCache.get(flavor)
                    byDefault = (flags & schema.TROVE_TROVES_BYDEFAULT) != 0
                    weakRef = (flags & schema.TROVE_TROVES_WEAKREF) != 0
                    trv.addTrove(name, version, flavor, byDefault = byDefault,
                                 weakRef = weakRef)
            except StopIteration:
                # we're at the end; that's okay
                pass

	    fileContents = {}
            try:
                while troveFilesCursor.peek()[0] == idx:
                    idxA, pathId, dirname, basename, versionId, fileId, stream = \
                            troveFilesCursor.next()
                    path = os.path.join(dirname, basename)
                    version = versions.VersionFromString(versionId)
                    trv.addFile(cu.frombinary(pathId), path, version, 
                                cu.frombinary(fileId))
		    if stream is not None:
			fileContents[fileId] = stream
            except StopIteration:
                # we're at the end; that's okay
                pass

            try:
                while troveRedirectsCursor.peek()[0] == idx:
                    idxA, targetName, targetBranch, targetFlavor = \
                            troveRedirectsCursor.next()
                    targetBranch = versions.VersionFromString(targetBranch)
                    if targetFlavor is not None:
                        targetFlavor = deps.ThawFlavor(targetFlavor)

                    trv.addRedirect(targetName, targetBranch, targetFlavor)
            except StopIteration:
                # we're at the end; that's okay
                pass

            self.depTables.get(cu, trv, troveInstanceId)
            self.troveInfoTable.getInfo(cu, trv, troveInstanceId)

	    if withFileStreams:
		yield trv, fileContents
	    else:
		yield trv

        # yield None for anything not found at the end
        while neededIdx < len(troveInfoList):
            neededIdx += 1
            yield None

    def findFileVersion(self, fileId):
        cu = self.db.cursor()
        cu.execute("SELECT stream FROM FileStreams WHERE fileId=?", (fileId,))

        for (stream,) in cu:
            # if stream is None, it means that this is just a reference
            # to a stream that actually lives in another repository.
            # there is a (unlikely) chance that there is another
            # row inthe table that matches this fileId, since there
            # isn't a unique constraint on fileId.
            if stream is None:
                continue
            return files.ThawFile(cu.frombinary(stream), cu.frombinary(fileId))

        return None

    def iterFilesInTrove(self, troveName, troveVersion, troveFlavor,
                         sortByPath = False, withFiles = False):
        troveInstanceId = self.instances.getInstanceId(
            troveName, troveVersion, troveFlavor)
        sort = ""
	if sortByPath:
	    sort = " order by d.dirname, b.basename"
        # iterfiles
	cu = self.db.cursor()
	cu.execute("""
        select fp.pathId, d.dirname, b.basename, fs.fileId,
               tf.versionId, fs.stream
        from TroveFiles as tf
        join FileStreams as fs using (streamId)
        join FilePaths as fp on tf.filePathId = fp.filePathId
        join Dirnames as d on fp.dirnameId = d.dirnameId
        join Basenames as b on fp.basenameId = b.basenameId
        where tf.instanceId = ?
        %s""" % sort, troveInstanceId)

	versionCache = {}
	for (pathId, dirname, basename, fileId, versionId, stream) in cu:
	    version = versionCache.get(versionId, None)
            path = os.path.join(dirname, basename)
	    if not version:
		version = self.versionTable.getBareId(versionId)
		versionCache[versionId] = version

            if stream: # try thawing as a sanity check
                files.ThawFile(cu.frombinary(stream), cu.frombinary(fileId))

	    if withFiles:
		yield (cu.frombinary(pathId), path, cu.frombinary(fileId), 
                       version, cu.frombinary(stream))
	    else:
		yield (cu.frombinary(pathId), path, cu.frombinary(fileId), 
                       version)

    def addFile(self, troveInfo, pathId, fileObj, path, fileId, fileVersion,
                fileStream = None):
	cu = troveInfo[0]
        newFilesInsertList = troveInfo[3]
        dirname, basename = os.path.split(path)
        
	versionId = self.getVersionId(fileVersion)
        # if we have seen this fileId before, ignore the new stream data
        if fileId in self.seenFileId:
            fileObj = fileStream = None
        if fileObj or fileStream:
            sha1 = None

            if fileStream is None:
                fileStream = fileObj.freeze()
            if fileObj is not None:
                if fileObj.hasContents:
                    sha1 = fileObj.contents.sha1()
            elif files.frozenFileHasContents(fileStream):
                cont = files.frozenFileContentInfo(fileStream)
                sha1 = cont.sha1()
            self.seenFileId.add(fileId)
            newFilesInsertList.append((cu.binary(pathId), versionId,
                                       cu.binary(fileId), cu.binary(fileStream),
                                       dirname, basename, cu.binary(sha1)))
	else:
            newFilesInsertList.append((cu.binary(pathId), versionId,
                                       cu.binary(fileId), None,
                                       dirname, basename, None))

    def getFile(self, pathId, fileId):
        cu = self.db.cursor()
        cu.execute("SELECT stream FROM FileStreams WHERE fileId=?",
                   cu.binary(fileId))
        try:
            stream = cu.next()[0]
        except StopIteration:
            raise errors.FileStreamMissing(fileId)

        if stream is not None:
            return files.ThawFile(cu.frombinary(stream), cu.frombinary(pathId))
        else:
            return None

    def getFiles(self, l):
        # this only needs a list of (pathId, fileId) pairs, but it sometimes
        # gets (pathId, fileId, version) pairs instead (which is what
        # the network repository client uses)
        retr = FileRetriever(self.db, self.log)
        d = retr.get(l)
        del retr
        return d

    def _cleanCache(self):
        self.versionIdCache = {}
        self.itemIdCache = {}
        self.seenFileId = set()

    def begin(self, serialize=False):
        self._cleanCache()
        cu = self.db.transaction()
        if serialize:
            schema.lockCommits(self.db)
        return cu
    
    def rollback(self):
        self._cleanCache()
        return self.db.rollback()

    def _removeTrove(self, name, version, flavor, markOnly = False):
        #if name.startswith('group-') and not name.endswith(':source'):
            #raise errors.CommitError('Marking a group as removed is not implemented')
        cu = self.db.cursor()

        cu.execute("""
        SELECT I.instanceId, I.itemId, I.versionId, I.flavorId, I.troveType
        FROM Instances as I
        JOIN Items USING (itemId)
        JOIN Versions ON I.versionId = Versions.versionId
        JOIN Flavors ON I.flavorId = Flavors.flavorId
        WHERE Items.item = ?
          AND Versions.version = ?
          AND Flavors.flavor = ?
        """, (name, version.asString(), flavor.freeze()))

        try:
            instanceId, itemId, versionId, flavorId, troveType = cu.next()
        except StopIteration:
            raise errors.TroveMissing(name, version)

        if troveType == trove.TROVE_TYPE_REMOVED:
            # double removes are okay; they just get ignored
            return []

        assert(troveType == trove.TROVE_TYPE_NORMAL or
               troveType == trove.TROVE_TYPE_REDIRECT)

        # remove all dependencies which are used only by this instanceId
        cu.execute("""
        select prov.depId as depId from
        ( select a.depId as depId from
          ( select depId, instanceId from Provides where instanceId = :instanceId
            union
            select depId, instanceId  from Requires where instanceId = :instanceId
          ) as a
          left join Provides as p on a.depId = p.depId and p.instanceId != a.instanceId
          where p.depId is NULL
        ) as prov
        join
        ( select a.depId as depId from
          ( select depId, instanceId from Provides where instanceId = :instanceId
            union
            select depId, instanceId  from Requires where instanceId = :instanceId
          ) as a
          left join Requires as r on a.depId = r.depId and r.instanceId != a.instanceId
          where r.depId is NULL
        ) as reqs
        on prov.depId = reqs.depId
        """, instanceId = instanceId )
        depsToRemove = [ x[0] for x in cu ]

        cu.execute("DELETE FROM Provides WHERE instanceId = ?", instanceId)
        cu.execute("DELETE FROM Requires WHERE instanceId = ?", instanceId)

        if depsToRemove:
            cu.execute("DELETE FROM Dependencies WHERE depId IN (%s)"
                       % ",".join([ "%d" % x for x in depsToRemove ]))

        # Remove from TroveInfo
        cu.execute("DELETE FROM TroveInfo WHERE instanceId = ?", instanceId)

        # Look for path/pathId combinations we don't need anymore
        cu.execute("""
        select FilePaths.filePathId
        from TroveFiles as TF
        join FilePaths using(filePathId)
        where TF.instanceId = ?
          and not exists (
              select instanceId from TroveFiles as Others
              where Others.filePathId = TF.filePathId
                and Others.instanceId != ? )
        """, (instanceId, instanceId))
        
        filePathIdsToRemove = [ x[0] for x in cu ]

        # Now remove the files. Gather a list of sha1s of files to remove
        # from the filestore.
        cu.execute("""
        select FileStreams.streamId, FileStreams.sha1
        from TroveFiles as TF
        join FileStreams using(streamId)
        where TF.instanceId = ?
          and not exists (
              select instanceId from TroveFiles as Others
              where Others.streamId = TF.streamId
                and Others.instanceId != ? )
        """, (instanceId, instanceId))

        r = cu.fetchall()
        # if sha1 is None, the file has no contents
        candidateSha1sToRemove = [ x[1] for x in r if x[1] is not None ]
        streamIdsToRemove = [ x[0] for x in r ]

        cu.execute("DELETE FROM TroveFiles WHERE instanceId = ?", instanceId)
        if streamIdsToRemove:
            cu.execute("DELETE FROM FileStreams WHERE streamId IN (%s)"
                       % ",".join("%d"%x for x in streamIdsToRemove))

        if filePathIdsToRemove:
            cu.execute("DELETE FROM FilePaths WHERE filePathId IN (%s)"
                       % ",".join("%d"%x for x in filePathIdsToRemove))
            # XXX: these cleanups are more expensive than they're worth, probably
            cu.execute(""" delete from Prefixes where not exists (
                select 1 from FilePaths as fp where fp.dirnameId = Prefixes.dirnameId ) """)
            cu.execute(""" delete from Dirnames where not exists (
                select 1 from FilePaths as fp where fp.dirnameId = Dirnames.dirnameId )
            and not exists (
                select 1 from Prefixes as p where p.prefixId = Dirnames.dirnameId ) """)
            cu.execute(""" delete from Basenames where not exists (
                select 1 from FilePaths as fp where fp.basenameId = Basenames.basenameId ) """)

        # we need to double check filesToRemove against other streams which
        # may need the same sha1
        filesToRemove = []
        for sha1 in candidateSha1sToRemove:
            cu.execute("SELECT COUNT(*) FROM FileStreams WHERE sha1=?", cu.binary(sha1))
            if cu.next()[0] == 0:
                filesToRemove.append(sha1)

        # tmpRemovals drives the removal of most of the shared tables
        schema.resetTable(cu, 'tmpRemovals')
        cu.execute("SELECT nodeId, branchId FROM Nodes "
                   "WHERE itemId = ? AND versionId = ?", (itemId, versionId))
        nodeId, branchId = cu.next()
        cu.execute("INSERT INTO tmpRemovals (itemId, versionId, flavorId, branchId) "
                   "VALUES (?, ?, ?, ?)", (itemId, versionId, flavorId, branchId),
                   start_transaction=False)
        # Look for troves which this trove references which aren't present
        # on this repository (if they are present, we shouldn't remove them)
        # and aren't referenced by anything else
        cu.execute("""
        INSERT INTO tmpRemovals (instanceId, itemId, versionId, flavorId, branchId)
        SELECT Instances.instanceId, Instances.itemId, Instances.versionId,
               Instances.flavorId, Nodes.branchId
        FROM TroveTroves
        JOIN Instances ON TroveTroves.includedId = Instances.instanceId
        JOIN Nodes ON
            Instances.itemId = Nodes.itemId AND
            Instances.versionId = Nodes.versionId
        WHERE
         TroveTroves.instanceId = ?
         and Instances.isPresent = ?
         and not exists (select instanceId from TroveTroves as Others
                          where Others.instanceId != ?
                            and Others.includedId = Instances.instanceId )
        """, (instanceId, instances.INSTANCE_PRESENT_MISSING, instanceId),
                   start_transaction=False)
        cu.execute("""
        INSERT INTO tmpRemovals (itemId, flavorId, branchId)
        SELECT TroveRedirects.itemId, TroveRedirects.flavorId,
               TroveRedirects.branchId
        FROM TroveRedirects WHERE TroveRedirects.instanceId = ?
        """, instanceId, start_transaction=False)
        self.db.analyze("tmpRemovals")

        # remove access to troves we're about to remove
        self.ri.deleteInstanceIds("tmpRemovals")
        cu.execute("DELETE FROM TroveTroves WHERE instanceId=?", instanceId)
        cu.execute("DELETE FROM TroveRedirects WHERE instanceId=?", instanceId)
        cu.execute("DELETE FROM Instances WHERE instanceId IN "
                        "(SELECT instanceId FROM tmpRemovals)")
        if markOnly:
            # We don't actually remove anything here; we just mark the trove
            # as removed instead
            cu.execute("UPDATE Instances SET troveType=? WHERE instanceId=?",
                       trove.TROVE_TYPE_REMOVED, instanceId)
            self.latest.update(cu, itemId, branchId, flavorId)
        else:
            self.ri.deleteInstanceId(instanceId)
            cu.execute("DELETE FROM Instances WHERE instanceId = ?", instanceId)

        # look for troves referenced by this one
        schema.resetTable(cu, 'tmpId')
        cu.execute("""
        INSERT INTO tmpId(id)
        SELECT Nodes.nodeId
        FROM tmpRemovals
        JOIN Nodes USING (itemId, versionId)
        LEFT JOIN Instances USING (itemId, versionId)
        WHERE Instances.itemId IS NULL
        """, start_transaction=False)
        self.db.analyze("tmpId")
        
        # Was this the only Instance for the node?
        cu.execute("""
        DELETE FROM Changelogs
        WHERE Changelogs.nodeId IN (SELECT id FROM tmpId)
        """)

        cu.execute("""
        DELETE FROM Nodes
        WHERE Nodes.nodeId IN (SELECT id FROM tmpId)
        """)

        # Now update the latest table
        self.latest.update(cu, itemId, branchId, flavorId)

        # Delete flavors which are no longer needed
        cu.execute("""
        DELETE FROM Flavors
        WHERE flavorId IN (
            SELECT tmpRemovals.flavorId
            FROM tmpRemovals
            LEFT JOIN LatestCache ON tmpRemovals.flavorId = LatestCache.flavorId
            LEFT JOIN TroveRedirects ON tmpRemovals.flavorId = TroveRedirects.flavorId
            WHERE LatestCache.flavorId IS NULL
              AND TroveRedirects.flavorId IS NULL
        )""")
        cu.execute("""
        DELETE FROM FlavorMap
        WHERE flavorId IN (
            SELECT tmpRemovals.flavorId
            FROM tmpRemovals
            LEFT JOIN Flavors USING (flavorId)
            WHERE Flavors.flavorId IS NULL
        )""")

        # do we need the labelmap entry anymore?
        cu.execute("SELECT COUNT(*) FROM Nodes WHERE itemId = ? AND "
                   "branchId = ?", itemId, branchId)
        count = cu.next()[0]

        # XXX This stinks, but to fix it we need a proper index column
        # on LabelMap.
        cu.execute("""
        SELECT itemId, branchId FROM tmpRemovals
        LEFT JOIN Nodes USING (itemId, branchId)
        WHERE Nodes.itemId IS NULL
        """)
        for rmItemId, rmBranchId in cu.fetchall():
            cu.execute("DELETE FROM LabelMap WHERE itemId=? AND branchId=?",
                       rmItemId, rmBranchId)

        # do we need these branchIds anymore?
        cu.execute("""
        DELETE FROM Branches
        WHERE branchId IN (
            SELECT tmpRemovals.branchId
            FROM tmpRemovals
            LEFT JOIN LabelMap ON tmpRemovals.branchId = LabelMap.branchId
            LEFT JOIN TroveRedirects ON tmpRemovals.branchId = TroveRedirects.branchId
            WHERE LabelMap.branchId IS NULL
              AND TroveRedirects.branchId IS NULL
        )""")

        # XXX It would be nice to narrow this down based on tmpRemovals, but
        # in reality the labels table never gets that big.
        schema.resetTable(cu, 'tmpId')
        cu.execute("""
        INSERT INTO tmpId(id)
        SELECT Labels.labelId
        FROM Labels
        LEFT JOIN LabelMap ON LabelMap.labelId = Labels.labelId
        WHERE LabelMap.labelId IS NULL
          AND Labels.labelId != 0
        """, start_transaction=False)
        self.db.analyze("tmpId")
        
        cu.execute("""
        DELETE FROM Labels
        WHERE labelId IN (SELECT id from tmpId)
        """)

        # do we need these branchIds anymore?
        cu.execute("""
        DELETE FROM Versions
        WHERE versionId IN (
            SELECT tmpRemovals.versionId
            FROM tmpRemovals
            LEFT JOIN Instances ON tmpRemovals.versionId = Instances.versionId
            LEFT JOIN TroveFiles ON tmpRemovals.versionId = TroveFiles.versionId
            WHERE Instances.versionId IS NULL
              AND TroveFiles.versionId IS NULL
        )""")

        cu.execute("""
        DELETE FROM Items
        WHERE itemId IN (
            SELECT tmpRemovals.itemId
            FROM tmpRemovals
            LEFT JOIN Instances ON tmpRemovals.itemId = Instances.itemId
            LEFT JOIN Nodes ON tmpRemovals.itemId = Nodes.itemId
            LEFT JOIN TroveRedirects ON tmpRemovals.itemId = TroveRedirects.itemId
            WHERE Instances.itemId IS NULL
              AND Nodes.itemId IS NULL
              AND TroveRedirects.itemId IS NULL
        )""")

        cu.execute("""
        DELETE FROM CheckTroveCache
        WHERE itemId IN (
            SELECT tmpRemovals.itemId
            FROM tmpRemovals
            LEFT JOIN Instances ON tmpRemovals.itemId = Instances.itemId
            LEFT JOIN Nodes ON tmpRemovals.itemId = Nodes.itemId
            LEFT JOIN TroveRedirects ON tmpRemovals.itemId = TroveRedirects.itemId
            WHERE Instances.itemId IS NULL
              AND Nodes.itemId IS NULL
              AND TroveRedirects.itemId IS NULL
        )""")

        # XXX what about metadata?
        return filesToRemove

    def markTroveRemoved(self, name, version, flavor):
        self.log(2, name, version, flavor)
        return self._removeTrove(name, version, flavor, markOnly = True)

    def getParentTroves(self, troveList):
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpNVF")
        schema.resetTable(cu, "tmpInstanceId")
        for (n,v,f) in troveList:
            cu.execute("insert into tmpNVF(name,version,flavor) values (?,?,?)",
                       (n,v,f), start_transaction=False)
        self.db.analyze("tmpNVF")
        # get the instanceIds of the parents of what we can find
        cu.execute("""
        insert into tmpInstanceId(instanceId)
        select distinct TroveTroves.instanceId
        from tmpNVF
        join Items on tmpNVF.name = Items.item
        join Versions on tmpNVF.version = Versions.version
        join Flavors on tmpNVF.flavor = Flavors.flavor
        join Instances on
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        join TroveTroves on TroveTroves.includedId = Instances.instanceId
        """, start_transaction=False)
        self.db.analyze("tmpInstanceId")
        # tmpInstanceId now has instanceIds of the parents
        cu.execute("""
        select Items.item, Versions.version, Flavors.flavor
        from tmpInstanceId
        join Instances on tmpInstanceId.instanceId = Instances.instanceId
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        """)
        return cu.fetchall()

    def commit(self):
	if self.needsCleanup:
	    assert(0)
	    self.instances.removeUnused()
	    self.items.removeUnused()
	    self.needsCleanup = False

	if self.versionOps.needsCleanup:
	    assert(0)
	    self.versionTable.removeUnused()
	    self.branchTable.removeUnused()
	    self.versionOps.labelMap.removeUnused()
	    self.versionOps.needsCleanup = False

	self.db.commit()
        self._cleanCache()

class FileRetriever:
    def __init__(self, db, log = None):
        self.db = db
        self.cu = db.cursor()
        schema.resetTable(self.cu, 'tmpFileId')
        self.log = log or tracelog.getLog(None)

    def get(self, l):
        lookup = range(len(l))

        insertL = []
        for itemId, tup in enumerate(l):
            (pathId, fileId) = tup[:2]
            insertL.append((itemId, self.cu.binary(fileId)))
            lookup[itemId] = (pathId, fileId)

        self.db.bulkload("tmpFileId", insertL, [ "itemId", "fileId" ],
                         start_transaction = False)

        self.db.analyze("tmpFileId")
        self.cu.execute("SELECT itemId, stream FROM tmpFileId "
                        "JOIN FileStreams using (fileId) ")
        d = {}
        for itemId, stream in self.cu:
            pathId, fileId = lookup[itemId]
            if stream is not None:
                f = files.ThawFile(self.cu.frombinary(stream), pathId)
            else:
                f = None
            d[(pathId, fileId)] = f
        schema.resetTable(self.cu, "tmpFileId")
        return d
