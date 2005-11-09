#
# Copyright (c) 2004-2005 rPath, Inc.
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
import cPickle
import fsrepos
import os
import re
import sys
import tempfile

from conary import files, trove, versions, sqlite3
from conary.deps import deps
from conary.lib import log, sha1helper, util
from conary.repository import changeset, errors, xmlshims
from conary.repository.netrepos import trovestore
from conary.datastore import IntegrityError
from conary.dbstore import idtable
from conary.lib.openpgpfile import KeyNotFound, BadSelfSignature, IncompatibleKey
from conary.lib.openpgpfile import TRUST_FULL
from conary.lib.openpgpkey import getKeyCache
from conary.lib.tracelog import logMe
from conary.local import sqldb, versiontable
from conary.repository.netrepos.netauth import NetworkAuthorization
from conary.repository import repository
from conary.trove import DigitalSignature

# a list of the protocol versions we understand. Make sure the first
# one in the list is the lowest protocol version we support and th
# last one is the current server protocol version
SERVER_VERSIONS = [ 36, 37 ]
CACHE_SCHEMA_VERSION = 17

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    schemaVersion = 7

    # lets the following exceptions pass:
    #
    # 1. Internal server error (unknown exception)
    # 2. netserver.InsufficientPermission

    # version filtering happens first. that's important for these flags
    # to make sense. it means that:
    #
    # _GET_TROVE_VERY_LATEST/_GET_TROVE_ALLOWED_FLAVOR
    #      returns all allowed flavors for the latest version of the trove
    #      which has any allowed flavor
    # _GET_TROVE_VERY_LATEST/_GET_TROVE_ALL_FLAVORS
    #      returns all flavors available for the latest version of the
    #      trove which has an allowed flavor
    # _GET_TROVE_VERY_LATEST/_GET_TROVE_BEST_FLAVOR
    #      returns the best flavor for the latest version of the trove
    #      which has at least one allowed flavor
    _GET_TROVE_ALL_VERSIONS = 1
    _GET_TROVE_VERY_LATEST  = 2         # latest of any flavor

    _GET_TROVE_NO_FLAVOR        = 1     # no flavor info is returned
    _GET_TROVE_ALL_FLAVORS      = 2     # all flavors (no scoring)
    _GET_TROVE_BEST_FLAVOR      = 3     # the best flavor for flavorFilter
    _GET_TROVE_ALLOWED_FLAVOR   = 4     # all flavors which are legal

    def callWrapper(self, protocol, port, methodname, authToken, args):
        def condRollback():
            if self.db.inTransaction:
                self.db.rollback()

	# reopens the sqlite db if it's changed
	self.reopen()
        self._port = port
        self._protocol = protocol

        try:
            # try and get the method to see if it exists
            method = self.__getattribute__(methodname)
        except AttributeError:
            return (True, ("MethodNotSupported", methodname, ""))

        logMe(2, "calling", methodname)
        try:
            # the first argument is a version number
            r = method(authToken, *args)
            return (False, r)
	except errors.TroveMissing, e:
            condRollback()
	    if not e.troveName:
		return (True, ("TroveMissing", "", ""))
	    elif not e.version:
		return (True, ("TroveMissing", e.troveName, ""))
	    else:
		return (True, ("TroveMissing", e.troveName, 
			self.fromVersion(e.version)))
        except IntegrityError, e:
            condRollback()
            return (True, ('IntegrityError', str(e)))
	except trove.TroveIntegrityError, e:
            condRollback()
            return (True, ("TroveIntegrityError", str(e) +
                           # add a helpful error message for now
                        ' (you may need to update to conary 0.62.12 or later)'))
        except errors.FileContentsNotFound, e:
            condRollback()
            return (True, ('FileContentsNotFound', self.fromFileId(e.val[0]),
                           self.fromVersion(e.val[1])))
        except errors.FileStreamNotFound, e:
            condRollback()
            return (True, ('FileStreamNotFound', self.fromFileId(e.val[0]),
                           self.fromVersion(e.val[1])))
        except sqlite3.InternalError, e:
            condRollback()
            if str(e) == 'database is locked':
                return (True, ('RepositoryLocked'))
            raise
	except Exception, e:
            condRollback()
            for klass, marshall in errors.simpleExceptions:
                if isinstance(e, klass):
                    return (True, (marshall, str(e)))
            raise
	#    return (True, ("Unknown Exception", str(e)))
	#except Exception:
	#    import traceback, sys, string
        #    import lib.debugger
        #    lib.debugger.st()
	#    excInfo = sys.exc_info()
	#    lines = traceback.format_exception(*excInfo)
	#    print string.joinfields(lines, "")
	#    if sys.stdout.isatty() and sys.stdin.isatty():
	#	lib.debugger.post_mortem(excInfo[2])
	#    raise

    def urlBase(self):
        return self.basicUrl % { 'port' : self._port,
                                 'protocol' : self._protocol }

    def addUser(self, authToken, clientVersion, user, newPassword):
        # adds a new user, with no acls. for now it requires full admin
        # rights
        if not self.auth.checkIsFullAdmin(authToken[0], authToken[1]):
            raise errors.InsufficientPermission

        self.auth.addUser(user, newPassword)

        return True

    def addUserByMD5(self, authToken, clientVersion, user, salt, newPassword):
        # adds a new user, with no acls. for now it requires full admin
        # rights
        if not self.auth.checkIsFullAdmin(authToken[0], authToken[1]):
            raise errors.InsufficientPermission

        #Base64 decode salt
        self.auth.addUserByMD5(user, base64.decodestring(salt), newPassword)
        return True

    def deleteUserByName(self, authToken, clientVersion, user):
        if not self.auth.checkIsFullAdmin(authToken[0], authToken[1]):
            raise errors.InsufficientPermission

        self.auth.deleteUserByName(user)
        return True

    def deleteUserById(self, authToken, clientVersion, userId):
        if not self.auth.checkIsFullAdmin(authToken[0], authToken[1]):
            raise errors.InsufficientPermission

        error = self.auth.deleteUserById(userId)
        if error:
            print >>sys.stderr, error
            sys.stderr.flush()
            return False
        else:
            return True

    def addAcl(self, authToken, clientVersion, userGroup, trovePattern,
               label, write, capped, admin):
        if not self.auth.checkIsFullAdmin(authToken[0], authToken[1]):
            raise errors.InsufficientPermission

        if trovePattern == "":
            trovePattern = None

        if label == "":
            label = None

        self.auth.addAcl(userGroup, trovePattern, label, write, capped,
                         admin)

        return True

    def editAcl(self, authToken, clientVersion, userGroup, oldTrovePattern,
                oldLabel, trovePattern, label, write, capped, admin):
        if not self.auth.checkIsFullAdmin(authToken[0], authToken[1]):
            raise errors.InsufficientPermission

        if trovePattern == "":
            trovePattern = "ALL"

        if label == "":
            label = "ALL"

        #Get the Ids
        troveId = self.troveStore.getItemId(trovePattern)
        oldTroveId = self.troveStore.items.get(oldTrovePattern, None)

        labelId = idtable.IdTable.get(self.troveStore.versionOps.labels, label, None)
        oldLabelId = idtable.IdTable.get(self.troveStore.versionOps.labels, oldLabel, None)

        self.auth.editAcl(userGroup, oldTroveId, oldLabelId, troveId, labelId,
            write, capped, admin)

        return True

    def changePassword(self, authToken, clientVersion, user, newPassword):
        if (not self.auth.checkIsFullAdmin(authToken[0], authToken[1])
            and user != authToken[0]):
            raise errors.InsufficientPermission

        self.auth.changePassword(user, newPassword)

        return True

    def getUserGroups(self, authToken, clientVersion):
        r = self.auth.getUserGroups(authToken[0])
        return r

    def updateMetadata(self, authToken, clientVersion,
                       troveName, branch, shortDesc, longDesc,
                       urls, categories, licenses, source, language):
        branch = self.toBranch(branch)
        if not self.auth.check(authToken, write = True,
                               label = branch.label(),
                               trove = troveName):
            raise errors.InsufficientPermission

        retval = self.troveStore.updateMetadata(troveName, branch,
                                                shortDesc, longDesc,
                                                urls, categories,
                                                licenses, source,
                                                language)
        self.troveStore.commit()
        return retval

    def getMetadata(self, authToken, clientVersion,
                    troveList, language):
        metadata = {}

        # XXX optimize this to one SQL query downstream
        for troveName, branch, version in troveList:
            branch = self.toBranch(branch)
            if not self.auth.check(authToken, write = False,
                                   label = branch.label(),
                                   trove = troveName):
                raise errors.InsufficientPermission
            if version:
                version = self.toVersion(version)
            else:
                version = None
            md = self.troveStore.getMetadata(troveName, branch, version, language)
            if md:
                metadata[troveName] = md.freeze() 

        return metadata
    
    def _setupFlavorFilter(self, cu, flavorSet):
        logMe(2, flavorSet)
        cu.execute("""
        CREATE TEMPORARY TABLE
        ffFlavor(
            flavorId INTEGER,
            base STRING,
            sense INTEGER, 
            flag STRING)
        """, start_transaction = False)
        for i, flavor in enumerate(flavorSet.iterkeys()):
            flavorId = i + 1
            flavorSet[flavor] = flavorId
            for depClass in self.toFlavor(flavor).getDepClasses().itervalues():
                for dep in depClass.getDeps():
                    cu.execute("INSERT INTO ffFlavor VALUES (?, ?, ?, NULL)",
                               flavorId, dep.name, deps.FLAG_SENSE_REQUIRED,
                               start_transaction = False)
                    for (flag, sense) in dep.flags.iteritems():
                        cu.execute("INSERT INTO ffFlavor VALUES (?, ?, ?, ?)",
                                   flavorId, dep.name, sense, flag, 
                                   start_transaction = False)
        logMe(3, "created temporary table ffFlavor")       

    def _setupTroveFilter(self, cu, troveSpecs, flavorIndices):
        logMe(2)
        cu.execute("""
        CREATE TEMPORARY TABLE
        gtvlTbl(
            item STRING,
            versionSpec STRING,
            flavorId INT)
        """, start_transaction = False)
        for troveName, versionDict in troveSpecs.iteritems():
            if type(versionDict) is list:
                versionDict = dict.fromkeys(versionDict, [ None ])

            for versionSpec, flavorList in versionDict.iteritems():
                if flavorList is None:
                    cu.execute("INSERT INTO gtvlTbl VALUES (?, ?, NULL)", 
                               troveName, versionSpec, 
                               start_transaction = False)
                else:
                    for flavorSpec in flavorList:
                        if flavorSpec:
                            flavorId = flavorIndices[flavorSpec]
                        else:
                            flavorId = None
                        cu.execute("INSERT INTO gtvlTbl VALUES (?, ?, ?)", 
                                   troveName, versionSpec, flavorId, 
                                   start_transaction = False)
        cu.execute("CREATE INDEX gtblIdx on gtvlTbl(item)", 
                   start_transaction = False)
        logMe(3, "created temporary table gtvlTbl")
        
    _GTL_VERSION_TYPE_NONE = 0
    _GTL_VERSION_TYPE_LABEL = 1
    _GTL_VERSION_TYPE_VERSION = 2
    _GTL_VERSION_TYPE_BRANCH = 3

    # FIXME: this function gets always called withVersions = true. The code
    # would simplify alot if we just assumed that instead of casing it
    def _getTroveList(self, authToken, clientVersion, troveSpecs,
                      versionType = _GTL_VERSION_TYPE_NONE,
                      latestFilter = _GET_TROVE_ALL_VERSIONS, 
                      flavorFilter = _GET_TROVE_ALL_FLAVORS,
                      withFlavors = False):
        logMe(2, versionType, latestFilter, flavorFilter)
        cu = self.db.cursor()
        singleVersionSpec = None
        dropTroveTable = False

        assert(versionType == self._GTL_VERSION_TYPE_NONE or
               versionType == self._GTL_VERSION_TYPE_BRANCH or
               versionType == self._GTL_VERSION_TYPE_VERSION or
               versionType == self._GTL_VERSION_TYPE_LABEL)

        # permission check first
        if not self.auth.check(authToken):
            return {}
        
        if troveSpecs:
            # populate flavorIndices with all of the flavor lookups we
            # need. a flavor of 0 (numeric) means "None"
            flavorIndices = {}
            for versionDict in troveSpecs.itervalues():
                for flavorList in versionDict.itervalues():
                    if flavorList is not None:
                        flavorIndices.update({}.fromkeys(flavorList))
            if flavorIndices.has_key(0):
                del flavorIndices[0]
        else:
            flavorIndices = {}

        if flavorIndices:
            self._setupFlavorFilter(cu, flavorIndices)

        if not troveSpecs or (len(troveSpecs) == 1 and 
                                 troveSpecs.has_key(None) and
                                 len(troveSpecs[None]) == 1 and
                                 troveSpecs[None].has_key(None)):
            # no trove names, and/or no version spec
            troveNameClause = "Items"
            assert(versionType == self._GTL_VERSION_TYPE_NONE)
        elif len(troveSpecs) == 1 and troveSpecs.has_key(None):
            # no trove names, and a single version spec (multiple ones
            # are disallowed)
            assert(len(troveSpecs[None]) == 1)
            troveNameClause = "Items"
            singleVersionSpec = troveSpecs[None].keys()[0]
        else:
            dropTroveTable = True
            self._setupTroveFilter(cu, troveSpecs, flavorIndices)
            troveNameClause = "gtvlTbl JOIN Items using (item)"
        
        getList = [ 'Items.item', 'permittedTrove']
        if dropTroveTable:
            getList.append('gtvlTbl.flavorId')
        else:
            getList.append('0')
        argList = [ authToken[0] ]

        getList += [ 'Versions.version', 'Nodes.timeStamps', 'Nodes.branchId',
                     'Nodes.finalTimestamp' ]
        versionClause = "join Versions ON Nodes.versionId = Versions.versionId"

        # FIXME: the '%s' in the next lines are wreaking havoc through
        # cached execution plans        
        if versionType == self._GTL_VERSION_TYPE_LABEL:
            if singleVersionSpec:
                labelClause = """ JOIN Labels ON
                    Labels.labelId = LabelMap.labelId AND
                    Labels.label = '%s'""" % singleVersionSpec
            else:
                labelClause = """JOIN Labels ON
                    Labels.labelId = LabelMap.labelId AND
                    Labels.label = gtvlTbl.versionSpec"""
        elif versionType == self._GTL_VERSION_TYPE_BRANCH:
            if singleVersionSpec:
                labelClause = """JOIN Branches ON
                    Branches.branchId = LabelMap.branchId AND
                    Branches.branch = '%s'""" % singleVersionSpec
            else:
                labelClause = """JOIN Branches ON
                    Branches.branchId = LabelMap.branchId AND
                    Branches.branch = gtvlTbl.versionSpec"""
        elif versionType == self._GTL_VERSION_TYPE_VERSION:
            labelClause = ""
            if singleVersionSpec:
                vc = "Versions.version = '%s'" % singleVersionSpec
            else:
                vc = "Versions.version = gtvlTbl.versionSpec"
            versionClause = """%s AND
            %s""" % (versionClause, vc)
        else:
            assert(versionType == self._GTL_VERSION_TYPE_NONE)
            labelClause = ""

        # we establish the execution domain out into the Nodes table
        # keep in mind: "leaves" == Latest ; "all" == Instances
        if latestFilter != self._GET_TROVE_ALL_VERSIONS:
            instanceClause = """join Latest as Domain using (itemId)
            join Nodes using (itemId, branchId, versionId)"""
        else:
            instanceClause = """join Instances as Domain using (itemId)
            join Nodes using (itemId, versionid)"""

        if withFlavors:
            getList.append("Flavors.flavor")
            flavorClause = "join Flavors ON Flavors.flavorId = Domain.flavorId"
        else:
            getList.append("NULL")
            flavorClause = ""

        if flavorIndices:
            assert(withFlavors)
            if len(flavorIndices) > 1:
                # if there is only one flavor we don't need to join based on
                # the gtvlTbl.flavorId (which is good, since it may not exist)
                extraJoin = """ffFlavor.flavorId = gtvlTbl.flavorId
                      AND
                """
            else:
                extraJoin = ""

            flavorScoringClause = """
            LEFT OUTER JOIN FlavorMap ON
                FlavorMap.flavorId = Flavors.flavorId
            LEFT OUTER JOIN ffFlavor ON
                %s
                ffFlavor.base = FlavorMap.base AND
                (  ffFlavor.flag = FlavorMap.flag OR
                   ( ffFlavor.flag is NULL AND
                     FlavorMap.flag is NULL )
                )
            LEFT OUTER JOIN FlavorScores ON
                FlavorScores.present = FlavorMap.sense AND
                (    FlavorScores.request = ffFlavor.sense OR
                     ( ffFlavor.sense is NULL AND
                       FlavorScores.request = 0 )
                )
            """ % extraJoin
                        #(FlavorScores.request = ffFlavor.sense OR
                        #    (ffFlavor.sense is NULL AND
                        #     FlavorScores.request = 0)
                        #)

            grouping = """GROUP BY
            Domain.itemId, Domain.versionId, Domain.flavorId, aclId"""
            if dropTroveTable:
                grouping = grouping + ", gtvlTbl.flavorId"

            # according to some SQL standard, the SUM in the case where all 
            # values are NULL is NULL. So we use coalesce to change NULL to 0
            getList.append("SUM(coalesce(FlavorScores.value, 0)) "
                           "as flavorScore")
            flavorScoreCheck = "HAVING flavorScore > -500000"
        else:
            assert(flavorFilter == self._GET_TROVE_ALL_FLAVORS)
            flavorScoringClause = ""
            grouping = ""
            getList.append("NULL")
            flavorScoreCheck = ""

        fullQuery = """
        SELECT 
            %s
        FROM
            %s
            %s
            join LabelMap using (itemid, branchId)
            join (
               select 
                   Permissions.labelId as labelId, 
                   PerItems.item as permittedTrove,
                   Permissions._ROWID_ as aclId
               from
                   Users
                   join UserGroupMembers using (userId)
                   join Permissions using (userGroupId)
                   join Items as PerItems using (itemId)
               where
                   Users.user = ?
               ) as UP on 
                   ( UP.labelId = 0 or UP.labelId = LabelMap.labelId )
            %s
            %s
            %s
            %s
        %s
        %s
        ORDER BY Items.item, Nodes.finalTimestamp
        """ % (", ".join(getList), troveNameClause, instanceClause, 
               versionClause, labelClause, flavorClause, flavorScoringClause,
               grouping, flavorScoreCheck)
        cu.execute(fullQuery, argList)
        logMe(3, "execute query", fullQuery, argList)

        # this prevents dups that could otherwise arise from multiple
        # acl's allowing access to the same information
        allowed = {}

        troveNames = []
        troveVersions = {}

        # FIXME: Remove the ORDER BY in the sql statement above and watch it
        # CRASH and BURN. Put a "DESC" in there to return some really wrong data
        #
        # That is because the loop below is dependent on the order in
        # which this data is provided, even though it is the same
        # dataset with and without "ORDER BY" -- gafton
        for (troveName, troveNamePattern, localFlavorId, versionStr, 
             timeStamps, branchId, finalTimestamp, flavor, flavorScore) in cu:
            if flavorScore is None:
                flavorScore = 0
                
            #logMe(3, troveName, versionStr, flavor, flavorScore, finalTimestamp)
            if allowed.has_key((troveName, versionStr, flavor)):
                continue

            if not self.auth.checkTrove(troveNamePattern, troveName):
                continue

            allowed[(troveName, versionStr, flavor)] = True

            # FIXME: since troveNames is no longer traveling through
            # here, this withVersions check has become superfluous.
            # Now we're always dealing with versions -- gafton
            if latestFilter == self._GET_TROVE_VERY_LATEST:
                d = troveVersions.setdefault(troveName, {})

                if flavorFilter == self._GET_TROVE_BEST_FLAVOR:
                    flavorIdentifier = localFlavorId
                else:
                    flavorIdentifier = flavor

                lastTimestamp, lastFlavorScore = d.get(
                        (branchId, flavorIdentifier), (0, -500000))[0:2]
                # this rule implements "later is better"; we've already
                # thrown out incompatible troves, so whatever is left
                # is at least compatible; within compatible, newer
                # wins (even if it isn't as "good" as something older)

                # FIXME: this OR-based serialization sucks.
                # if the following pairs of (score, timestamp) come in the
                # order showed, we end up picking different results.
                #  (assume GET_TROVE_BEST_FLAVOR here)
                # (1, 3), (3, 2), (2, 1)  -> (3, 2)  [WRONG]
                # (2, 1) , (3, 2), (1, 3) -> (1, 3)  [RIGHT]
                #
                # XXX: this is why the row order of the SQL result matters.
                #      We ain't doing the right thing here.
                if (flavorFilter == self._GET_TROVE_BEST_FLAVOR and
                    flavorScore > lastFlavorScore) or \
                    finalTimestamp > lastTimestamp:
                    d[(branchId, flavorIdentifier)] = \
                        (finalTimestamp, flavorScore, versionStr, 
                         timeStamps, flavor)
                    #logMe(3, lastTimestamp, lastFlavorScore, d)

            elif flavorFilter == self._GET_TROVE_BEST_FLAVOR:
                assert(latestFilter == self._GET_TROVE_ALL_VERSIONS)
                assert(withFlavors)

                d = troveVersions.get(troveName, None)
                if d is None:
                    d = {}
                    troveVersions[troveName] = d

                lastTimestamp, lastFlavorScore = d.get(
                        (versionStr, localFlavorId), (0, -500000))[0:2]

                if (flavorScore > lastFlavorScore):
                    d[(versionStr, localFlavorId)] = \
                        (finalTimestamp, flavorScore, versionStr, 
                         timeStamps, flavor)
            else:
                # if _GET_TROVE_ALL_VERSIONS is used, withFlavors must
                # be specified (or the various latest versions can't
                # be differentiated)
                assert(latestFilter == self._GET_TROVE_ALL_VERSIONS)
                assert(withFlavors)

                version = versions.VersionFromString(versionStr)
                version.setTimeStamps([float(x) for x in 
                                            timeStamps.split(":")])

                d = troveVersions.get(troveName, None)
                if d is None:
                    d = {}
                    troveVersions[troveName] = d

                version = version.freeze()
                l = d.get(version, None)
                if l is None:
                    l = []
                    d[version] = l
                l.append(flavor)
        logMe(3, "extracted query results")

        if dropTroveTable:
            cu.execute("DROP TABLE gtvlTbl", start_transaction = False)

        if flavorIndices:
            cu.execute("DROP TABLE ffFlavor", start_transaction = False)

        if latestFilter == self._GET_TROVE_VERY_LATEST or \
                    flavorFilter == self._GET_TROVE_BEST_FLAVOR:
            newTroveVersions = {}
            for troveName, versionDict in troveVersions.iteritems():
                if withFlavors:
                    l = {}
                else:
                    l = []

                for (finalTimestamp, flavorScore, versionStr, timeStamps, 
                     flavor) in versionDict.itervalues():
                    version = versions.VersionFromString(versionStr)
                    version.setTimeStamps([float(x) for x in 
                                                timeStamps.split(":")])
                    version = self.freezeVersion(version)

                    if withFlavors:
                        if flavor == None:
                            flavor = "none"
                        flist = l.setdefault(version, [])
                        flist.append(flavor)
                    else:
                        l.append(version)

                newTroveVersions[troveName] = l

            troveVersions = newTroveVersions

        logMe(3, "processed troveVersions")
        return troveVersions

    def troveNames(self, authToken, clientVersion, labelStr):
        logMe(1, labelStr)
        # authenticate this user first
        if not self.auth.check(authToken):
            return {}
        username = authToken[0]        
        cu = self.db.cursor()
        # now get them troves
        args = [ username ]
        query = """
        select distinct
            Items.Item as trove, UP.pattern as pattern
        from
	    ( select 
	        Permissions.labelId as labelId, 
	        PerItems.item as pattern
	      from
	             Users
                join UserGroupMembers using (userId)
                join Permissions using (userGroupId)
                join Items as PerItems using (itemId)
	      where
	            Users.user = ?
	    ) as UP
            join LabelMap on ( UP.labelId = 0 or UP.labelId = LabelMap.labelId )
            join Items using (itemId) """
        where = [ "Items.hasTrove = 1" ]
        if labelStr:
            query = query + """
            join Labels on LabelMap.labelId = Labels.labelId """
            where.append("Labels.label = ?")
            args.append(labelStr)       
        query = """%s
        where %s
        """ % (query, " AND ".join(where))
        cu.execute(query, args)
        logMe(3, "query", query, args)
        names = set()
        for (trove, pattern) in cu:
            if not self.auth.checkTrove(pattern, trove):
                continue
            names.add(trove)
        return list(names)

    def getTroveVersionList(self, authToken, clientVersion, troveSpecs):
        logMe(1)
        troveFilter = {}

        for name, flavors in troveSpecs.iteritems():
            if len(name) == 0:
                name = None

            if type(flavors) is list:
                troveFilter[name] = { None : flavors }
            else:
                troveFilter[name] = { None : None }
            
        return self._getTroveList(authToken, clientVersion, troveFilter,
                                  withFlavors = True)

    def getTroveVersionFlavors(self, authToken, clientVersion, troveSpecs,
                               bestFlavor):
        logMe(1)
        return self._getTroveVerInfoByVer(authToken, clientVersion, troveSpecs, 
                              bestFlavor, self._GTL_VERSION_TYPE_VERSION, 
                              latestFilter = self._GET_TROVE_ALL_VERSIONS)

    def getAllTroveLeaves(self, authToken, clientVersion, troveSpecs,
                          flavorFilter = 0):
        logMe(1, troveSpecs)
        troveFilter = {}
        for name, flavors in troveSpecs.iteritems():
            if len(name) == 0:
                name = None
            if type(flavors) is list:
                troveFilter[name] = { None : flavors }
            else:
                troveFilter[name] = { None : None }
        logMe(3, troveFilter)
        # dispatch the more complex version to the old getTroveList        
        if not troveSpecs == { '' : True }:
            return self._getTroveList(authToken, clientVersion, troveFilter,
                                      latestFilter = self._GET_TROVE_VERY_LATEST,
                                      withFlavors = True)
        # faster version for the "get-all" case
        # authenticate this user first
        if not self.auth.check(authToken):
            return {}
        username = authToken[0]
        query = """
        select
            Items.item as trove, 
            Versions.version as version,
            Flavors.flavor as flavor,
            Nodes.timeStamps as timeStamps,
            UP.pattern as pattern
        from
            ( select
                Permissions.labelId as labelId,
                PerItems.item as pattern
            from
                Users
                join UserGroupMembers using(userId)
                join Permissions using(userGroupId)
                join Items as PerItems using (itemId)
            where
                Users.user = ?
            ) as UP
            join LabelMap on ( UP.labelId = 0 or UP.labelId = LabelMap.labelId )
            join Latest using (itemId, branchId)
            join Nodes using (itemId, branchId, versionId)
            join Items using (itemId),
            Flavors, Versions
        where
                Latest.flavorId = Flavors.flavorId
            and Latest.versionId = Versions.versionId
            """
        cu = self.db.cursor()
        cu.execute(query, [username,])
        logMe(3, "executing query", query, [username])
        ret = {}
        for (trove, version, flavor, timeStamps, pattern) in cu:      
            if not self.auth.checkTrove(pattern, trove):
                continue
            # NOTE: this is the "safe' way of doing it. It is very, very slow.
            # version = versions.VersionFromString(version)
            # version.setTimeStamps([float(x) for x in timeStamps.split(":")])
            # version = self.freezeVersion(version)

            # FIXME: prolly should use some standard thaw/freeze calls instead of
            # hardcoding the "%.3f" format. One day I'll learn about all these calls.
            version = versions.strToFrozen(version, [ "%.3f" % (float(x),)
                                                      for x in timeStamps.split(":") ])
            if flavor is None:
                flavor = "none"
            retname = ret.setdefault(trove, {})
            flist = retname.setdefault(version, [])
            flist.append(flavor)
        logMe(3, "finished processing results")
        return ret
        
    def _getTroveVerInfoByVer(self, authToken, clientVersion, troveSpecs, 
                              bestFlavor, versionType, latestFilter):
        logMe(2)
        hasFlavors = False
        d = {}
        for (name, labels) in troveSpecs.iteritems():
            if not name:
                name = None

            d[name] = {}
            for label, flavors in labels.iteritems():
                if type(flavors) == list:
                    d[name][label] = flavors
                    hasFlavors = True
                else:
                    d[name][label] = None

        # FIXME: Usually when we want the very latest we don't want to be
        # constrained by the "best flavor". But just testing for
        # 'latestFilter!=self._GET_TROVE_VERY_LATEST' to avoid asking for
        # BEST_FLAVOR doesn't work because there are other things being keyed
        # on this in the _getTroveList function
        #
        # some MAJOR logic rework needed here...        
        if bestFlavor and hasFlavors:
            flavorFilter = self._GET_TROVE_BEST_FLAVOR
        else:
            flavorFilter = self._GET_TROVE_ALL_FLAVORS
        return self._getTroveList(authToken, clientVersion, d, 
                                  flavorFilter = flavorFilter,
                                  versionType = versionType,
                                  latestFilter = latestFilter,
                                  withFlavors = True)

    def getTroveVersionsByBranch(self, authToken, clientVersion, troveSpecs,
                                 bestFlavor):
        logMe(1)
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_BRANCH, 
                                          self._GET_TROVE_ALL_VERSIONS)

    def getTroveLeavesByBranch(self, authToken, clientVersion, troveSpecs,
                               bestFlavor):
        logMe(1)
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_BRANCH, 
                                          self._GET_TROVE_VERY_LATEST)

    def getTroveLeavesByLabel(self, authToken, clientVersion, troveNameList, 
                              labelStr, flavorFilter = None):
        logMe(1, labelStr)
        troveSpecs = troveNameList
        bestFlavor = labelStr
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_LABEL, 
                                          self._GET_TROVE_VERY_LATEST)

    def getTroveVersionsByLabel(self, authToken, clientVersion, troveNameList, 
                              labelStr, flavorFilter = None):
        logMe(1, labelStr)
        troveSpecs = troveNameList
        bestFlavor = labelStr
        return self._getTroveVerInfoByVer(authToken, clientVersion,
                                          troveSpecs, bestFlavor,
                                          self._GTL_VERSION_TYPE_LABEL, 
                                          self._GET_TROVE_ALL_VERSIONS)

    def getFileContents(self, authToken, clientVersion, fileList):
        logMe(1)
        try:
            (fd, path) = tempfile.mkstemp(dir = self.tmpPath,
                                          suffix = '.cf-out')

            sizeList = []

            for fileId, fileVersion in fileList:
                fileVersion = self.toVersion(fileVersion)
                fileLabel = fileVersion.branch().label()
                fileId = self.toFileId(fileId)

                if not self.auth.check(authToken, write = False,
                                       label = fileLabel):
                    raise errors.InsufficientPermission

                fileObj = self.troveStore.findFileVersion(fileId)
                if fileObj is None:
                    raise errors.FileStreamNotFound((fileId, fileVersion))

                filePath = self.repos.contentsStore.hashToPath(
                    sha1helper.sha1ToString(fileObj.contents.sha1()))
                try:
                    size = os.stat(filePath).st_size
                except OSError, e:
                    raise errors.FileContentsNotFound((fileId, fileVersion))
                sizeList.append(size)
                os.write(fd, "%s %d\n" % (filePath, size))
            url = os.path.join(self.urlBase(), 
                               "changeset?%s" % os.path.basename(path)[:-4])
            return url, sizeList
        finally:
            os.close(fd)

    def getTroveLatestVersion(self, authToken, clientVersion, pkgName, 
                              branchStr):
        logMe(1)
        r = self.getTroveLeavesByBranch(authToken, clientVersion, 
                                { pkgName : { branchStr : None } },
                                True)
        if pkgName not in r:
            return 0
        elif len(r[pkgName]) != 1:
            return 0

        return r[pkgName].keys()[0]

    def getChangeSet(self, authToken, clientVersion, chgSetList, recurse, 
                     withFiles, withFileContents, excludeAutoSource):

        logMe(1)
        def _cvtTroveList(l):
            new = []
            for (name, (oldV, oldF), (newV, newF), absolute) in l:
                if oldV:
                    oldV = self.fromVersion(oldV)
                    oldF = self.fromFlavor(oldF)
                else:
                    oldV = 0
                    oldF = 0

                if newV:
                    newV = self.fromVersion(newV)
                    newF = self.fromFlavor(newF)
                else:
                    # this happens when a distributed group has a trove
                    # on a remote repository disappear
                    newV = 0
                    newF = 0

                new.append((name, (oldV, oldF), (newV, newF), absolute))

            return new

        def _cvtFileList(l):
            new = []
            for (pathId, troveName, (oldTroveV, oldTroveF, oldFileId, oldFileV), 
                                    (newTroveV, newTroveF, newFileId, newFileV)) in l:
                if oldFileV:
                    oldTroveV = self.fromVersion(oldTroveV)
                    oldFileV = self.fromVersion(oldFileV)
                    oldFileId = self.fromFileId(oldFileId)
                    oldTroveF = self.fromFlavor(oldTroveF)
                else:
                    oldTroveV = 0
                    oldFileV = 0
                    oldFileId = 0
                    oldTroveF = 0

                newTroveV = self.fromVersion(newTroveV)
                newFileV = self.fromVersion(newFileV)
                newFileId = self.fromFileId(newFileId)
                newTroveF = self.fromFlavor(newTroveF)

                pathId = self.fromPathId(pathId)

                new.append((pathId, troveName, 
                               (oldTroveV, oldTroveF, oldFileId, oldFileV),
                               (newTroveV, newTroveF, newFileId, newFileV)))

            return new

        pathList = []
        newChgSetList = []
        allFilesNeeded = []

        # XXX all of these cache lookups should be a single operation through a 
        # temporary table
	for (name, (old, oldFlavor), (new, newFlavor), absolute) in chgSetList:
	    newVer = self.toVersion(new)

	    if not self.auth.check(authToken, write = False, trove = name,
				   label = newVer.branch().label()):
		raise errors.InsufficientPermission

	    if old == 0:
		l = (name, (None, None),
			   (self.toVersion(new), self.toFlavor(newFlavor)),
			   absolute)
	    else:
		l = (name, (self.toVersion(old), self.toFlavor(oldFlavor)),
			   (self.toVersion(new), self.toFlavor(newFlavor)),
			   absolute)

            cacheEntry = self.cache.getEntry(l, recurse, withFiles, 
                                        withFileContents, excludeAutoSource)
            if cacheEntry is None:
                ret = self.repos.createChangeSet([ l ], 
                                        recurse = recurse, 
                                        withFiles = withFiles,
                                        withFileContents = withFileContents,
                                        excludeAutoSource = excludeAutoSource)

                (cs, trovesNeeded, filesNeeded) = ret

                # look up the version w/ timestamps
                primary = (l[0], l[2][0], l[2][1])
                trvCs = cs.getNewTroveVersion(*primary)
                primary = (l[0], trvCs.getNewVersion(), l[2][1])
                cs.addPrimaryTrove(*primary)

                try:
                    (key, path) = self.cache.addEntry(l, recurse, withFiles, 
                                                      withFileContents, 
                                                      excludeAutoSource,
                                                      (trovesNeeded,
                                                       filesNeeded))
                except:
                    # something went wrong.  make sure that we roll
                    # back any pending change
                    if self.cache.db.inTransaction:
                        self.cache.db.rollback()
                    raise
                size = cs.writeToFile(path, withReferences = True)
                self.cache.setEntrySize(key, size)
            else:
                path, (trovesNeeded, filesNeeded), size = cacheEntry

            newChgSetList += _cvtTroveList(trovesNeeded)
            allFilesNeeded += _cvtFileList(filesNeeded)

            pathList.append((path, size))

        (fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.cf-out')
        url = os.path.join(self.urlBase(), 
                           "changeset?%s" % os.path.basename(path[:-4]))
        f = os.fdopen(fd, 'w')
        sizes = []
        for path, size in pathList:
            sizes.append(size)
            f.write("%s %d\n" % (path, size))
        f.close()

        return url, sizes, newChgSetList, allFilesNeeded

    def getDepSuggestions(self, authToken, clientVersion, label, requiresList):
        logMe(1)
        
	if not self.auth.check(authToken, write = False, 
			       label = self.toLabel(label)):
	    raise errors.InsufficientPermission

	requires = {}
	for dep in requiresList:
	    requires[self.toDepSet(dep)] = dep

        label = self.toLabel(label)

	sugDict = self.troveStore.resolveRequirements(label, requires.keys())

        result = {}
        for (key, val) in sugDict.iteritems():
            result[requires[key]] = val
                
        return result

    def prepareChangeSet(self, authToken, clientVersion):
        logMe(1)
	# make sure they have a valid account and permission to commit to
	# *something*
	if not self.auth.check(authToken, write = True):
	    raise errors.InsufficientPermission

	(fd, path) = tempfile.mkstemp(dir = self.tmpPath, suffix = '.ccs-in')
	os.close(fd)
	fileName = os.path.basename(path)

        return os.path.join(self.urlBase(), "?%s" % fileName[:-3])

    def commitChangeSet(self, authToken, clientVersion, url):
	assert(url.startswith(self.urlBase()))
        logMe(1, url)
	# +1 strips off the ? from the query url
	fileName = url[len(self.urlBase()) + 1:] + "-in"
	path = "%s/%s" % (self.tmpPath, fileName)
	try:
	    cs = changeset.ChangeSetFromFile(path)
	finally:
	    #print path
	    os.unlink(path)

	# walk through all of the branches this change set commits to
	# and make sure the user has enough permissions for the operation
	items = {}
	for troveCs in cs.iterNewTroveList():
	    items[(troveCs.getName(), troveCs.getNewVersion())] = True
	    if not self.auth.check(authToken, write = True, 
		       label = troveCs.getNewVersion().branch().label(),
		       trove = troveCs.getName()):
		raise errors.InsufficientPermission

	self.repos.commitChangeSet(cs, self.name)

	if not self.commitAction:
	    return True

        d = { 'reppath' : self.urlBase(),
              'user' : authToken[0], }
        cmd = self.commitAction % d
        p = util.popen(cmd, "w")
	for troveCs in cs.iterNewTroveList():
            p.write("%s\n%s\n%s\n" %(troveCs.getName(),
		                     troveCs.getNewVersion().asString(),
                                     deps.formatFlavor(troveCs.getNewFlavor())))
        p.close()

	return True

    def getFileVersions(self, authToken, clientVersion, fileList):
        logMe(1)
	# XXX needs to authentication against the trove the file is part of,
	# which is unfortunate, though you have to wonder what could be so
        # special in an inode...
        r = []
        for (pathId, fileId) in fileList:
            f = self.troveStore.getFile(self.toPathId(pathId), 
                                        self.toFileId(fileId))
            r.append(self.fromFile(f))

        return r

    def getFileVersion(self, authToken, clientVersion, pathId, fileId, 
                       withContents = 0):
        logMe(1)
	# XXX needs to authentication against the trove the file is part of,
	# which is unfortunate, though you have to wonder what could be so
        # special in an inode...
	f = self.troveStore.getFile(self.toPathId(pathId), 
                                    self.toFileId(fileId))
	return self.fromFile(f)

    def getPackageBranchPathIds(self, authToken, clientVersion, sourceName, 
                                branch):
        logMe(1, sourceName, branch)
	if not self.auth.check(authToken, write = False, 
                               trove = sourceName,
			       label = self.toBranch(branch).label()):
	    raise errors.InsufficientPermission

        cu = self.db.cursor()

        query = """
            SELECT DISTINCT pathId, path, version, fileId FROM
                TroveInfo JOIN Instances using (instanceId)
                INNER JOIN Nodes using (itemId, versionId)
                INNER JOIN Branches using (branchId)
                INNER JOIN TroveFiles ON
                    Instances.instanceId = TroveFiles.instanceId
                INNER JOIN Versions using (versionId)
                INNER JOIN FileStreams ON
                    TroveFiles.streamId = FileStreams.streamId
                WHERE
                    TroveInfo.infoType = ? AND
                    TroveInfo.data = ? AND
                    Branches.branch = ?
                ORDER BY 
                    Nodes.finalTimestamp DESC
        """
        args = [trove._TROVEINFO_TAG_SOURCENAME, sourceName, branch]
        cu.execute(query, args)
        logMe(3, "execute query", query, args)

        ids = {}
        for (pathId, path, version, fileId) in cu:
            encodedPath = self.fromPath(path)
            if not encodedPath in ids:
                ids[encodedPath] = (self.fromPathId(pathId),
                                   version,
                                   self.fromFileId(fileId))
        return ids

    def getCollectionMembers(self, authToken, clientVersion, troveName, 
                                branch):
        logMe(1, troveName, branch)
	if not self.auth.check(authToken, write = False, 
                               trove = troveName,
			       label = self.toBranch(branch).label()):
	    raise errors.InsufficientPermission

        cu = self.db.cursor()
        query = """
            SELECT DISTINCT IncludedItems.item FROM
                Items, Nodes, Branches, Instances, TroveTroves,
                Instances AS IncludedInstances,
                Items AS IncludedItems
            WHERE
                Items.item = ? AND
                Items.itemId = Nodes.itemId AND
                Nodes.branchId = Branches.branchId AND
                Branches.branch = ? AND
                Instances.itemId = Nodes.itemId AND
                Instances.versionId = Nodes.versionId AND
                TroveTroves.instanceId = Instances.instanceId AND
                IncludedInstances.instanceId = TroveTroves.includedId AND
                IncludedItems.itemId = IncludedInstances.itemId
            """
        args = [troveName, branch]
        cu.execute(query, args)
        logMe(3, "execute query", query, args)
        ret = [ x[0] for x in cu ]
        return ret

    def getTrovesBySource(self, authToken, clientVersion, sourceName, 
                          sourceVersion):
        logMe(1, sourceName, sourceVersion)       
	if not self.auth.check(authToken, write = False, trove = sourceName,
                   label = self.toVersion(sourceVersion).branch().label()):
	    raise errors.InsufficientPermission

        versionMatch = sourceVersion + '-%'

        cu = self.db.cursor()
        query = """
        SELECT item, version, flavor FROM 
            TroveInfo JOIN Instances using (instanceId)
            JOIN Items using (itemId)
            JOIN Versions ON
                Instances.versionId = Versions.versionId
            JOIN Flavors ON
                Instances.flavorId = Flavors.flavorId
            WHERE
                TroveInfo.infoType = 1 AND
                TroveInfo.data = ? AND
                Versions.version LIKE ?
                """
        args = [sourceName, versionMatch]
        cu.execute(query, args)
        logMe(3, "execute query", query, args)
        matches = [ tuple(x) for x in cu ]
        return matches

    def addDigitalSignature(self, authToken, clientVersion, name, version,
                            flavor, encSig):
        logMe(1, name, version, flavor)
        version = self.toVersion(version)
        flavor = self.toFlavor(flavor)
	if not self.auth.check(authToken, write = True, trove = name,
                               label = version.branch().label()):
	    raise errors.InsufficientPermission

        trv = self.repos.getTrove(name, version, flavor)

        signature = DigitalSignature()
        signature.thaw(base64.b64decode(encSig))

        sig = signature.get()
        # ensure repo knows this key
        keyCache = self.repos.troveStore.keyTable.keyCache
        pubKey = keyCache.getPublicKey(sig[0])

        if pubKey.isRevoked():
            raise errors.IncompatibleKey('Key %s has been revoked. '
                                  'Signature rejected' %sig[0])

        if (pubKey.getTimestamp()):
            raise errors.IncompatibleKey('Key %s has expired. '
                                  'Signature rejected' %sig[0])

        #need to verify this key hasn't signed this trove already
        try:
            trv.getDigitalSignature(sig[0])
            foundSig = 1
        except KeyNotFound:
            foundSig = 0

        if foundSig:
            raise errors.AlreadySignedError("Trove already signed by key")

        trv.addPrecomputedDigitalSignature(sig)

        # verify the new signature is actually good
        trv.verifyDigitalSignatures(keyCache = keyCache)

        # start a transaction now, this ensures that queries and updates
        # are happening in a consistent way.
        self.db._begin()
        cu = self.db.cursor()
        trv = self.repos.getTrove(name, version, flavor)
        trv.addPrecomputedDigitalSignature(sig)

        # get the instanceId that corresponds to this trove.
        # FIXME: get instanceId in a better fashion.
        # XXX: I'm fairly positive this many LEFT JOINS should be considered harmful
        query = """SELECT instanceId FROM Instances
                       LEFT JOIN Items ON Items.itemId=Instances.itemId
                       LEFT JOIN Versions
                           ON Versions.versionId=Instances.versionId
                       LEFT JOIN Flavors
                           ON Flavors.flavorId=Instances.flavorId
                   WHERE item=? AND version=? AND flavor=?"""
        # if this instance is unflavored, the magic value is 'none'
        flavorStr = flavor.freeze() or 'none'
        cu.execute(query, (name, version.asString(), flavorStr))
        instanceId = cu.fetchone()[0]

        # see if there's any troveinfo in the database now
        cu.execute("""SELECT COUNT(*) FROM TroveInfo
                      WHERE instanceId=? AND infoType=9""", (instanceId,))
        if cu.fetchone()[0]:
            # if we have TroveInfo, so update it
            cu.execute("""UPDATE TroveInfo SET data=?
                          WHERE instanceId=? AND infoType=9""",
                       (trv.troveInfo.sigs.freeze(), instanceId))
        else:
            # otherwise we need to create a new row with the signatures
            cu.execute('INSERT INTO TroveInfo VALUES(?, 9, ?)',
                       (instanceId, trv.troveInfo.sigs.freeze()))
        self.cache.invalidateEntry(trv.getName(), trv.getVersion(),
                                   trv.getFlavor())
        self.db.commit()
        return True

    def addNewAsciiPGPKey(self, authToken, label, user, keyData):
        if (not self.auth.checkIsFullAdmin(authToken[0], authToken[1])
            and user != authToken[0]):
            raise errors.InsufficientPermission
        uid = self.auth.getUserIdByName(user)
        self.repos.troveStore.keyTable.addNewAsciiKey(uid, keyData)
        return True

    def addNewPGPKey(self, authToken, label, user, encKeyData):
        import base64
        if (not self.auth.checkIsFullAdmin(authToken[0], authToken[1])
            and user != authToken[0]):
            raise errors.InsufficientPermission
        uid = self.auth.getUserIdByName(user)
        keyData = base64.b64decode(encKeyData)
        self.repos.troveStore.keyTable.addNewKey(uid, keyData)
        return True

    def changePGPKeyOwner(self, authToken, label, user, key):
        if (not self.auth.checkIsFullAdmin(*authToken)):
            raise errors.InsufficientPermission
        if user:
            uid = self.auth.getUserIdByName(user)
        else:
            uid = None
        self.repos.troveStore.keyTable.updateOwner(uid, key)

    def getAsciiOpenPGPKey(self, authToken, label, keyId):
        # don't check auth. this is a public function
        return self.repos.troveStore.keyTable.getAsciiPGPKeyData(keyId)

    def listUsersMainKeys(self, authToken, label, userId = None):
        # the only reason to lock this fuction down is because it correlates
        # a valid userId to valid fingerprints. neither of these pieces of
        # information is sensitive separately.
        if (not self.auth.checkIsFullAdmin(authToken[0], authToken[1])
            and userId != self.auth.getUserIdByName(authToken[0])):
            raise errors.InsufficientPermission
        return self.repos.troveStore.keyTable.getUsersMainKeys(userId)

    def listSubkeys(self, authToken, label, fingerprint):
        return self.repos.troveStore.keyTable.getSubkeys(fingerprint)

    def getOpenPGPKeyUserIds(self, authToken, label, keyId):
        return self.repos.troveStore.keyTable.getUserIds(keyId)

    def getConaryUrl(self, authtoken, clientVersion, \
                     revStr, flavorStr):
        """
        Returns a url to a downloadable changeset for the conary
        client that is guaranteed to work with this server's version.
        """       
        # adjust accordingly.... all urls returned are relative to this
        _baseUrl = "ftp://download.rpath.com/conary/"
        # Note: if this hash is getting too big, we will switch to a
        # database table. The "default" entry is a last resort.
        _clientUrls = {
            # revision { flavor : relative path }
            ## "default" : { "is: x86"    : "conary.x86.ccs",
            ##               "is: x86_64" : "conary.x86_64.ccs", }
            }            
        logMe(3, revStr, flavorStr)
        rev = versions.Revision(revStr)
        revision = rev.getVersion()
        flavor = self.toFlavor(flavorStr)
        ret = ""
        bestMatch = -1000000
        match = _clientUrls.get("default", {})
        if _clientUrls.has_key(revision):
            match = _clientUrls[revision]        
        for mStr in match.keys():
            mFlavor = deps.parseFlavor(mStr)
            score = mFlavor.score(flavor)
            if score is False:
                continue
            if score > bestMatch:
                ret = match[mStr]
        if len(ret):
            return "%s/%s" % (_baseUrl, ret)
        return ""
    
    def checkVersion(self, authToken, clientVersion):
	if not self.auth.check(authToken, write = False):
	    raise errors.InsufficientPermission

        # cut off older clients entirely, no negotiation
        if clientVersion < SERVER_VERSIONS[0]:
            raise errors.InvalidClientVersion(
               'Invalid client version %s.  Server accepts client versions %s '
               '- read http://wiki.conary.com/ConaryConversion' % 
               (clientVersion, ', '.join(str(x) for x in SERVER_VERSIONS)))

        return SERVER_VERSIONS

    def cacheChangeSets(self):
        return isinstance(self.cache, CacheSet)

    def versionCheck(self):
        logMe(3)
        cu = self.db.cursor()
        count = cu.execute("SELECT COUNT(*) FROM sqlite_master WHERE "
                           "name='DatabaseVersion'").next()[0]
        if count == 0:
            # if DatabaseVersion does not exist, but any other tables do exist,
            # then the database version is old
            count = cu.execute("SELECT count(*) FROM sqlite_master").next()[0]
            if count:
                return False

            cu.execute("CREATE TABLE DatabaseVersion (version INTEGER)",
		       start_transaction = False)
            cu.execute("INSERT INTO DatabaseVersion VALUES (?)", 
                       self.schemaVersion, start_transaction = False)
        else:
            version = cu.execute("SELECT * FROM DatabaseVersion").next()[0]
            if version == 1:
                #This is the update from using Null as the wildcard for 
                #Items/Troves and Labels to using 0/ALL

                ## First insert the new Item and Label keys
                cu.execute("INSERT INTO Items VALUES(0, 'ALL')")
                cu.execute("INSERT INTO Labels VALUES(0, 'ALL')")

                ## Now replace all Nulls in the following tables with '0'
                itemTables =   ('Permissions', 'Instances', 'Latest', 
                                'Metadata', 'Nodes', 'LabelMap')
                labelTables =  ('Permissions', 'LabelMap')
                for table in itemTables:
                    cu.execute('UPDATE %s SET itemId=0 WHERE itemId IS NULL' % 
                        table)
                for table in labelTables:
                    cu.execute('UPDATE %s SET labelId=0 WHERE labelId IS NULL' %
                        table)

                ## Finally fix the index
                cu.execute("DROP INDEX PermissionsIdx")
                cu.execute("""CREATE UNIQUE INDEX PermissionsIdx ON 
                    Permissions(userGroupId, labelId, itemId)""")
                cu.execute("UPDATE DatabaseVersion SET version=2")
                self.db.commit()
                version = 2

            # migration to version 3
            # -- add a smaller index for the Latest table
            if version == 2:
                cu.execute("CREATE INDEX LatestItemIdx on Latest(itemId)")
                cu.execute("UPDATE DatabaseVersion SET version=3")
                self.db.commit()
                version = 3

            # migration to schema version 4
            if version == 3:
                from lib.tracelog import printErr
                msg = """
                Conversion to version 4 requires script available
                from http://wiki.rpath.com/ConaryConversion
                """
                printErr(msg)
                print msg
                return False

            # schema version 5 adds a few views and various cleanups
            if version == 4:
                logMe(3, "migrating schema from version", version)
                # FlavorScoresIdx was not unique
                cu.execute("DROP INDEX FlavorScoresIdx")
                cu.execute("CREATE UNIQUE INDEX FlavorScoresIdx "
                           "    on FlavorScores(request, present)")
                # remove redundancy/rename                
                cu.execute("DROP INDEX NodesIdx")
                cu.execute("DROP INDEX NodesIdx2")
                cu.execute("""CREATE UNIQUE INDEX NodesItemBranchVersionIdx
                                  ON Nodes(itemId, branchId, versionId)""")
                cu.execute("""CREATE INDEX NodesItemVersionIdx
                                  ON Nodes(itemId, versionId)""")
                # the views are added by the __init__ methods of their
                # respective classes
                cu.execute("UPDATE DatabaseVersion SET version=5")
                self.db.commit()
                version = 5

            if version == 5:
                logMe(3, "migrating schema from version", version)
                # calculate path hashes for every trove
                instanceIds = [ x[0] for x in cu.execute(
                        "select instanceId from instances") ]
                for i, instanceId in enumerate(instanceIds):
                    ph = trove.PathHashes()
                    for path, in cu.execute(
                            "select path from trovefiles where instanceid=?",
                            instanceId):
                        ph.addPath(path)
                    cu.execute("""
                        insert into troveinfo(instanceId, infoType, data)
                            values(?, ?, ?)""", instanceId,
                            trove._TROVEINFO_TAG_PATH_HASHES, ph.freeze())

                # add a hasTrove flag to the Items table for various 
                # optimizations
                # update the Items table                
                cu.execute(" ALTER TABLE Items ADD COLUMN "
                           " hasTrove INTEGER NOT NULL DEFAULT 0 ")
                cu.execute("""
                UPDATE Items SET hasTrove = 1
                WHERE Items.itemId IN (
                    SELECT Instances.itemId FROM Instances
                    WHERE Instances.isPresent = 1 ) """)

                cu.execute("UPDATE DatabaseVersion SET version=6")
                self.db.commit()
                version = 6
                logMe(3, "finished migrating schema to version", version)

            if version == 6:
                logMe(3, "migrating schema from version", version)

                # erase signatures due to troveInfo storage changes
                cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                           trove._TROVEINFO_TAG_SIGS)
                # erase what used to be isCollection, to be replaced
                # with flags stream
                cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                           trove._TROVEINFO_TAG_FLAGS)
                # get rid of install buckets
                cu.execute("DELETE FROM TroveInfo WHERE infoType=?",
                           trove._TROVEINFO_TAG_INSTALLBUCKET)

                flags = trove.TroveFlagsStream()
                flags.isCollection(set = True)
                collectionStream = flags.freeze()
                flags.isCollection(set = False)
                notCollectionStream = flags.freeze()

                cu.execute("""
                    INSERT INTO TroveInfo
                        (instanceId, infoType, data)
                    SELECT
                        instanceId, ?, ?
                    FROM
                        Items, Instances
                    WHERE
                            NOT (item LIKE '%:%' OR item LIKE 'fileset-%')
                        AND Items.itemId = Instances.itemId
                    """, (trove._TROVEINFO_TAG_FLAGS, collectionStream))

                cu.execute("""
                    INSERT INTO TroveInfo
                        (instanceId, infoType, data)
                    SELECT
                        instanceId, ?, ?
                    FROM
                        Items, Instances
                    WHERE
                            (item LIKE '%:%' OR item LIKE 'fileset-%')
                        AND Items.itemId = Instances.itemId
                    """, (trove._TROVEINFO_TAG_FLAGS, notCollectionStream))

                cu.execute("UPDATE DatabaseVersion SET version=7")
                self.db.commit()
                version = 7
                logMe(3, "finished migrating schema to version", version)

            if version != self.schemaVersion:
                return False

        return True

    def open(self):
        logMe(1)
	if self.troveStore is not None:
	    self.close()

        self.db = sqlite3.connect(self.sqlDbPath, timeout=30000)
	if not self.versionCheck():
	    raise SchemaVersion

	self.troveStore = trovestore.TroveStore(self.db)
	sb = os.stat(self.sqlDbPath)
	self.sqlDeviceInode = (sb.st_dev, sb.st_ino)

        self.repos = fsrepos.FilesystemRepository(self.name, self.troveStore,
                                                  self.repPath, self.map,
                                                  logFile = self.logFile,
                                                  requireSigs = self.requireSigs)
	self.auth = NetworkAuthorization(self.db, self.name)

    def reopen(self):
        logMe(1)
	sb = os.stat(self.sqlDbPath)

	sqlDeviceInode = (sb.st_dev, sb.st_ino)
	if self.sqlDeviceInode != sqlDeviceInode:
	    del self.troveStore
            del self.auth
            del self.repos
	    # self.db doesn't seem to be getting gc'd (and closed) properly
	    # here, so close it explicitly
	    self.db.close()
            del self.db

            self.db = sqlite3.connect(self.sqlDbPath, timeout=30000)
	    if not self.versionCheck():
		raise SchemaVersion
	    self.troveStore = trovestore.TroveStore(self.db)

	    sb = os.stat(self.sqlDbPath)
	    self.sqlDeviceInode = (sb.st_dev, sb.st_ino)

            self.repos = fsrepos.FilesystemRepository(self.name, 
                                                      self.troveStore,
                                                      self.repPath, self.map,
                                                      logFile = self.logFile)
            self.auth = NetworkAuthorization(self.db, self.name)

    def __init__(self, path, tmpPath, basicUrl, name,
		 repositoryMap, commitAction = None, cacheChangeSets = False,
                 logFile = None, requireSigs = False):
	self.map = repositoryMap
	self.repPath = path
	self.tmpPath = tmpPath
	self.basicUrl = basicUrl
	self.name = name
	self.commitAction = commitAction
        self.sqlDbPath = self.repPath + '/sqldb'
        self.troveStore = None
        self.logFile = logFile
        self.requireSigs = requireSigs

        logMe(1, path, basicUrl, name)
	try:
	    util.mkdirChain(self.repPath)
	except OSError, e:
	    raise errors.OpenError(str(e))

        if cacheChangeSets:
            self.cache = CacheSet(path + "/cache.sql", tmpPath, 
                                  CACHE_SCHEMA_VERSION)
        else:
            self.cache = NullCacheSet(tmpPath)

        self.open()

class NullCacheSet:
    def getEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource):
        return None 

    def addEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource, returnVal):
        (fd, path) = tempfile.mkstemp(dir = self.tmpPath, 
                                      suffix = '.ccs-out')
        os.close(fd)
        return None, path

    def setEntrySize(self, row, size):
        pass

    def invalidateEntry(self, name, version, flavor):
        pass

    def __init__(self, tmpPath):
        self.tmpPath = tmpPath

class CacheSet:

    filePattern = "%s/cache-%s.ccs-out"

    def getEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource):
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
            SELECT row, returnValue, size FROM CacheContents WHERE
                troveName=? AND
                oldFlavorId=? AND oldVersionId=? AND
                newFlavorId=? AND newVersionId=? AND
                absolute=? AND recurse=? AND withFiles=?  
                AND withFileContents=? AND excludeAutoSource=?
            """, (name, oldFlavorId, oldVersionId, newFlavorId, 
                  newVersionId, absolute, recurse, withFiles, withFileContents,
                  excludeAutoSource))

        # since we begin and commit a transaction inside the loop
        # over the returned rows, we must use fetchall() here so that we
        # release our read lock.
        for (row, returnVal, size) in cu.fetchall():
            path = self.filePattern % (self.tmpDir, row)
            # if we have no size or we can't access the file, it's
            # bad entry.  delete it.
            if not size or not os.access(path, os.R_OK):
                cu.execute("DELETE FROM CacheContents WHERE row=?", row)
                self.db.commit()
                continue
            return (path, cPickle.loads(returnVal), size)

        return None

    def addEntry(self, item, recurse, withFiles, withFileContents,
                 excludeAutoSource, returnVal):
        (name, (oldVersion, oldFlavor), (newVersion, newFlavor), absolute) = \
            item

        oldVersionId = 0
        oldFlavorId = 0
        newFlavorId = 0

        # start a transaction now to avoid race conditions when getting
        # or adding IDs for versions and flavors
        self.db._begin()

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
            INSERT INTO CacheContents VALUES(NULL, ?, ?, ?, ?, ?, ?, 
                                             ?, ?, ?, ?, ?, NULL)
        """, name, oldFlavorId, oldVersionId, newFlavorId, newVersionId, 
             absolute, recurse, withFiles, withFileContents, 
             excludeAutoSource, cPickle.dumps(returnVal, protocol = -1))

        row = cu.lastrowid
        path = self.filePattern % (self.tmpDir, row)

        self.db.commit()

        return (row, path)

    def invalidateEntry(self, name, version, flavor):
        """
        invalidates (and deletes) any cached changeset that matches
        the given name, version, flavor.
        """
        flavorId = self.flavors.get(flavor, None)
        versionId = self.versions.get(version, None)

        if flavorId is None or versionId is None:
            # this should not happen, but we'll handle it anyway
            return

        cu = self.db.cursor()
        # start a transaction to retain a consistent state
        self.db._begin()
        cu.execute("""
        SELECT row, returnValue, size
        FROM CacheContents
        WHERE troveName=? AND newFlavorId=? AND newVersionId=?
        """, (name, flavorId, versionId))
        
        # delete all matching entries from the db and the file system
        for (row, returnVal, size) in cu.fetchall():
            cu.execute("DELETE FROM CacheContents WHERE row=?", row)
            path = self.filePattern % (self.tmpDir, row)
            if os.path.exists(path):
                try:
                    os.unlink(path)
                except OSError:
                    pass
        self.db.commit()

    def setEntrySize(self, row, size):
        cu = self.db.cursor()
        cu.execute("UPDATE CacheContents SET size=? WHERE row=?", size, row)
        self.db.commit()

    def createSchema(self, dbpath, schemaVersion):
	self.db = sqlite3.connect(dbpath, timeout = 30000)
        self.db._begin()
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if "CacheContents" in tables:
            cu.execute("SELECT version FROM CacheVersion")
            version = cu.next()[0]
            if version != schemaVersion:
                cu.execute("SELECT row from CacheContents")
                for (row,) in cu:
                    fn = self.filePattern % (self.tmpDir, row)
                    if os.path.exists(fn):
                        try:
                            os.unlink(fn)
                        except OSError:
                            pass

                self.db.close()
                try:
                    os.unlink(dbpath)
                except OSError:
                    pass
                self.db = sqlite3.connect(dbpath, timeout = 30000)
                tables = []

        if "CacheContents" not in tables:
            cu.execute("""
            CREATE TABLE CacheContents(
               row              INTEGER PRIMARY KEY,
               troveName        STRING,
               oldFlavorId      INTEGER,
               oldVersionId     INTEGER,
               newFlavorId      INTEGER,
               newVersionId     INTEGER,
               absolute         BOOLEAN,
               recurse          BOOLEAN,
               withFiles        BOOLEAN,
               withFileContents BOOLEAN,
               excludeAutoSource BOOLEAN,
               returnValue      BINARY,
               size             INTEGER
            )""")
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
        self.flavors = sqldb.Flavors(self.db)
        self.versions = versiontable.VersionTable(self.db)
        self.db.commit()

class ClosedRepositoryServer(xmlshims.NetworkConvertors):
    def callWrapper(self, *args):
        return (True, ("RepositoryClosed", self.closedMessage))

    def __init__(self, closedMessage):
        self.closedMessage = closedMessage

class SchemaVersion(Exception):
    pass
