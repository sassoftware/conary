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
import os
import re
import sys
import tempfile
import time

from conary import files, trove, versions
from conary.conarycfg import CfgRepoMap
from conary.deps import deps
from conary.lib import log, sha1helper, util
from conary.lib.cfg import *
from conary.repository import changeset, errors, xmlshims
from conary.repository.netrepos import fsrepos, trovestore
from conary.lib.openpgpfile import KeyNotFound, BadSelfSignature, IncompatibleKey
from conary.lib.openpgpfile import TRUST_FULL
from conary.lib.openpgpkey import getKeyCache
from conary.lib.tracelog import logMe
from conary.repository.netrepos.netauth import NetworkAuthorization
from conary.repository import repository
from conary.trove import DigitalSignature
from conary.repository.netrepos import schema, cacheset
from conary import dbstore
from conary.dbstore import idtable, sqlerrors

# a list of the protocol versions we understand. Make sure the first
# one in the list is the lowest protocol version we support and th
# last one is the current server protocol version
SERVER_VERSIONS = [ 36, 37 ]

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

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
            self.db.rollback()
	    if not e.troveName:
		return (True, ("TroveMissing", "", ""))
	    elif not e.version:
		return (True, ("TroveMissing", e.troveName, ""))
	    else:
		return (True, ("TroveMissing", e.troveName,
			self.fromVersion(e.version)))
        except errors.IntegrityError, e:
            self.db.rollback()
            return (True, ('IntegrityError', str(e)))
	except trove.TroveIntegrityError, e:
            self.db.rollback()
            return (True, ("TroveIntegrityError", str(e) +
                           # add a helpful error message for now
                        ' (you may need to update to conary 0.62.12 or later)'))
        except errors.FileContentsNotFound, e:
            self.db.rollback()
            return (True, ('FileContentsNotFound', self.fromFileId(e.val[0]),
                           self.fromVersion(e.val[1])))
        except errors.FileStreamNotFound, e:
            self.db.rollback()
            return (True, ('FileStreamNotFound', self.fromFileId(e.val[0]),
                           self.fromVersion(e.val[1])))
        except sqlerrors.DatabaseLocked:
            self.db.rollback()
            return (True, ('RepositoryLocked'))
	except Exception, e:
            self.db.rollback()
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
        # FIXME: avoid starting a transaction when using sqlite
        cu.execute("""
        CREATE TEMPORARY TABLE
        ffFlavor(
            flavorId INTEGER,
            base STRING,
            sense INTEGER,
            flag STRING)
        """)
        for i, flavor in enumerate(flavorSet.iterkeys()):
            flavorId = i + 1
            flavorSet[flavor] = flavorId
            for depClass in self.toFlavor(flavor).getDepClasses().itervalues():
                for dep in depClass.getDeps():
                    # FIXME: start_transaction = False for sqlite
                    cu.execute("INSERT INTO ffFlavor VALUES (?, ?, ?, NULL)",
                               flavorId, dep.name, deps.FLAG_SENSE_REQUIRED)
                    for (flag, sense) in dep.flags.iteritems():
                        # FIXME: start_transaction = False for sqlite
                        cu.execute("INSERT INTO ffFlavor VALUES (?, ?, ?, ?)",
                                   flavorId, dep.name, sense, flag)
        cu.execute("select count(*) from ffFlavor")
        entries = cu.next()[0]
        logMe(3, "created temporary table ffFlavor", entries)

    def _setupTroveFilter(self, cu, troveSpecs, flavorIndices):
        logMe(2)
        # FIXME: start_transaction = False for sqlite
        cu.execute("""
        CREATE TEMPORARY TABLE
        gtvlTbl(
            item STRING,
            versionSpec STRING,
            flavorId INT)
        """)
        for troveName, versionDict in troveSpecs.iteritems():
            if type(versionDict) is list:
                versionDict = dict.fromkeys(versionDict, [ None ])

            for versionSpec, flavorList in versionDict.iteritems():
                if flavorList is None:
                    # FIXME: start_transaction = False for sqlite
                    cu.execute("INSERT INTO gtvlTbl VALUES (?, ?, NULL)",
                               troveName, versionSpec)
                else:
                    for flavorSpec in flavorList:
                        if flavorSpec:
                            flavorId = flavorIndices[flavorSpec]
                        else:
                            flavorId = None
                        # FIXME: start_transaction = False for sqlite
                        cu.execute("INSERT INTO gtvlTbl VALUES (?, ?, ?)",
                                   troveName, versionSpec, flavorId)
        # FIXME: start_transaction = False for sqlite
        cu.execute("CREATE INDEX gtblIdx on gtvlTbl(item)")
        cu.execute("select count(*) from gtvlTbl")
        entries = cu.next()[0]
        logMe(3, "created temporary table gtvlTbl", entries)

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
                   ( ffFlavor.flag is NULL AND FlavorMap.flag is NULL )
                )
            LEFT OUTER JOIN FlavorScores ON
                FlavorScores.present = FlavorMap.sense AND
                (    FlavorScores.request = ffFlavor.sense OR
                     ( ffFlavor.sense is NULL AND FlavorScores.request = 0 )
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

        # FIXME: start_transaction = False for sqlite
        if dropTroveTable:
            cu.execute("DROP TABLE gtvlTbl")
        if flavorIndices:
            cu.execute("DROP TABLE ffFlavor")

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

        if (pubKey.getTimestamp()) and (pubKey.getTimestamp() < time.time()):
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

        cu = self.db.transaction()
        # get the instanceId that corresponds to this trove.
        # if this instance is unflavored, the magic value is 'none'
        flavorStr = flavor.freeze() or 'none'
        cu.execute("SELECT flavorId from Flavors WHERE flavor=?",
                   flavorStr)
        flavorId = cu.fetchone()[0]
        cu.execute("SELECT versionId FROM Versions WHERE version=?",
                   version.asString())
        versionId = cu.fetchone()[0]
        cu.execute("SELECT itemId from Items WHERE item=?", name)
        itemId = cu.fetchone()[0]

        cu.execute("""SELECT instanceId FROM Instances
                      WHERE itemId=? AND versionId=? AND flavorId=?""",
                   itemId, versionId, flavorId)
        instanceId = cu.fetchone()[0]

        # see if there's currently any troveinfo in the database
        cu.execute("""SELECT COUNT(*) FROM TroveInfo
                          WHERE instanceId=? AND infoType=9""", (instanceId,))
        trvInfo = cu.fetchone()[0]
        # start a transaction now. ensures simultaneous signatures by separate
        # clients won't cause a race condition.
        try:
            # add the signature while it's protected, to ensure no collissions
            trv = self.repos.getTrove(name, version, flavor)
            trv.addPrecomputedDigitalSignature(sig)
            if trvInfo:
                # we have TroveInfo, so update it
                cu.execute("""UPDATE TroveInfo SET data=?
                              WHERE instanceId=? AND infoType=9""",
                           (trv.troveInfo.sigs.freeze(), instanceId))
            else:
                # otherwise we need to create a new row with the signatures
                cu.execute('INSERT INTO TroveInfo VALUES(?, 9, ?)',
                           (instanceId, trv.troveInfo.sigs.freeze()))
            self.cache.invalidateEntry(trv.getName(), trv.getVersion(),
                                       trv.getFlavor())
        except:
            self.db.rollback()
            raise
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
        return isinstance(self.cache, cacheset.CacheSet)

    # XXX: database handle should be pushed into the global namespace
    # to avoid connects/disconnects on every request processed.
    def open(self):
        logMe(1)
        # XXX: don't hardcode the driver (requires config file update)
        self.db = dbstore.connect(self.sqlDbPath, driver = "sqlite")
        schema.checkVersion(self.db)
	self.troveStore = trovestore.TroveStore(self.db)
        self.repos = fsrepos.FilesystemRepository(
            self.name, self.troveStore, self.repPath, self.map,
            logFile = self.logFile, requireSigs = self.requireSigs)
	self.auth = NetworkAuthorization(self.db, self.name)

    def reopen(self):
        logMe(1)
        if self.db.reopen():
	    del self.troveStore
            del self.auth
            del self.repos
            self.open()

    # FIXME - sqlite-ism: stop assuming databases live on pathnames...
    def __init__(self, cfg, basicUrl):
	self.map = cfg.repositoryMap
	self.repPath = cfg.repositoryDir
	self.tmpPath = cfg.tmpDir
	self.basicUrl = basicUrl
	self.name = cfg.serverName
	self.commitAction = cfg.commitAction
        # FIXME: sqlite-ism - database shouldn't be assumed a pathname
        self.sqlDbPath = self.repPath + '/sqldb'
        self.troveStore = None
        self.logFile = cfg.logFile
        self.requireSigs = cfg.requireSigs

        logMe(1, basicUrl)
	try:
	    util.mkdirChain(self.repPath)
	except OSError, e:
	    raise errors.OpenError(str(e))

        if cfg.cacheChangeSets:
            self.cache = cacheset.CacheSet(path + "/cache.sql", tmpPath)
        else:
            self.cache = cacheset.NullCacheSet(self.tmpPath)

        self.open()


class ClosedRepositoryServer(xmlshims.NetworkConvertors):
    def callWrapper(self, *args):
        return (True, ("RepositoryClosed", self.closedMessage))

    def __init__(self, closedMessage):
        self.closedMessage = closedMessage

class ServerConfig(ConfigFile):
    cacheChangeSets         = CfgBool
    closed                  = CfgString
    commitAction            = CfgString
    forceSSL                = CfgBool
    logFile                 = CfgPath
    repositoryDir           = CfgString
    repositoryMap           = CfgRepoMap
    requireSigs             = CfgBool
    serverName              = CfgString
    tmpDir                  = (CfgPath, '/var/tmp')
