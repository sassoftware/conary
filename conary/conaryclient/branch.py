#
# Copyright (c) 2005 rPath, Inc.
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

from conary.deps import deps
from conary.repository import changeset
from conary import errors
from conary import versions

class BranchError(errors.ClientError):
    pass

class ClientBranch:
    BRANCH_SOURCE = 1 << 0
    BRANCH_BINARY = 1 << 1
    BRANCH_ALL = BRANCH_SOURCE | BRANCH_BINARY

    def createBranchChangeSet(self, newLabel, 
                              troveList = [], branchType=BRANCH_ALL):
        return self._createBranchOrShadow(newLabel, troveList, shadow = False, 
                                          branchType = branchType)

    def createShadowChangeSet(self, newLabel, troveList = [], 
                              branchType=BRANCH_ALL):
        return self._createBranchOrShadow(newLabel, troveList, shadow = True, 
                                          branchType = branchType)

    def _checkForLaterShadows(self, newLabel, troves):
        # check to see if we've already shadowed any versions later than
        # these versions to newLabel.
        query = {}
        for trove in troves:
            versionDict = query.setdefault(trove.getName(), {})
            b = trove.getVersion().branch().createShadow(newLabel)
            versionDict[b] = None
        # get the latest version of the new branches
        results = self.repos.getTroveLeavesByBranch(query)

        if not results:
            return []

        oldTroves = []
        for trove in troves:
            versionDict = results.get(trove.getName(), {})
            b = trove.getVersion().branch().createShadow(newLabel)
            versionList = [ x for x in versionDict if x.branch() == b and not x.isModifiedShadow() ]
            if not versionList:
                continue
            latestVersion = max(versionList)
            oldVersion = latestVersion.parentVersion()
            # now get the upstream timeStamps associated with the already
            # shadowed versions.
            oldTroves.extend((trove.getName(), oldVersion, x) \
                             for x in versionDict[latestVersion])

        shadowedTroves = self.repos.getTroves(oldTroves, withFiles=False)

        shadowed = {}
        for shadowedTrove in shadowedTroves:
            (n,v,f) = shadowedTrove.getNameVersionFlavor()
            shadowed[n, v.branch(), f] = v

        laterShadows = []
        for trove in troves:
            (n,v,f) = trove.getNameVersionFlavor()
            shadowedVer = shadowed.get((n, v.branch(), f), None)
            if not shadowedVer:
                continue
            if v < shadowedVer:
                # the version we shadowed before had a later timestamp
                # than the version we're shadowing now.
                laterShadows.append((n, v, f, shadowedVer))
        return laterShadows

    def _createBranchOrShadow(self, newLabel, troveList, shadow,
                              branchType=BRANCH_ALL):
        cs = changeset.ChangeSet()
        
        seen = set(troveList)
        dupList = []
        needsCommit = False

        newLabel = versions.Label(newLabel)

	while troveList:
            leavesByLabelOps = {}

            troves = self.repos.getTroves(troveList)
            troveList = set()
            branchedTroves = {}

            if shadow:
                laterShadows = self._checkForLaterShadows(newLabel, troves)
                # we disallow shadowing an earlier trove if a later
                # later one has already been shadowed - it makes our
                # cvc merge algorithm not work well.
                if laterShadows:
                    msg = []
                    for n, v, f, shadowedVer in laterShadows:
                        msg.append('''\
Cannot shadow backwards - already shadowed
    %s=%s[%s]
cannot shadow earlier trove
    %s=%s[%s]
''' % (n, shadowedVer, f, n, v, f))
                    raise BranchError('\n\n'.join(msg))

	    for trove in troves:
                # add contained troves to the todo-list
                newTroves = [ x for x in 
                        trove.iterTroveList(strongRefs=True,
                                            weakRefs=True) if x not in seen ]
                troveList.update(newTroves)
                seen.update(newTroves)

                troveName = trove.getName()

                if troveName.endswith(':source'):
                    if not(branchType & self.BRANCH_SOURCE):
                        continue

                elif branchType & self.BRANCH_SOURCE:
                    # name doesn't end in :source - if we want 
                    # to shadow the listed troves' sources, do so now

                    # XXX this can go away once we don't care about
                    # pre-troveInfo troves
                    if not trove.getSourceName():
                        from conary.lib import log
                        log.warning('%s has no source information' % troveName)
                        sourceName = troveName
                    else:
                        sourceName = trove.getSourceName()

                    key  = (sourceName, 
                            trove.getVersion().getSourceVersion(),
                            deps.DependencySet())
                    if key not in seen:
                        troveList.add(key)
                        seen.add(key)

                    if not(branchType & self.BRANCH_BINARY):
                        continue

                if shadow:
                    branchedVersion = trove.getVersion().createShadow(newLabel)
                else:
                    branchedVersion = trove.getVersion().createBranch(newLabel,
                                                               withVerRel = 1)

                branchedTrove = trove.copy()
		branchedTrove.changeVersion(branchedVersion)
                #this clears the digital signatures from the shadow
                branchedTrove.troveInfo.sigs.reset()
                # FIXME we should add a new digital signature in cases
                # where we can (aka user is at kb and can provide secret key

		for ((name, version, flavor), byDefault, isStrong) \
                                            in trove.iterTroveListInfo():
                    if shadow:
                        branchedVersion = version.createShadow(newLabel)
                    else:
                        branchedVersion = version.createBranch(newLabel,
                                                               withVerRel = 1)
		    branchedTrove.delTrove(name, version, flavor,
                                           missingOkay = False,
                                           weakRef=not isStrong)
		    branchedTrove.addTrove(name, branchedVersion, flavor,
                                           byDefault=byDefault, 
                                           weakRef=not isStrong)

                key = (trove.getName(), branchedTrove.getVersion(),
                       trove.getFlavor())
                branchedTroves[key] = branchedTrove.diff(None,
                                                         absolute = True)[0]

            # check for duplicates
            hasTroves = self.repos.hasTroves(branchedTroves)

            queryDict = {}
            for (name, version, flavor), troveCs in branchedTroves.iteritems():
                if hasTroves[name, version, flavor]:
                    dupList.append((name, version.branch()))
                else:
                    cs.newTrove(troveCs)
                    cs.addPrimaryTrove(name, version, flavor)
                    needsCommit = True

        if not needsCommit:
            cs = None 

	return dupList, cs

