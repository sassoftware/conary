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


from conary.deps import deps
from conary.repository import changeset
from conary.repository import errors as repoerrors
from conary import errors
from conary import versions
from conary import trove

class BranchError(errors.ClientError):
    pass

class ClientBranch(object):
    BRANCH_SOURCE = 1 << 0
    BRANCH_BINARY = 1 << 1
    BRANCH_ALL = BRANCH_SOURCE | BRANCH_BINARY

    __developer_api__ = True

    def createBranchChangeSet(self, newLabel,
                              troveList = [], branchType=BRANCH_ALL,
                              sigKeyId = None):
        return self._createBranchOrShadow(newLabel, troveList, shadow = False,
                                          branchType = branchType,
                                          sigKeyId = sigKeyId)

    def createShadowChangeSet(self, newLabel, troveList = [],
                              branchType=BRANCH_ALL,
                              sigKeyId=None,
                              allowEmptyShadow=False):
        return self._createBranchOrShadow(newLabel, troveList, shadow=True,
                                          branchType=branchType,
                                          sigKeyId=sigKeyId,
                                          allowEmptyShadow=allowEmptyShadow)

    def _checkForLaterShadows(self, newLabel, troves):
        # check to see if we've already shadowed any versions later than
        # these versions to newLabel.
        query = {}
        for trv in troves:
            versionDict = query.setdefault(trv.getName(), {})
            b = trv.getVersion().branch().createShadow(newLabel)
            versionDict[b] = None
        # get the latest version of the new branches
        results = self.repos.getTroveLeavesByBranch(query)

        if not results:
            return []

        oldTroves = []
        for trv in troves:
            versionDict = results.get(trv.getName(), {})
            b = trv.getVersion().branch().createShadow(newLabel)
            versionList = [ x for x in versionDict if x.branch() == b and not x.isModifiedShadow() ]
            if not versionList:
                continue
            latestVersion = max(versionList)
            oldVersion = latestVersion.parentVersion()
            # now get the upstream timeStamps associated with the already
            # shadowed versions.
            oldTroves.extend((trv.getName(), oldVersion, x) \
                             for x in versionDict[latestVersion])

        shadowedTroves = self.repos.getTroves(oldTroves, withFiles=False)

        shadowed = {}
        for shadowedTrove in shadowedTroves:
            (n,v,f) = shadowedTrove.getNameVersionFlavor()
            shadowed[n, v.branch(), f] = v

        laterShadows = []
        for trv in troves:
            (n,v,f) = trv.getNameVersionFlavor()
            shadowedVer = shadowed.get((n, v.branch(), f), None)
            if not shadowedVer:
                continue
            if v < shadowedVer:
                # the version we shadowed before had a later timestamp
                # than the version we're shadowing now.
                laterShadows.append((n, v, f, shadowedVer))
        return laterShadows

    def _createBranchOrShadow(self, newLabel, troveList, shadow,
                              branchType=BRANCH_ALL, sigKeyId=None,
                              allowEmptyShadow=False):
        cs = changeset.ChangeSet()

        seen = set(troveList)
        sourceTroveList = set()
        troveList = set(troveList)
        dupList = []
        needsCommit = False

        newLabel = versions.Label(newLabel)

        while troveList:
            troves = self.repos.getTroves(troveList)
            troveList = set()
            branchedTroves = {}

            if sourceTroveList:
                for st in sourceTroveList:
                    try:
                        sourceTrove = self.repos.getTrove(*st)
                    except repoerrors.TroveMissing:
                        if allowEmptyShadow:
                            st[1].resetTimeStamps()
                            sourceTrove = trove.Trove(*st)
                        else:
                            raise
                    troves.append(sourceTrove)
                sourceTroveList = set()

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

            for trv in troves:
                if trv.isRedirect():
                    raise errors.ShadowRedirect(*trv.getNameVersionFlavor())

                # add contained troves to the todo-list
                newTroves = [ x for x in
                        trv.iterTroveList(strongRefs=True,
                                            weakRefs=True) if x not in seen ]
                troveList.update(newTroves)
                seen.update(newTroves)

                troveName = trv.getName()

                if troveName.endswith(':source'):
                    if not(branchType & self.BRANCH_SOURCE):
                        continue

                elif branchType & self.BRANCH_SOURCE:
                    # name doesn't end in :source - if we want
                    # to shadow the listed troves' sources, do so now

                    # XXX this can go away once we don't care about
                    # pre-troveInfo troves
                    if not trv.getSourceName():
                        from conary.lib import log
                        log.warning('%s has no source information' % troveName)
                        sourceName = troveName
                    else:
                        sourceName = trv.getSourceName()

                    key  = (sourceName,
                            trv.getVersion().getSourceVersion(False),
                            deps.Flavor())
                    if key not in seen:
                        seen.add(key)
                        sourceTroveList.add(key)

                    if not(branchType & self.BRANCH_BINARY):
                        continue

                if shadow:
                    branchedVersion = trv.getVersion().createShadow(newLabel)
                else:
                    branchedVersion = trv.getVersion().createBranch(newLabel,
                                                               withVerRel = 1)

                branchedTrove = trv.copy()
                branchedTrove.changeVersion(branchedVersion)
                #this clears the digital signatures from the shadow
                branchedTrove.troveInfo.sigs.reset()
                # this flattens the old metadata and removes signatures
                branchedTrove.copyMetadata(trv)
                # FIXME we should add a new digital signature in cases
                # where we can (aka user is at kb and can provide secret key

                for ((name, version, flavor), byDefault, isStrong) \
                                            in trv.iterTroveListInfo():
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

                key = (trv.getName(), branchedTrove.getVersion(),
                       trv.getFlavor())

                if sigKeyId is not None:
                    branchedTrove.addDigitalSignature(sigKeyId)
                else:
                    # if no sigKeyId, just add sha1s
                    branchedTrove.computeDigests()

                # use a relative changeset if we're staying on the same host
                if branchedTrove.getVersion().trailingLabel().getHost() == \
                   trv.getVersion().trailingLabel().getHost():
                    branchedTroves[key] = branchedTrove.diff(trv,
                                                           absolute = False)[0]
                else:
                    branchedTroves[key] = branchedTrove.diff(None,
                                                           absolute = True)[0]

            # check for duplicates
            hasTroves = self.repos.hasTroves(branchedTroves)

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
