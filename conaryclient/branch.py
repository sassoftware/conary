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

from deps import deps
from repository import changeset
import versions

class ClientBranch:
    BRANCH_ALL         = 0
    BRANCH_SOURCE_ONLY = 1
    BRANCH_BINARY_ONLY = 2

    def createBranch(self, newLabel, troveList = [], branchType=BRANCH_ALL):
        return self._createBranchOrShadow(newLabel, troveList, shadow = False, 
                                          branchType = branchType)

    def createShadow(self, newLabel, troveList = [], branchType=BRANCH_ALL):
        return self._createBranchOrShadow(newLabel, troveList, shadow = True, 
                                          branchType = branchType)

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

	    for trove in troves:
                # add contained troves to the todo-list
                newTroves = [ x for x in trove.iterTroveList() if x not in seen ]
                troveList.update(newTroves)
                seen.update(newTroves)

                troveName = trove.getName()

                if troveName.endswith(':source'):
                    if branchType == self.BRANCH_BINARY_ONLY:
                        continue

                elif branchType != self.BRANCH_BINARY_ONLY:
                    # name doesn't end in :source - if we want 
                    # to shadow the listed troves' sources, do so now

                    # XXX this can go away once we don't care about
                    # pre-troveInfo troves
                    if not trove.getSourceName():
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

                    if branchType == self.BRANCH_SOURCE_ONLY:
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

		for (name, version, flavor) in trove.iterTroveList():
                    if shadow:
                        branchedVersion = version.createShadow(newLabel)
                    else:
                        branchedVersion = version.createBranch(newLabel,
                                                               withVerRel = 1)
                    byDefault = trove.includeTroveByDefault(name, 
                                                            version, flavor)
		    branchedTrove.delTrove(name, version, flavor,
                                           missingOkay = False)
		    branchedTrove.addTrove(name, branchedVersion, flavor,
                                            byDefault=byDefault)

                key = (trove.getName(), branchedTrove.getVersion(),
                       trove.getFlavor())
                branchedTroves[key] = branchedTrove.diff(None)[0]

            # check for duplicates - XXX this could be more efficient with
            # a better repository API
            queryDict = {}
            for (name, version, flavor) in branchedTroves.iterkeys():
                l = queryDict.setdefault(name, [])
                l.append(version)

            matches = self.repos.getAllTroveFlavors(queryDict)

            for (name, version, flavor), troveCs in branchedTroves.iteritems():
                if (matches.has_key(name) and matches[name].has_key(version) 
                    and flavor in matches[name][version]):
                    # this trove has already been branched
                    dupList.append((name, version.branch()))
                else:
                    cs.newTrove(troveCs)
                    cs.addPrimaryTrove(name, version, flavor)
                    needsCommit = True

        if needsCommit:
            self.repos.commitChangeSet(cs)

	return dupList

