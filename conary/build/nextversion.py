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


def nextVersion(repos, db, troveNames, sourceVersion, troveFlavor,
                targetLabel=None, alwaysBumpCount=False):
    """
    Calculates the version to use for a newly built trove which is about
    to be added to the repository.

    @param repos: repository proxy
    @type repos: NetworkRepositoryClient
    @param troveNames: name(s) of the trove(s) being built
    @type troveNames: str
    @param sourceVersion: the source version that we are incrementing
    @type sourceVersion: Version
    @param troveFlavor: flavor of the trove being built
    @type troveFlavor: deps.Flavor
    @param alwaysBumpCount: if True, then do not return a version that
    matches an existing trove, even if their flavors would differentiate
    them, instead, increase the appropriate count.
    @type alwaysBumpCount: bool
    """
    if not isinstance(troveNames, (list, tuple, set)):
        troveNames = [troveNames]

    if isinstance(troveFlavor, list):
        troveFlavorSet = set(troveFlavor)
    elif isinstance(troveFlavor, set):
        troveFlavorSet = troveFlavor
    else:
        troveFlavorSet = set([troveFlavor])

    # strip off any components and remove duplicates
    pkgNames = set([x.split(':')[0] for x in troveNames])

    if targetLabel:
        # we want to make sure this version is unique...but it must
        # be unique on the target label!  Instead of asserting that
        # this is a direct shadow of a binary that is non-existant
        # we look at binary numbers on the target label.
        sourceVersion = sourceVersion.createShadow(targetLabel)

    # search for all the packages that are being created by this cook -
    # we take the max of all of these versions as our latest.
    query = dict.fromkeys(pkgNames,
                  {sourceVersion.getBinaryVersion().trailingLabel() : None })

    if repos and not sourceVersion.isOnLocalHost():
        d = repos.getTroveVersionsByLabel(query,
                                          troveTypes = repos.TROVE_QUERY_ALL)
    else:
        d = {}
    return _nextVersionFromQuery(d, db, pkgNames, sourceVersion,
                                 troveFlavorSet,
                                 alwaysBumpCount=alwaysBumpCount)


def nextVersions(repos, db, sourceBinaryList, alwaysBumpCount=False):
    # search for all the packages that are being created by this cook -
    # we take the max of all of these versions as our latest.
    query = {}
    d = {}
    if repos:
        for sourceVersion, troveNames, troveFlavors in sourceBinaryList:
            if sourceVersion.isOnLocalHost():
                continue
            pkgNames = set([x.split(':')[0] for x in troveNames])
            for pkgName in pkgNames:
                if pkgName not in query:
                    query[pkgName] = {}
                label = sourceVersion.getBinaryVersion().trailingLabel()
                query[pkgName][label] = None

        d = repos.getTroveVersionsByLabel(query,
                                          troveTypes = repos.TROVE_QUERY_ALL)
    nextVersions = []
    for sourceVersion, troveNames, troveFlavors in sourceBinaryList:
        if not isinstance(troveFlavors, (list, tuple, set)):
            troveFlavors = set([troveFlavors])
        else:
            troveFlavors = set(troveFlavors)
        newVersion = _nextVersionFromQuery(d, db, troveNames, sourceVersion,
                                           troveFlavors,
                                           alwaysBumpCount=alwaysBumpCount)
        nextVersions.append(newVersion)
    return nextVersions

def _nextVersionFromQuery(query, db, troveNames, sourceVersion,
                          troveFlavorSet, alwaysBumpCount=False):
    pkgNames = set([x.split(':')[0] for x in troveNames])
    latest = None
    relVersions = []
    for pkgName in pkgNames:
        if pkgName in query:
            for version in query[pkgName]:
                if (version.getSourceVersion().trailingRevision() ==
                            sourceVersion.trailingRevision()
                    and version.trailingLabel() ==
                            sourceVersion.trailingLabel()):
                    relVersions.append((version, query[pkgName][version]))
    del pkgName

    defaultLatest = sourceVersion.copy()
    defaultLatest = defaultLatest.getBinaryVersion()
    defaultLatest.incrementBuildCount()
    shadowCount = defaultLatest.trailingRevision().shadowCount()

    matches = [ x for x in relVersions
                if x[0].trailingRevision().shadowCount() == shadowCount ]


    if matches:
        # all these versions only differ by build count.
        # but we can't rely on the timestamp sort, because the build counts
        # are on different packages that might have come from different commits
        # All these packages should have the same shadow count though,
        # - which is the shadow could that should
        # XXX does this deal with shadowed versions correctly?

        relVersions.sort()
        matches.sort(key=lambda x: x[0].trailingRevision().buildCount)
        latest, flavors = matches[-1]
        latestByStamp = relVersions[-1][0]
        incCount = False

        if alwaysBumpCount:
            # case 1.  There is a binary trove with this source
            # version, and we always want to bump the build count
            incCount = True
        else:
            if troveFlavorSet & set(flavors):
                # case 2.  There is a binary trove with this source
                # version, and our flavor matches one already existing
                # with this build count, so bump the build count
                incCount = True
            elif latest.getSourceVersion() == sourceVersion:
                # case 3.  There is a binary trove with this source
                # version, and our flavor does not exist at this build
                # count.

                if latestByStamp != latest:
                    # case 3a. the latest possible match for our branch
                    # is not the _latest_ as defined by getTroveLatestByLabel.
                    # Avoid adding a new package
                    # to some older spot in the version tree
                    incCount = True
                else:
                    # case 3b. development has been occuring on this branch
                    # on this label, and there is an open spot for this
                    # flavor, so reuse this version.
                    pass
            else:
                # case 4. There is a binary trove on a different branch
                # (but the same label)
                incCount = True

        if incCount:
            revision = latest.trailingRevision().copy()
            latest = sourceVersion.branch().createVersion(revision)
            latest.incrementBuildCount()

    if not latest:
        # case 4.  There is no binary trove derived from this source
        # version.
        latest = defaultLatest
    if latest.isOnLocalHost():
        return nextLocalVersion(db, troveNames, latest, troveFlavorSet)
    else:
        return latest

def nextLocalVersion(db, troveNames, latest, troveFlavorSet):
    # if we've branched on to a local label, we check
    # the database for installed versions to see if we need to
    # bump the build count on this label

    # search for both pkgs and their components
    pkgNames = set([x.split(':')[0] for x in troveNames])
    pkgNames.update(troveNames)

    query = dict.fromkeys(troveNames, {latest.branch() : None })
    results = db.getTroveLeavesByBranch(query)

    relVersions = []
    for troveName in troveNames:
        if troveName in results:
            for version in results[troveName]:
                if version.getSourceVersion() == latest.getSourceVersion():
                    relVersions.append((version,
                                        results[troveName][version]))
    if not relVersions:
        return latest

    relVersions.sort(lambda a, b: cmp(a[0].trailingRevision().buildCount,
                                      b[0].trailingRevision().buildCount))
    latest, flavors = relVersions[-1]
    if troveFlavorSet & set(flavors):
        latest.incrementBuildCount()
    return latest

def nextSourceVersion(targetBranch, revision, existingVersionList):
    """
        Returns the correct source version on the branch given
        with the revision number specified given the list of
        existing versions.
        @param targetBranch: the branch to create the version on
        @param revision: a revision object that contains the desired upstream
        version and source count to use in the version.
        This may be or modified to fit the target branch (for example,
        if it has too many .'s in it for the branch it is being moved to).
        @param existingVersionList: list of version objects that are the other
        source versions for this package on this branch.
    """
    # we're going to be incrementing this
    revision = revision.copy()
    # not sure if we actually need to copy this but it can't hurt...
    desiredVersion = targetBranch.createVersion(revision).copy()
    # this could have too many .'s in it
    if desiredVersion.shadowLength() < revision.shadowCount():
        # this truncates the dotted version string
        revision.getSourceCount().truncateShadowCount(
                                    desiredVersion.shadowLength())
        desiredVersion = targetBranch.createVersion(revision)

    # the last shadow count is not allowed to be a 0
    if [ x for x in revision.getSourceCount().iterCounts() ][-1] == 0:
        desiredVersion.incrementSourceCount()
    # if 1-3.6 exists we don't want to be created 1-3.5.
    matchingUpstream = [ x.trailingRevision()
                         for x in existingVersionList
                         if (x.trailingRevision().getVersion()
                             == revision.getVersion()) ]
    if (revision in matchingUpstream
        and desiredVersion.shadowLength() > revision.shadowCount()):
        desiredVersion.incrementSourceCount()
        revision = desiredVersion.trailingRevision()

    if matchingUpstream:
        def _sourceCounts(revision):
            return list(revision.getSourceCount().iterCounts())
        shadowCounts = _sourceCounts(revision)
        matchingShadowCounts = [ x for x in matchingUpstream
                           if _sourceCounts(x)[:-1] == shadowCounts[:-1] ]
        if matchingShadowCounts:
            latest = sorted(matchingShadowCounts, key=_sourceCounts)[-1]
            if (revision in matchingShadowCounts
                or _sourceCounts(latest) > _sourceCounts(revision)):
                revision = latest.copy()
                desiredVersion = targetBranch.createVersion(revision)
                desiredVersion.incrementSourceCount()

    assert(not desiredVersion in existingVersionList)
    return desiredVersion
