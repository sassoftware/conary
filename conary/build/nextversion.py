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
    @type troveFlavor: deps.DependencySet
    @param alwaysBumpCount: if True, then do not return a version that 
    matches an existing trove, even if their flavors would differentiate 
    them, instead, increase the appropriate count.  
    @type alwaysBumpCount: bool
    """
    if not isinstance(troveNames, (list, tuple, set)):
        troveNames = [troveNames]

    # strip off any components and remove duplicates
    pkgNames = set([x.split(':')[0] for x in troveNames])

    # search for all the packages that are being created by this cook - 
    # we take the max of all of these versions as our latest.
    query = dict.fromkeys(pkgNames, 
                          {sourceVersion.getBinaryVersion().branch() : None })
    
    if repos:
        d = repos.getTroveVersionsByBranch(query)
    else:
        d = {}

    latest = None

    relVersions = []
    for pkgName in pkgNames:
        if pkgName in d:
            for version in d[pkgName]:
                if (not version.isBranchedBinary()
                    and version.getSourceVersion() == sourceVersion):
                    relVersions.append((version, d[pkgName][version]))
    del pkgName

    if relVersions:
        # all these versions only differ by build count.
        # but we can't rely on the timestamp sort, because the build counts
        # are on different packages that might have come from different commits
        # XXX does this deal with shadowed versions correctly?
        relVersions.sort(lambda a, b: cmp(a[0].trailingRevision().buildCount,
                                          b[0].trailingRevision().buildCount))
        latest, flavors = relVersions[-1]
        latest = latest.copy()

        if targetLabel:
            latest = latest.createBranch(targetLabel, withVerRel = True)

        if alwaysBumpCount:
            # case 1.  There is a binary trove with this source
            # version, and we always want to bump the build count
            latest.incrementBuildCount()
        else:
            if troveFlavor in flavors:
                # case 2.  There is a binary trove with this source
                # version, and our flavor matches one already existing
                # with this build count, so bump the build count
                latest.incrementBuildCount()
            # case 3.  There is a binary trove with this source
            # version, and our flavor does not exist at this build 
            # count, so reuse the latest binary version
    if not latest:
        # case 4.  There is no binary trove derived from this source 
        # version.  
        latest = sourceVersion.copy()

        if targetLabel:
            latest = latest.createBranch(targetLabel, withVerRel = True)
        else:
            latest = sourceVersion
        latest = latest.getBinaryVersion()
        latest.incrementBuildCount()
    if latest.isOnLocalHost():
        return nextLocalVersion(db, troveNames, latest, troveFlavor) 
    else:
        return latest
        
def nextLocalVersion(db, troveNames, latest, troveFlavor):
    # if we've branched on to a local label, we check
    # the database for installed versions to see if we need to
    # bump the build count on this label

    # search for both pkgs and their components
    pkgNames = set([x.split(':')[0] for x in troveNames] + troveNames)

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
    if troveFlavor in flavors:
        latest.incrementBuildCount()
    return latest

