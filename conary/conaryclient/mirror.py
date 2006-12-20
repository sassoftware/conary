# -*- mode: python -*-
#
# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
#

import itertools
import os

from conary import conarycfg, callbacks, trove
from conary.lib import cfg, util, log
from conary.repository import errors, changeset

class MirrorConfigurationSection(cfg.ConfigSection):
    repositoryMap         =  conarycfg.CfgRepoMap
    user                  =  conarycfg.CfgUserInfo

class MirrorConfiguration(cfg.SectionedConfigFile):
    host                  =  cfg.CfgString
    entitlementDirectory  =  (cfg.CfgPath, '/etc/conary/entitlements')
    labels                =  conarycfg.CfgInstallLabelPath
    matchTroves           =  cfg.CfgSignedRegExpList
    recurseGroups         =  (cfg.CfgBool, False)
    source                =  MirrorConfigurationSection
    target                =  MirrorConfigurationSection
    uploadRateLimit       =  (conarycfg.CfgInt, 0)
    downloadRateLimit     =  (conarycfg.CfgInt, 0)
    lockFile              =  cfg.CfgString

    def __init__(self):
        cfg.SectionedConfigFile.__init__(self)


def filterAlreadyPresent(repos, troveList):
    # Filter out troves which are already in the local repository. Since
    # the marks aren't distinct (they increase, but not monotonially), it's
    # possible that something new got committed with the same mark we
    # last updated to, so we have to look again at all of the troves in the
    # source repository with the last mark which made it into our target.
    present = repos.hasTroves([ x[1] for x in troveList ])
    filteredList = [ x for x in troveList if not present[x[1]] ]

    return filteredList

def filterSigsWithoutTroves(repos, currentMark, sigList):
    # Sigs whose mark is the same as currentMark might not have their trove
    # available on the server (it might be coming as part of this mirror
    # run). sigList has to be sorted by mark for this to work.

    inQuestion = [ x for x in sigList if x[0] == currentMark ]
    present = repos.hasTroves([ x[1] for x in inQuestion ])
    # keep troves whose mark are older than currentMark and troves which
    # are present on the target
    sigList = [ x for x in sigList if present.get(x[1], True) ]
    return sigList

def groupTroves(troveList):
    # combine the troves into indisolvable groups based on their version and
    # flavor; it's assumed that adjacent troves with the same version/flavor
    # must be in a single commit
    grouping = {}
    for info in troveList:
        (n, v, f) = info[1]
        crtGrp = grouping.setdefault((v,f), [])
        crtGrp.append(info)
    grouping = grouping.values()
    # make sure the groups are sorted in ascending order of their mark
    grouping.sort(lambda a,b: cmp(a[0][0], b[0][0]))
    return grouping

def buildJobList(repos, groupList):
    # Match each trove with something we already have; this is to mirror
    # using relative changesets, which is a lot more efficient than using
    # absolute ones.
    q = {}
    for group in groupList:
        for mark, (name, version, flavor) in group:
            # force groups to always be transferred using absolute changesets
            if name.startswith("group-"):
                continue
            d = q.setdefault(name, {})
            l = d.setdefault(version.branch(), [])
            l.append(flavor)

    latestAvailable = {}
    if len(q):
        latestAvailable = repos.getTroveLeavesByBranch(q)

    # we'll keep latestAvailable in sync with what the target will look like
    # as the mirror progresses
    jobList = []
    for group in groupList:
        groupJobList = []
        # for each job find what it's relative to and build up groupJobList
        # as the job list for this group
        for mark, (name, version, flavor) in group:
            if name not in latestAvailable:
                job = (name, (None, None), (version, flavor), True)
                currentMatch = (None, None, None, None)
            else:
                d = latestAvailable[name]
                verFlvMap = set()
                # name, version, versionDistance, flavorScore
                currentMatch = (None, None, None, None)
                for repVersion, flavorList in d.iteritems():
                    for repFlavor in flavorList:
                        score = flavor.score(repFlavor)
                        if repVersion == version:
                            closeness = 100000
                        else:
                            closeness = version.closeness(repVersion)

                        if score is False: continue
                        if score < currentMatch[3]:
                            continue
                        elif score > currentMatch[3]:
                            currentMatch = (repVersion, repFlavor, closeness,
                                            score)
                        elif closeness < currentMatch[2]:
                            continue
                        else:
                            currentMatch = (repVersion, repFlavor, closeness,
                                            score)

                job = (name, (currentMatch[0], currentMatch[1]),
                              (version, flavor), currentMatch[0] is None)

            groupJobList.append((mark, job))

        # now iterate through groupJobList and update latestAvailable to
        # reflect the state of the mirror after this job completes
        for mark, job in groupJobList:
            name = job[0]
            if name.startswith("group-"):
                continue
            oldVersion, oldFlavor = job[1]
            newVersion, newFlavor = job[2]

            d = latestAvailable.setdefault(name, {})

            if oldVersion in d and oldVersion.branch() == newVersion.branch():
                # If the old version is on the same branch as the new one,
                # replace the old with the new. If it's on a different
                # branch, we'll track both.
                d[oldVersion].remove(oldFlavor)
                if not d[oldVersion]: del d[oldVersion]

            flavorList = d.setdefault(newVersion, [])
            flavorList.append(newFlavor)

        jobList.append(groupJobList)

    return jobList

recursedGroups = set()
def recurseTrove(sourceRepos, name, version, flavor,
                 callback = callbacks.ChangesetCallback()):
    global recursedGroups
    assert(name.startswith("group-"))
    # there's nothing much we can recurse from the source
    if name.endswith(":source"):
        return []
    # avoid grabbing the same group multiple times
    if (name, version, flavor) in recursedGroups:
        return []
    # we need to grab the trove list recursively for
    # mirroring. Unfortunately the netclient does not wire the
    # repository's getChangeSet parameters, so we need to cheat a
    # little to keep the roundtrips to a minimum
    log.debug("recursing group trove: %s=%s[%s]" % (name, version, flavor))
    groupCs = sourceRepos.createChangeSet(
        [(name, (None, None), (version, flavor), True)],
        withFiles=False, withFileContents = False, recurse = True,
        callback = callback)
    recursedGroups.add((name, version, flavor))
    ret = []
    removedList = []
    for troveCs in groupCs.iterNewTroveList():
        (trvName, trvVersion, trvFlavor) = troveCs.getNewNameVersionFlavor()
        # keep track of groups we have already recursed through
        if trvName.startswith("group-"):
            recursedGroups.add((trvName, trvVersion, trvFlavor))
        if troveCs.getType() == trove.TROVE_TYPE_REMOVED:
            removedList.append((trvName, trvVersion, trvFlavor))
        else:
            ret.append((trvName, trvVersion, trvFlavor))
    return ret, removedList

# format a bundle for display
def displayBundle(bundle):
    minMark = min([x[0] for x in bundle])
    names = [x[1][0] for x in bundle]
    names.sort()
    oldVF = set([x[1][1] for x in bundle])
    newVF = set([x[1][2] for x in bundle])
    if len(oldVF) > 1 or len(newVF) > 1:
        # this bundle doesn't use common version/flavors
        # XXX: find out why? for now, return old style display
        return [ x[1] for x in bundle ]
    oldVF = list(oldVF)[0]
    newVF = list(newVF)[0]
    ret = []
    if minMark > 0:
        markLine = "mark: %.0f " % (minMark,)
    else:
        markLine = ""
    if oldVF == (None, None):
        markLine += "absolute changeset"
        ret.append(markLine)
    else:
        markLine += "relative changeset"
        ret.append(markLine)
    ret.append("troves: " + ' '.join(names))
    if oldVF != (None, None):
        ret.append("oldVF: %s" % (oldVF,))
    ret.append("newVF: %s" % (newVF,))
    return "\n  ".join(ret)

# wrapper for displaying a simple jobList
def displayJobList(jobList):
    return displayBundle([(0, x) for x in jobList])

# this is to keep track of GPG keys we already added to avoid repeated
# add operation into the target
addedKeys = {}
def mirrorGPGKeys(sourceRepos, targetRepos, cfg, host, test = False):
    global addedKeys
    # avoid duplicate effort
    if addedKeys.has_key(host):
        return
    log.debug("mirroring pgp keys for %s", host)
    # we mirror the entire set of GPG keys in one step to avoid
    # multiple roundtrips to the sourceRepos. Also, mirroring new
    # signatures for old troves is the first mirroring step, so we
    # need to have the GPG keys available early on
    keyList = sourceRepos.getNewPGPKeys(host, -1)
    if test:
        log.debug("(not adding %d keys due to test mode)", len(keyList))
        return
    if len(keyList):
        log.debug("adding %d keys to target", len(keyList))
        targetRepos.addPGPKeyList(cfg.host, keyList)
    else:
        keyList = [ False ]
    addedKeys[host] = set(keyList)

# mirroring stuff when we are running into PathIdConflict errors
def splitJobList(jobList, sourceRepos, targetRepos, callback = None):
    log.debug("PathIdConflict detected; splitting job further...")
    jobs = {}
    for job in jobList:
        name = job[0]
        if ':' in name:
            name = name.split(':')[0]
        l = jobs.setdefault(name, [])
        l.append(job)
    i = 0
    for smallJobList in jobs.itervalues():
        (outFd, tmpName) = util.mkstemp()
        os.close(outFd)
        log.debug("jobsplit %d of %d %s" % (
            i + 1, len(jobs), displayBundle([(0,x) for x in smallJobList])))
        cs = sourceRepos.createChangeSetFile(smallJobList, tmpName, recurse = False,
                                             callback = callback)
        log.debug("committing")
        targetRepos.commitChangeSetFile(tmpName, mirror = True, callback = callback)
        os.unlink(tmpName)
        callback.done()
        i += 1
    return

def mirrorSignatures(sourceRepos, targetRepos, currentMark, cfg,
                     test = False, syncSigs = False):
    # miror the GPG keys for the main source repo
    mirrorGPGKeys(sourceRepos, targetRepos, cfg, cfg.host, test)

    if syncSigs:
        log.debug("getting full trove list for signature sync")
        troveDict = sourceRepos.getTroveVersionList(cfg.host, { None : None })
        sigList = []
        for name, versionD in troveDict.iteritems():
            for version, flavorList in versionD.iteritems():
                for flavor in flavorList:
                    sigList.append((currentMark, (name, version, flavor)))
    else:
        log.debug("looking for new trove signatures")
        sigList = sourceRepos.getNewSigList(cfg.host, currentMark)
    # protection against duplicate items returned in the list by some servers
    if not len(sigList):
        return 0
    sigList = list(set(sigList))
    sigList.sort(lambda a,b: cmp(a[0], b[0]))
    log.debug("%d new signatures are available" % len(sigList))

    # also weed out the signatures that don't belong on our label. Having none
    # left after this isn't that big a deal, so we  don't have to return
    if cfg.labels and len(sigList):
        sigList = [ x for x in sigList if
                    x[1][1].branch().label() in cfg.labels ]
        log.debug("after label filtering %d sigs are needed", len(sigList))
    # filter out signatures for troves we aren't interested in because
    # of the matchTroves setting
    if cfg.matchTroves and len(sigList):
        sigList = [x for x in sigList if
                   cfg.matchTroves.match(x[1][0]) > 0]
        log.debug("after matchTroves %d sigs are needed", len(sigList))

    log.debug("removing signatures for troves not yet mirrored")
    sigList = filterSigsWithoutTroves(targetRepos, currentMark, sigList)
    log.debug("%d signatures need to be mirrored", len(sigList))

    updateCount = 0
    if sigList:
        sigs = sourceRepos.getTroveSigs([ x[1] for x in sigList ])
        # build the ((n,v,f), signature) list only for the troves that have signatures
        sigs = [ (x[0][1], x[1]) for x in itertools.izip(sigList, sigs) if len(x[1]) > 0 ]
        if test:
            log.debug("not mirroring %d signatures due to test mode", len(sigs))
        else:
            updateCount = targetRepos.setTroveSigs(sigs)

    return updateCount

# this mirrors all the troves marked as removed from the sourceRepos into the targetRepos
def mirrorRemoved(sourceRepos, targetRepos, troveSet, test = False, callback = None):
    if not troveSet:
        return 0
    log.debug("checking on %d removed troves", len(troveSet))
    # these removed troves better exist on the target
    present = targetRepos.hasTroves(list(troveSet))
    missing = [ x for x in troveSet if not present[x] ]
    # we can not have any "missing" troves while we mirror removals
    for t in missing:
        log.warning("Mirroring removed trove: valid trove not found on target: %s", t)
        troveSet.remove(t)
    # for the remaining removed troves, are any of them already mirrored?
    jobList = [ (name, (None, None), (version, flavor), True) for
                (name, version, flavor) in troveSet ]
    cs = targetRepos.createChangeSet(jobList, recurse=False, withFiles=False,
                                     withFileContents=False, callback=callback)
    for trvCs in cs.iterNewTroveList():
        if trvCs.getType() == trove.TROVE_TYPE_REMOVED:
            troveSet.remove(trvCs.getNewNameVersionFlavor())
    log.debug("mirroring %d removed troves", len(troveSet))
    if not troveSet:
        return 0

    jobList = [ (name, (None, None), (version, flavor), True) for
                (name, version, flavor) in troveSet ]
    log.debug("mirroring removed troves %s" % (displayJobList(jobList),))
    # grab the removed troves changeset
    cs = sourceRepos.createChangeSet(jobList, recurse = False,
                                     withFiles = False, withFileContents = False,
                                     callback = callback)
    log.debug("committing")
    targetRepos.commitChangeSet(cs, mirror = True, callback = callback)
    callback.done()
    return len(jobList)

# While running under --test, we should not touch the mirror mark of the target repository
CurrentTestMark = None
LastBundleSet = None

def mirrorRepository(sourceRepos, targetRepos, cfg,
                     test = False, sync = False, syncSigs = False,
                     callback = callbacks.ChangesetCallback()):
    global CurrentTestMark
    global LastBundleSet

    # find the latest timestamp stored on the target mirror
    if sync:
        currentMark = -1
        if test:
            CurrentTestMark = currentMark
        else:
            targetRepos.setMirrorMark(cfg.host, currentMark)
    else:
        if test and CurrentTestMark is not None:
            currentMark = CurrentTestMark
        else:
            currentMark = targetRepos.getMirrorMark(cfg.host)
            CurrentTestMark = currentMark

    log.debug("currently up to date through %d", int(currentMark))

    # first, mirror signatures for troves already mirrored
    updateCount = mirrorSignatures(sourceRepos, targetRepos, currentMark,
                                   cfg = cfg, test = test, syncSigs = syncSigs)

    log.debug("looking for new troves")
    # now find all of the troves we need from from the mirror source
    # FIXME: getNewTroveList should accept and only return troves on
    # the labels we're interested in
    troveList = sourceRepos.getNewTroveList(cfg.host, currentMark)
    # we need to protect ourselves from duplicate items in the troveList
    troveList = list(set(troveList))
    troveList.sort(lambda a,b: cmp(a[0], b[0]))
    log.debug("%d new troves are available", len(troveList))
    crtTroveLen = len(troveList)
    if not crtTroveLen:
        # this should be the end - no more troves to look at
        return 0

    # prepare a new max mark to be used when we need to break out of a loop
    crtMaxMark = max(x[0] for x in troveList)
    if currentMark > 0 and crtMaxMark == currentMark:
        # if we're hung on the current max then we need to
        # forcibly advance the mark in case we're stuck
        crtMaxMark += 1 # only used if we filter out all troves below

    # we're trying to "weed out" troves that don't belong on the configured labels.
    if cfg.labels:
        troveList = [ x for x in troveList
                      if x[1][1].branch().label() in cfg.labels ]
        log.debug("after label filtering %d troves are needed", len(troveList))

    # filter out troves based on the matchTroves value
    if cfg.matchTroves:
        troveList = [ x for x in troveList
                      if cfg.matchTroves.match(x[1][0]) > 0 ]
        log.debug("after matchTroves %d troves are needed", len(troveList))

    # removed troves are a special blend - we keep them separate
    removedSet  = set([ x[1] for x in troveList if x[2] == trove.TROVE_TYPE_REMOVED ])
    troveList = [ (x[0], x[1]) for x in troveList if x[2] != trove.TROVE_TYPE_REMOVED ]

    # figure out if we need to recurse the group-troves
    if cfg.recurseGroups:
        # avoid adding duplicates
        troveSetList = set([x[1] for x in troveList])
        for (mark, (name, version, flavor), troveType) in troveList:
            if name.startswith("group-"):
                recTroves, rmTroves = recurseTrove(sourceRepos, name, version, flavor,
                                         callback = callback)
                # add the results at the end with the current mark
                for (n,v,f) in recTroves:
                    if (n,v,f) not in troveSetList:
                        troveList.append((mark, (n,v,f)))
                        troveSetList.add((n,v,f))
                for x in rmTroves:
                    removedSet.add(x)
        log.debug("after group recursion %d troves are needed", len(troveList))
        # we need to make sure we mirror the GPG keys of any newly added troves
        for host in set([x[1].getHost() for x in troveSetList + removedSet]) - set([cfg.host]):
            mirrorGPGKeys(sourceRepos, targetRepos, cfg, host, test)

    if len(troveList):
        # now filter the ones already existing
        troveList = filterAlreadyPresent(targetRepos, troveList)
        log.debug("found %d troves not present in the mirror", len(troveList))

    # if we were returned troves, but we filtered them all out, advance the
    # mark and signal "try again"
    if len(troveList) == 0 and len(removedSet) == 0 and crtTroveLen:
        # we had troves and now we don't
        log.debug("no troves found for our label %s" % cfg.labels)
        log.debug("advancing newMark to %d" % int(crtMaxMark))
        if test:
            CurrentTestMark = crtMaxMark
        else:
            targetRepos.setMirrorMark(cfg.host, crtMaxMark)
        # try again
        return -1

    bundles = []
    if troveList:
        log.debug("grouping %d troves based on version and flavor", len(troveList))
        groupList = groupTroves(troveList)
        log.debug("building grouped job list")
        bundles = buildJobList(targetRepos, groupList)

        if len(bundles) > 1:
            # We cut off the last bundle if there is more than one and let the
            # next pass through this function handle it. That makes sure that
            # we don't half-commit a package when the server limits the number
            # of responses getNewTroveList() will return
            bundles = bundles[:-1]

        if test and LastBundleSet == bundles:
            log.debug("test mode detected a loop, ending...")
            return 0
        LastBundleSet = bundles

    for i, bundle in enumerate(bundles):
        jobList = [ x[1] for x in bundle ]
        # XXX it's a shame we can't give a hint as to what server to use
        # to avoid having to open the changeset and read in bits of it
        if test:
            log.debug("test mode: skipping retrieval (%d of %d) %s" % (i + 1, len(bundles), jobList))
            log.debug("test mode: skipping commit")
        else:
            (outFd, tmpName) = util.mkstemp()
            os.close(outFd)
            log.debug("getting (%d of %d) %s" % (i + 1, len(bundles), displayBundle(bundle)))
            try:
                cs = sourceRepos.createChangeSetFile(jobList, tmpName, recurse = False,
                                                     callback = callback)
            except changeset.PathIdsConflictError, e:
                splitJobList(jobList, sourceRepos, targetRepos, callback = callback)
            else:
                log.debug("committing")
                targetRepos.commitChangeSetFile(tmpName, mirror = True, callback = callback)
            try:
                os.unlink(tmpName)
            except OSError:
                pass
            callback.done()
        updateCount += len(bundle)
    else: # only when we're all done looping advance mark to the new max
        if len(bundles):
            # compute the max mark of the bundles we comitted
            crtMaxMark = max([min([x[0] for x in bundle]) for bundle in bundles])
        log.debug("setting the mirror mark to %d", int(crtMaxMark))
        if test:
            CurrentTestMark = crtMaxMark
        else:
            targetRepos.setMirrorMark(cfg.host, crtMaxMark)
    updateCount += mirrorRemoved(sourceRepos, targetRepos, removedSet,
                                 test = test, callback = callback)
    return updateCount
