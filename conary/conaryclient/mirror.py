#!/usr/bin/python2.4
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
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
#

import itertools
import os

from conary import conarycfg
from conary.lib import cfg, util, log

class MirrorConfigurationSection(cfg.ConfigSection):
    repositoryMap         =  conarycfg.CfgRepoMap
    user                  =  conarycfg.CfgUserInfo

class MirrorConfiguration(cfg.SectionedConfigFile):
    host                  =  cfg.CfgString
    entitlementDirectory  =  (cfg.CfgPath, '/etc/conary/entitlements')
    labels                =  conarycfg.CfgInstallLabelPath
    source                =  MirrorConfigurationSection
    target                =  MirrorConfigurationSection

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
    grouping = []
    currentGroup = []
    for info in troveList:
        troveInfo = info[1]
        if not currentGroup:
            currentGroup.append(info)
        elif troveInfo[1:] == currentGroup[0][1][1:]:
            currentGroup.append(info)
        else:
            grouping.append(currentGroup)
            currentGroup = [ info ]

    if currentGroup:
        grouping.append(currentGroup)

    return grouping

def buildJobList(repos, groupList):
    # Match each trove with something we already have; this is to mirror
    # using relative changesets, which is a lot more efficient than using
    # absolute ones.
    q = {}
    for group in groupList:
        for mark, (name, version, flavor) in group:
            d = q.setdefault(name, {})
            l = d.setdefault(version.branch(), [])
            l.append(flavor)

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

def mirrorSignatures(sourceRepos, targetRepos, sigList):
    sigs = sourceRepos.getTroveSigs([ x[1] for x in sigList ])
    updateCount = targetRepos.setTroveSigs(
                [ (x[0][1], x[1]) for x in itertools.izip(sigList, sigs) ])

    return updateCount

def mirrorRepository(sourceRepos, targetRepos, cfg, test, sync, syncSigs):
    if sync:
        currentMark = -1
    else:
        # find the latest timestamp stored on the target mirror
        currentMark = targetRepos.getMirrorMark(cfg.host)
    log.debug("currently up to date through %d", int(currentMark))

    # now find all of the troves we need from from the mirror source
    troveList = sourceRepos.getNewTroveList(cfg.host, currentMark)
    log.debug("%d new troves are available", len(troveList))

    # FIXME: getnewTroveList should accept and only return troves on
    # the labels we're interested in
    if cfg.labels and len(troveList):
        # XXX: temporary fix: we're trying to "weed out" troves that don't
        # belong on the configured labels. If we're left with no troves after
        # filtering, we need to force the target mark to the max of the current
        # set in order to be able to move on - otherwise the next call to
        # getNewTroveList will return the same data set
        crtMaxMark = max(x[0] for x in troveList)
        crtTroveLen = len(troveList)
        if currentMark > 0 and crtMaxMark == currentMark:
            # if we're hung on the current max then we need to
            # forcibly advance the mark in case we're stuck
            crtMaxMark += 1 # only used if we filter out all troves below
        labelDict = set(cfg.labels)
        troveList = [ x for x in troveList if
                            x[1][1].branch().label() in cfg.labels ]
        log.debug("after label filtering %d troves are needed", len(troveList))
        if len(troveList) == 0 and crtTroveLen:
            # we had troves and now we don't
            log.debug("getNewTroveList did not return any troves for our label %s" % cfg.labels)
            log.debug("setting newMark to %s" % crtMaxMark)
            targetRepos.setMirrorMark(cfg.host, crtMaxMark)
            # try again
            return -1

    log.debug("looking for new pgp keys")
    keyList = sourceRepos.getNewPGPKeys(cfg.host, currentMark)
    if test:
        log.debug("(not adding %d keys due to test mode)", len(keyList))
    else:
        log.debug("adding %d keys to target", len(keyList))
        targetRepos.addPGPKeyList(cfg.host, keyList)

    if syncSigs:
        log.debug("getting full trove list for signature sync")
        troveDict = sourceRepos.getTroveVersionList(host, { None : None })
        sigList = []
        for name, versionD in troveDict.iterkeys():
            for version, flavorList in versionD.iterkeys():
                sigList += [ (name, version, x) for x in flavorList ]
    else:
        log.debug("looking for new trove signatures")
        sigList = sourceRepos.getNewSigList(cfg.host, currentMark)

    log.debug("%d new signatures are available" % len(sigList))

    # also weed out the signatures that don't belong on our label. Having none
    # left after this isn't that big a deal, so we  don't have to return
    if cfg.labels and len(sigList):
        sigList = [ x for x in sigList if
                            x[1][1].branch().label() in cfg.labels ]
        log.debug("after label filtering %d sigs are needed", len(sigList))

    log.debug("removing signatures for troves not yet mirrored")
    sigList = filterSigsWithoutTroves(targetRepos, currentMark, sigList)
    log.debug("%d signatures need to be mirrored", len(sigList))

    log.debug("filtering troves already present on the mirror")

    troveList = filterAlreadyPresent(targetRepos, troveList)
    log.debug("grouping %d troves based on version and flavor", len(troveList))
    groupList = groupTroves(troveList)
    log.debug("building grouped job list")
    bundles = buildJobList(targetRepos, groupList)

    if sigList:
        updateCount = mirrorSignatures(sourceRepos, targetRepos, sigList)
    else:
        updateCount = 0

    if len(bundles) > 1:
        # We cut off the last bundle if there is more than one and let the
        # next pass through this function handle it. That makes sure that
        # we don't half-commit a package when the server limits the number
        # of responses getNewTroveList() will return
        bundles = bundles[:-1]

    for i, bundle in enumerate(bundles):
        (outFd, tmpName) = util.mkstemp()
        os.close(outFd)
        log.debug("getting (%d of %d) %s" % (i + 1, len(bundles), bundle))
        jobList = [ x[1] for x in bundle ]
        newMark = max(x[0] for x in bundle)
        cs = sourceRepos.createChangeSetFile(jobList, tmpName, recurse = False)
        # XXX it's a shame we can't give a hint as to what server to use
        # to avoid having to open the changeset and read in bits of it
        if test:
            log.debug("(skipping commit due to test mode)")
        else:
            log.debug("committing")
            targetRepos.commitChangeSetFile(tmpName, mirror = True)
            targetRepos.setMirrorMark(cfg.host, newMark)

        os.unlink(tmpName)
        updateCount += len(bundle)

    return updateCount
