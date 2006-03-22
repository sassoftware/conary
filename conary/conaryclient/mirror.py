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
from conary.repository import errors

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

# this is to keep track of PGP keys we already added to avoid repeated
# add operation into the target
addedKeys = set()

def mirrorSignatures(sourceRepos, targetRepos, currentMark, cfg,
                     test = False, syncSigs = False):
    global addedKeys
    # when mirroring keylist, the first time we ask we should get all
    # of the newly available, since that's when the mark will be the
    # lowest. That's why really, just one round of getNew/add should suffice
    if not len(addedKeys):
        log.debug("looking for new pgp keys")
        keyList = sourceRepos.getNewPGPKeys(cfg.host, currentMark)
        if test:
            log.debug("(not adding %d keys due to test mode)", len(keyList))
        elif len(keyList):
            log.debug("adding %d keys to target", len(keyList))
            targetRepos.addPGPKeyList(cfg.host, keyList)
        else:
            keyList = [ False ]
        addedKeys = set(keyList)

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

def mirrorRepository(sourceRepos, targetRepos, cfg,
                     test = False, sync = False, syncSigs = False):
    # find the latest timestamp stored on the target mirror
    if sync:
        currentMark = -1
        targetRepos.setMirrorMark(cfg.host, currentMark)
    else:
        currentMark = targetRepos.getMirrorMark(cfg.host)
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

    # prepare a new max mark to be used when we need to break out of a loop
    crtMaxMark = max(x[0] for x in troveList)
    crtTroveLen = len(troveList)
    if not crtTroveLen:
        # this should be the end - no more troves to look at
        return 0
    if currentMark > 0 and crtMaxMark == currentMark:
        # if we're hung on the current max then we need to
        # forcibly advance the mark in case we're stuck
        crtMaxMark += 1 # only used if we filter out all troves below

    # we're trying to "weed out" troves that don't belong on the configured labels.
    if cfg.labels:
        troveList = [ x for x in troveList if
                            x[1][1].branch().label() in cfg.labels ]
        log.debug("after label filtering %d troves are needed", len(troveList))

    if len(troveList):
        # now filter the ones already existing
        troveList = filterAlreadyPresent(targetRepos, troveList)
        log.debug("found %d troves not present in the mirror", len(troveList))

    # if we were returned troves, but we filtered them all out, advance the
    # mark and signal "try again"
    if len(troveList) == 0 and crtTroveLen:
        # we had troves and now we don't
        log.debug("no troves found for our label %s" % cfg.labels)
        log.debug("advancing newMark to %d" % int(crtMaxMark))
        targetRepos.setMirrorMark(cfg.host, crtMaxMark)
        # try again
        return -1

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

    for i, bundle in enumerate(bundles):
        (outFd, tmpName) = util.mkstemp()
        os.close(outFd)
        jobList = [ x[1] for x in bundle ]
        log.debug("getting (%d of %d) %s" % (i + 1, len(bundles), jobList))
        cs = sourceRepos.createChangeSetFile(jobList, tmpName, recurse = False)
        # XXX it's a shame we can't give a hint as to what server to use
        # to avoid having to open the changeset and read in bits of it
        if test:
            log.debug("(skipping commit due to test mode)")
        else:
            log.debug("committing")
            try:
                targetRepos.commitChangeSetFile(tmpName, mirror = True)
            except errors.InternalServerError:
                log.debug("relative changeset could not be committed")
                jobList = [(x[0], (None, None), x[2], x[3]) for x in jobList]
                log.debug("getting absolute changeset %s", jobList)
                # try again as an absolute changeset
                cs = sourceRepos.createChangeSetFile(jobList, tmpName,
                                                     recurse = False)
                log.debug("committing absolute changeset")
                targetRepos.commitChangeSetFile(tmpName, mirror = True)
        os.unlink(tmpName)
        updateCount += len(bundle)
    else: # only when we're all done looping advance mark to the new max
        # compute the max mark of the bundles we comitted
        crtMaxMark = max([max([x[0] for x in bundle]) for bundle in bundles])
        log.debug("setting the mirror mark to %d", int(crtMaxMark))
        targetRepos.setMirrorMark(cfg.host, crtMaxMark)
    return updateCount
