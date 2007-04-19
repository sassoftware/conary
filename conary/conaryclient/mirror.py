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

import fcntl
import itertools
import optparse
import os
import sys

from conary.conaryclient import callbacks as clientCallbacks
from conary import conarycfg, callbacks, trove
from conary.lib import cfg, util, log
from conary.repository import errors, changeset, netclient

class OptionError(Exception):
    def __init__(self, errcode, errmsg, *args):
        self.errcode = errcode
        self.errmsg = errmsg
        Exception.__init__(self, *args)

def parseArgs(argv):
    parser = optparse.OptionParser(version = '%prog 0.1')
    parser.add_option("--config-file", dest = "configFile",
                      help = "configuration file", metavar = "FILE")
    parser.add_option("--full-sig-sync", dest = "syncSigs",
                      action = "store_true", default = False,
                      help = "replace all of the trove signatures on "
                             "the target repository")
    parser.add_option("--full-trove-sync", dest = "sync", action = "store_true",
                      default = False,
                      help = "ignore the last-mirrored timestamp in the "
                             "target repository")
    parser.add_option("--test", dest = "test", action = "store_true",
                      default = False,
                      help = "skip commiting changes to the target repository")
    parser.add_option("-v", "--verbose", dest = "verbose",
                      action = "store_true", default = False,
                      help = "display information on what is going on")

    (options, args) = parser.parse_args(argv)

    if options.configFile is None:
        raise OptionError(1, 'a mirror configuration must be provided')
    elif args:
        raise OptionError(1, 'unexpected arguments: %s' % " ".join(args))

    return options

class VerboseChangesetCallback(clientCallbacks.ChangesetCallback):
    def done(self):
        self._message('\r')

class MirrorConfigurationSection(cfg.ConfigSection):
    repositoryMap         =  conarycfg.CfgRepoMap
    user                  =  conarycfg.CfgUserInfo

class MirrorConfiguration(cfg.SectionedConfigFile):
    host                  =  cfg.CfgString
    entitlementDirectory  =  cfg.CfgPath
    labels                =  conarycfg.CfgInstallLabelPath
    matchTroves           =  cfg.CfgSignedRegExpList
    recurseGroups         =  (cfg.CfgBool, False)
    uploadRateLimit       =  (conarycfg.CfgInt, 0)
    downloadRateLimit     =  (conarycfg.CfgInt, 0)
    lockFile              =  cfg.CfgString

    _allowNewSections = True
    _defaultSectionType = MirrorConfigurationSection
    
    def __init__(self):
        cfg.SectionedConfigFile.__init__(self)


def Main(argv=sys.argv[1:]):
    try:
        options = parseArgs(argv)
    except OptionError, e:
        sys.stderr.write(e.errmsg)
        sys.stderr.write("\n")
        return e.errcode

    cfg = MirrorConfiguration()
    cfg.read(options.configFile, exception = True)
    callback = callbacks.ChangesetCallback()

    if options.verbose:
        log.setVerbosity(log.DEBUG)
        callback = VerboseChangesetCallback()

    if cfg.lockFile:
        try:
            log.debug('checking for lock file')
            lock = open(cfg.lockFile, 'w')
            fcntl.lockf(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        except IOError:
            log.debug('lock held by another process, exiting')
            sys.exit(0)

    # need to make sure we have a 'source' section
    if not cfg.hasSection('source'):
        log.debug("ERROR: mirror configuration file is missing a [source] section")
        sys,exit(-1)
    srcCfg = cfg.getSection('source')
    sourceRepos = netclient.NetworkRepositoryClient(
        srcCfg.repositoryMap, srcCfg.user,
        uploadRateLimit = cfg.uploadRateLimit,
        downloadRateLimit = cfg.downloadRateLimit,
        entitlementDir = cfg.entitlementDirectory)
    # we need to build a target repo client for each of the "target*"
    # sections in the config file
    targets = []
    for name in cfg.iterSectionNames():
        if not name.startswith("target"):
            continue
        secCfg = cfg.getSection(name)
        target = netclient.NetworkRepositoryClient(
            secCfg.repositoryMap, secCfg.user,
            uploadRateLimit = cfg.uploadRateLimit,
            downloadRateLimit = cfg.downloadRateLimit,
            entitlementDir = cfg.entitlementDirectory)
        target = TargetRepository(target, cfg, name, test=options.test)
        targets.append(target)
    # we pass in the sync flag only the first time around, because after
    # that we need the targetRepos mark to advance accordingly after being
    # reset to -1
    callAgain = mirrorRepository(sourceRepos, targets, cfg,
                                 test = options.test, sync = options.sync,
                                 syncSigs = options.syncSigs,
                                 callback = callback)
    while callAgain:
        callAgain = mirrorRepository(sourceRepos, targets, cfg,
                                     test = options.test, callback = callback)


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

# mirroring stuff when we are running into PathIdConflict errors
def splitJobList(jobList, src, targetSet, callback = None):
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
        cs = src.createChangeSetFile(smallJobList, tmpName, recurse = False,
                                     callback = callback)
        for target in targetSet:
            target.commitChangeSetFile(tmpName, callback = callback)
        os.unlink(tmpName)
        callback.done()
        i += 1
    return

def mirrorSignatures(sourceRepos, targets, currentMark, cfg, syncSigs=False):
    for t in targets:
        t.mirrorGPG(sourceRepos, cfg.host)
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
        sigList = sourceRepos.getNewSigList(cfg.host, str(currentMark))
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
    if not sigList:
        return 0
    
    updateCount = 0
    log.debug("downloading %d signatures from source repository", len(sigList))
    sigs = sourceRepos.getTroveSigs([ x[1] for x in sigList ])
    # update these sigs on each target
    for t in targets:
        updateCount += t.setTroveSigs(sigList, sigs)
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

# target repo class that helps dealing with testing mode
class TargetRepository:
    def __init__(self, repo, cfg, name = 'target', test=False):
        self.repo = repo
        self.test = test
        self.cfg = cfg
        self.__mark = None
        self.name = name
        self.__gpg = {}
    def getMirrorMark(self):
        if self.__mark is None:
            self.__mark = self.repo.getMirrorMark(self.cfg.host)
        self.__mark = str(int(self.__mark))
        return self.__mark
    def setMirrorMark(self, mark):
        self.__mark = str(int(mark))
        log.debug("%s setting mirror mark to %s", self.name, self.__mark)
        if self.test:
            return
        self.repo.setMirrorMark(self.cfg.host, self.__mark)
    def mirrorGPG(self, src, host):
        if self.__gpg.has_key(host):
            return
        keyList = src.getNewPGPKeys(host, -1)
        self.__gpg[host] = keyList
        if not len(keyList):
            return
        log.debug("%s adding %d gpg keys", self.name, len(keyList))
        if self.test:
            return
        self.repo.addPGPKeyList(self.cfg.host, keyList)
    def setTroveSigs(self, sigList, sigs):
        log.debug("%s checking which trove sigs are required", self.name)
        # Sigs whose mark is the same as currentMark might not have their trove
        # available on the server (it might be coming as part of this mirror
        # run). sigList has to be sorted by mark for this to work.
        inQuestion = [ x[1] for x in sigList if x[0] == self.__mark ]
        present = self.repo.hasTroves(inQuestion)
        # build the ((n,v,f), signature) list only for the troves that have signatures
        setSigs = [ (x[0][1], x[1]) for x in itertools.izip(sigList, sigs) if len(x[1]) > 0 ]
        # filter out the ones that are not present
        setSigs = [ x for x in setSigs if present.get(x[0], True) ]
        log.debug("%s uploading %d signatures", self.name, len(sigs))
        if self.test:
            return 0
        self.repo.setTroveSigs(setSigs)
        return len(setSigs)
    def addTroveList(self, tl):
        # Filter out troves which are already in the local repository. Since
        # the marks aren't distinct (they increase, but not monotonially), it's
        # possible that something new got committed with the same mark we
        # last updated to, so we have to look again at all of the troves in the
        # source repository with the last mark which made it into our target.
        present = self.repo.hasTroves([ x[1] for x in tl ], hidden = True)
        ret = [ x for x in tl if not present[x[1]] ]
        log.debug("%s found %d troves not present", self.name, len(ret))
        return ret
    def commitChangeSetFile(self, filename, callback):
        if self.test:
            return 0
        log.debug("%s committing", self.name)
        ret = self.repo.commitChangeSetFile(filename, mirror=True, callback=callback)
        callback.done()
        return ret
    
# split a troveList in changeset jobs
def buildBundles(target, troveList):
    bundles = []
    log.debug("grouping %d troves based on version and flavor", len(troveList))
    groupList = groupTroves(troveList)
    log.debug("building grouped job list")
    bundles = buildJobList(target.repo, groupList)
    return bundles

def mirrorRepository(sourceRepos, targetRepos, cfg,
                     test = False, sync = False, syncSigs = False,
                     callback = callbacks.ChangesetCallback()):
    if not hasattr(targetRepos, '__iter__'):
        targetRepos = [ targetRepos ]
    targets = []
    for t in targetRepos:
        if isinstance(t, netclient.NetworkRepositoryClient):
            targets.append(TargetRepository(t, cfg, test=test))
        elif isinstance(t, TargetRepository):
            targets.append(t)
        else:
            raise RuntimeError("Can not handle unknown target repository type", t)
    if sync:
        currentMark = -1
    else:
        marks = [ int(t.getMirrorMark()) for t in targets ]
        # we use the oldest mark as a starting point (since we have to
        # get stuff from source for that oldest one anyway)
        currentMark = min(marks)
    log.debug("using common mirror mark %s", currentMark)
    # reset mirror mark to the lowest common denominator
    for t in targets:
        if int(t.getMirrorMark()) != currentMark:
            t.setMirrorMark(currentMark)
    # first, we need to mirror changed signatures for troves in each target
    updateCount = mirrorSignatures(sourceRepos, targets, currentMark, cfg, syncSigs)

    # FIXME: getNewTroveList should accept and only return troves on
    # the labels we're interested in
    log.debug("looking for new troves")
    # make sure we always treat the mark as an integer
    troveList = [(int(m), (n,v,f), t) for m, (n,v,f), t in
                  sourceRepos.getNewTroveList(cfg.host, str(currentMark))]
    # we need to protect ourselves from duplicate items in the troveList
    troveList = list(set(troveList))
    if not len(troveList):
        # this should be the end - no more troves to look at
        return 0
    
    # sort by mark
    troveList.sort(lambda a,b: cmp(a[0], b[0]))
    log.debug("%d new troves returned", len(troveList))

    # We cut off the last troves that have the same flavor, version to
    # avoid committing an incomplete trove. This could happen if the
    # server side only listed some of a trove's components due to
    # server side limits on how many results it can return on each query
    lastIdx = len(troveList) - 1
    # compare with the last one
    ml, (nl,vl,fl), tl = troveList[-1]
    while lastIdx >= 0:
        m, (n,v,f), t = troveList[lastIdx]
        if v == vl and f == fl:
            lastIdx -= 1
        else:
            break
    # if all troves in troveList have the same version/flavor, do nothing
    if lastIdx >= 0:
        troveList = troveList[:lastIdx+1]
        log.debug("reduced new trove list to %d to avoid partial commits", len(troveList))
    # prepare a new max mark to be used when we need to break out of a loop
    crtMaxMark = max(x[0] for x in troveList)
    if currentMark > 0 and crtMaxMark == currentMark:
        # if we're hung on the current max then we need to
        # forcibly advance the mark in case we're stuck
        crtMaxMark += 1 # only used if we filter out all troves below
    initTLlen = len(troveList)
    
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
            for t in targets:
                t.mirrorGPG(src, host)

    # we check which troves from the troveList are needed on each
    # target and we split the troveList into separate lists depending
    # on how many targets require each
    byTarget = {}
    targetSetList = []
    if len(troveList):
        byTrove = {}
        for i, target in enumerate(targets):
            for t in target.addTroveList(troveList):
                bt = byTrove.setdefault(t, set())
                bt.add(i)
        # invert the dict by target now
        for trv, ts in byTrove.iteritems():
            targetSet = [ targets[i] for i in ts ]
            try:
                targetIdx = targetSetList.index(targetSet)
            except ValueError:
                targetSetList.append(targetSet)
                targetIdx = len(targetSetList)-1
            bt = byTarget.setdefault(targetIdx, [])
            bt.append(trv)
        del byTrove
    # if we were returned troves, but we filtered them all out, advance the
    # mark and signal "try again"
    if len(byTarget) == 0 and len(removedSet) == 0 and initTLlen:
        # we had troves and now we don't
        log.debug("no troves found for our label %s" % cfg.labels)
        for t in targets:
            t.setMirrorMark(crtMaxMark)
        # try again
        return -1
    
    # now we get each section of the troveList for each targetSet. We
    # start off mirroring by those required by fewer targets, using
    # the assumption that those troves are what is required for the
    # targets to catch up to a common set
    if len(byTarget) > 1:
        log.debug("split %d troves into %d chunks by target", len(troveList), len(byTarget))
    # sort the targetSets by length
    targetSets = list(enumerate(targetSetList))
    targetSets.sort(lambda a,b: cmp(len(a[1]), len(b[1])))
    bundlesMark = 0
    for idx, targetSet in targetSets:
        troveList = byTarget[idx]
        if not troveList: # XXX: should notn happen...
            continue
        log.debug("mirroring %d troves into %d targets", len(troveList), len(targetSet))
        # since these troves are required for all targets, we can use
        # the "first" one to build the relative changeset requests
        target = list(targetSet)[0]
        bundles = buildBundles(target, troveList)

        for i, bundle in enumerate(bundles):
            jobList = [ x[1] for x in bundle ]
            # XXX it's a shame we can't give a hint as to what server to use
            # to avoid having to open the changeset and read in bits of it
            if test:
                log.debug("test mode: not mirroring (%d of %d) %s" % (i + 1, len(bundles), jobList))
                updateCount += len(bundle)
                continue
            (outFd, tmpName) = util.mkstemp()
            os.close(outFd)
            log.debug("getting (%d of %d) %s" % (i + 1, len(bundles), displayBundle(bundle)))
            try:
                cs = sourceRepos.createChangeSetFile(jobList, tmpName, recurse = False,
                                                     callback = callback)
            except changeset.ChangeSetKeyConflictError, e:
                splitJobList(jobList, sourceRepos, targetSet, callback = callback)
            else:
                for target in targetSet:
                    target.commitChangeSetFile(tmpName, callback=callback)
            try:
                os.unlink(tmpName)
            except OSError:
                pass
            callback.done()
        updateCount += len(bundle)
        # compute the max mark of the bundles we comitted
        mark = max([min([x[0] for x in bundle]) for bundle in bundles])
        if mark > bundlesMark:
            bundlesMark = mark
    else: # only when we're all done looping advance mark to the new max
        if bundlesMark == 0 or bundlesMark == currentMark:
            bundlesMark = crtMaxMark # avoid repeating the same query...
        for target in targetRepos:
            target.setMirrorMark(bundlesMark)
    # mirroring removed troves requires one by one processing
    for target in targets:
        updateCount += mirrorRemoved(sourceRepos, target.repo, removedSet,
                                     test=test, callback=callback)
    return updateCount
