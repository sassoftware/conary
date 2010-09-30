#!/usr/bin/python
#
# Copyright (C) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import itertools, os, sys

if os.path.dirname(sys.argv[0]) != ".":
    if sys.argv[0][0] == "/":
        fullPath = os.path.dirname(sys.argv[0])
    else:
        fullPath = os.getcwd() + "/" + os.path.dirname(sys.argv[0])
else:
    fullPath = os.getcwd()

sys.path.insert(0, os.path.dirname(fullPath))
from conary.lib import util
sys.excepthook = util.genExcepthook(debug=True)

from conary import conarycfg, conaryclient, trove, versions
from conary.conaryclient import modelupdate, systemmodel
from conary.deps import deps
from conary.trovetup import TroveSpec

from conary.lib import log

OrigFindAction = modelupdate.SysModelFindAction
class TrackFindAction(OrigFindAction):

    findMap = {}
    remap = False

    def __call__(self, actionList, data):
        result = OrigFindAction.__call__(self, actionList, data)
        if not self.remap:
            return result

        origSpecs = []
        origResults = []
        from conary.conaryclient import troveset
        for action in actionList:
            origSpecs.append(action.troveSpecs)
            origResults.append(action.outSet)

            assert(len(action.troveSpecs) == 1)
            o = action.troveSpecs[0]
            for attempt in [ TroveSpec(o[0], None, None),
                             TroveSpec(o[0], None, o[2]),
                             TroveSpec(o[0], o[1], None) ]:
                if attempt[1] and 'local' in attempt[1]:
                    continue

                action.troveSpecs = [ attempt ]
                action.outSet = troveset.TroveTupleSet(graph = action.outSet.g)

                try:
                    OrigFindAction.__call__(self, [ action ], data)
                    if (action.outSet.installSet == origResults[-1].installSet):
                        self.findMap.update( (x, attempt)
                                                for x in origSpecs[-1] )
                        break
                except troveset.MissingTroves:
                    pass

        for action in actionList:
            action.troveSpecs = origSpecs.pop(0)
            action.outSet = origResults.pop(0)

        return result

modelupdate.SysModelFindAction = TrackFindAction

def buildJobs(client, cache, model):
    print "====== Candidate model " + "=" * 55
    print "\t" + "\n\t".join(x[:-1] for x in model.iterFormat())

    TrackFindAction.findMap = {}
    updJob = client.newUpdateJob()
    ts = client.systemModelGraph(model)
    client._updateFromTroveSetGraph(updJob, ts, cache, ignoreMissingDeps = True)

    return list(itertools.chain(*updJob.getJobs())), updJob

def orderByPackage(jobList):
    installMap = {}
    eraseMap = {}
    for job in jobList:
        assert(not trove.troveIsFileSet(job[0]))
        assert(not trove.troveIsGroup(job[0]))

        pkgName = job[0].split(":")[0]
        if job[1][0] is not None:
            packageMap = installMap
            pkgTuple = (pkgName, job[1][0], job[1][1])
        else:
            packageMap = eraseMap
            pkgTuple = (pkgName, job[2][0], job[2][1])

        packageMap.setdefault(pkgTuple, [])
        packageMap[pkgTuple].append(job)

    return installMap, eraseMap

def fmtVer(v):
    if v.isOnLocalHost():
        return v.asString()

    return "%s/%s" % (v.trailingLabel(), v.trailingRevision())

def addInstallJob(model, job):
    if job[2][1] is not None:
        newOp = systemmodel.UpdateTroveOperation(
                item = [ TroveSpec(job[0], fmtVer(job[1][0]),
                                     str(job[1][1])) ] )
    else:
        newOp = systemmodel.InstallTroveOperation(
                item = [ TroveSpec(job[0], fmtVer(job[1][0]),
                                     str(job[1][1])) ] )

    if newOp not in model.systemItems:
        model.appendTroveOp(newOp)
        updatedModel = True
    else:
        updatedModel = False

    return updatedModel

def addEraseJob(model, job):
    newOp = systemmodel.EraseTroveOperation(
                item = [ TroveSpec(job[0], job[2][0].asString(),
                                   str(job[2][1])) ])

    if newOp not in model.systemItems:
        model.appendTroveOp(newOp)
        updatedModel = True
    else:
        updatedModel = False

    return updatedModel

if __name__ == '__main__':
    #log.setVerbosity(log.INFO)

    cfg = conarycfg.ConaryConfiguration(readConfigFiles = True)
    cfg.initializeFlavors()

    client = conaryclient.ConaryClient(cfg = cfg)
    db = client.getDatabase()

    cu = db.db.db.cursor()
    cu.execute("select troveName,version,flavor from versions join instances using (versionid) join flavors using (flavorid) where version like '%local@%' and isPresent=1")
    localTroves = set([ (x[0], versions.VersionFromString(x[1]),
                         deps.ThawFlavor(x[2])) for x in cu ])

    cache = modelupdate.SystemModelTroveCache(db, client.getRepos())
    cache.load("/var/lib/conarydb/modelcache")
    cache.cacheTroves(localTroves)

    installedTroves = dict( (tup, pinned) for (tup, pinned)
                                in db.iterAllTroves(withPins = True) )

    # look for groups first, and eliminate groups which are included in
    # the other groups we find
    allGroupTups = [ x for x in installedTroves if trove.troveIsGroup(x[0]) ]
    allGroupTroves = db.getTroves(allGroupTups)

    # simplistic, but we can't have loops in groups so good enough
    groupTroves = []
    for trv in allGroupTroves:
        includedElsewhere = False
        for otherTrv in allGroupTroves:
            if (otherTrv.isStrongReference(*trv.getNameVersionFlavor()) and
                   otherTrv.includeTroveByDefault(*trv.getNameVersionFlavor())):
                includedElsewhere = True
                break

        if not includedElsewhere:
            groupTroves.append(trv)

    model = systemmodel.SystemModelText(cfg)

    if ('group-gnome-dist' in [ x[0] for x in allGroupTups ]):
        trv = [ x for x in allGroupTroves
                    if x.getName() == 'group-gnome-dist' ][0]
        model.appendToSearchPath(systemmodel.SearchTrove(
                item = TroveSpec('group-world', fmtVer(trv.getVersion()),
                                 str(trv.getFlavor()) ) ) )
        model.appendToSearchPath(systemmodel.SearchTrove(
                item = TroveSpec('group-world', fmtVer(trv.getVersion()),
                                 'is:x86' ) ))

    for trv in groupTroves:
        model.appendTroveOp(systemmodel.InstallTroveOperation(
                item = [ TroveSpec(trv.getName(),
                                   fmtVer(trv.getVersion()),
                                   str(trv.getFlavor())) ] ))


    allCandidates = []
    updatedModel = True
    # remember that job order is backwards! it's trying to move from
    # what's there to what the model says; we want to undo those operations
    while updatedModel:
        candidateJob, uJob = buildJobs(client, cache, model)
        if candidateJob in allCandidates:
            break

        allCandidates.append(candidateJob)

        installPackageMap, erasePackageMap = orderByPackage(candidateJob)

        updatedModel = False

        # look for packages to install/update
        for priorityList in [ ( 'runtime', 'doc' ),
                              ( 'doc', ),
                              ( 'runtime', ),
                              ( 'devel', ),
                              ( 'python', ),
                              ( 'java', ),
                              ( 'perl', ) ]:
            for pkgTuple, jobList in installPackageMap.items():
                componentSet = set( [ (pkgTuple[0] + ":" + x,
                                       pkgTuple[1], pkgTuple[2])
                                      for x in priorityList ] )
                newInstalls = set([ (x[0], x[1][0], x[1][1]) for x in jobList ])
                if (componentSet - newInstalls):
                    # are all of the components we care about present
                    continue

                if pkgTuple in newInstalls:
                    print "   updating model for job", jobList
                    installJob = [ x for x in jobList if
                                   (x[0], x[1][0], x[1][1]) == pkgTuple ]
                    assert(len(installJob) == 1)
                    updatedModel = (addInstallJob(model, installJob[0]) or
                                    updatedModel)

            if updatedModel:
                # break out of the priorityList for loop
                break

        if updatedModel:
            continue

        # handle packageless changes
        for pkgTuple, jobList in installPackageMap.items():
            newInstalls = set([ (x[0], x[1][0], x[1][1]) for x in jobList ])

            if pkgTuple in newInstalls:
                continue

            if pkgTuple in installedTroves:
                # we have bits of this installed already; let's see if
                # installing the whole package helps
                pkgJob = ( pkgTuple[0], pkgTuple[1:], (None, None), False )
                
                changedModel = addInstallJob(model, pkgJob)
                if changedModel:
                    updatedModel = True
                    continue

            for compJob in jobList:
                updatedModel = (addInstallJob(model, compJob) or updatedModel)

    # handle erases separately
    erases = (set(erasePackageMap.keys())
                        - set(installPackageMap.keys()))
    for pkgTuple in erases:
        jobList = erasePackageMap[pkgTuple]
        pkgJobList = [ x for x in jobList if trove.troveIsPackage(x[0]) ]
        if pkgJobList:
            assert(len(pkgJobList) == 1)
            updatedModel = addEraseJob(model, pkgJobList[0]) or updatedModel
        else:
            for job in jobList:
                updatedModel = addEraseJob(model, job) or updatedModel

    TrackFindAction.remap = True
    candidateJob, uJob = buildJobs(client, cache, model)

    print "-----"
    print "simplification map"
    for big, little in TrackFindAction.findMap.iteritems():
        print "%s -> %s" % (big, little)

    finalModel = systemmodel.SystemModelText(cfg)
    for searchItem in model.searchPath:
        finalModel.appendToSearchPath(searchItem)

    for op in model.systemItems:
        newOp = op.__class__(item = [ TrackFindAction.findMap.get(spec, spec)
                                      for spec in op ] )
        finalModel.appendTroveOp(newOp)

    TrackFindAction.remap = False
    finalJob, uJob = buildJobs(client, cache, model)
    assert(finalJob == candidateJob)

    print "----"
    print "Final Model"
    print "\t" + "\n\t".join(x[:-1] for x in finalModel.iterFormat())

    if finalJob:
        print
        print "The following additional operations would be needed to make the"
        print "system match the model, and would be applied to the system by "
        print 'a "conary sync" operation:'

        from conary.cmds import updatecmd
        updatecmd.displayUpdateInfo(uJob, cfg)

