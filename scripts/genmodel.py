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

import itertools, fcntl, os, smtplib, sys, tempfile
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart

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

from conary import conarycfg, conaryclient, errors, trove, versions
from conary.conaryclient import modelupdate, systemmodel
from conary.deps import deps
from conary.trovetup import TroveSpec

# pyflakes=ignore
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
                except errors.TroveSpecsNotFound:
                    pass

        for action in actionList:
            action.troveSpecs = origSpecs.pop(0)
            action.outSet = origResults.pop(0)

        return result

modelupdate.SysModelFindAction = TrackFindAction

def buildJobs(client, cache, model):
    print "====== Candidate model " + "=" * 55
    print "\t" + "\n\t".join(model.iterFormat())

    TrackFindAction.findMap = {}
    updJob = client.newUpdateJob()
    ts = client.systemModelGraph(model)
    client._updateFromTroveSetGraph(updJob, ts, cache, ignoreMissingDeps = True)

    return list(itertools.chain(*updJob.getJobs())), updJob

def orderByPackage(jobList):
    installMap = {}
    eraseMap = {}
    for job in jobList:
        if trove.troveIsGroup(job[0]) and job[1][0] is None:
            # skip groups we've decided to install
            continue

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


def getAnswer(question):
    print question,
    sys.stdout.flush()
    return raw_input()

def findParent(client, path, grpName, latest = False):
    releaseTroves = list(client.getDatabase().iterTrovesByPath(path))
    assert(len(releaseTroves) == 1)

    possibleGroupList = client.getRepos().getTroveReferences(
            releaseTroves[0].getVersion().trailingLabel().getHost(),
            [ releaseTroves[0].getNameVersionFlavor() ] )[0]

    possibleGroupList = sorted(
            [ x for x in possibleGroupList if x[0] == grpName ],
            groupDataCompare)

    if latest:
        return possibleGroupList[-1]

    return possibleGroupList[0]

def groupDataCompare(tup1, tup2):
    ver1 = tup1[1]
    ver2 = tup2[1]

    return cmp(ver1.trailingRevision().getVersion(),
               ver2.trailingRevision().getVersion())

def initialForesightModel(installedTroves, model):
    allGroupTups = [ x for x in installedTroves
                        if trove.troveIsGroup(x[0]) ]
    allGroupTroves = db.getTroves(allGroupTups)
    mainLabels = set()

    # simplistic, but we can't have loops in groups so good enough
    groupTroves = []
    for trv in allGroupTroves:
        # look for groups first, and eliminate groups which are included in
        # the other groups we find
        includedElsewhere = False
        for otherTrv in allGroupTroves:
            if (otherTrv.isStrongReference(*trv.getNameVersionFlavor()) and
                   otherTrv.includeTroveByDefault(*trv.getNameVersionFlavor())):
                includedElsewhere = True
                break

        if not includedElsewhere:
            groupTroves.append(trv)

    trv = None
    if ('group-gnome-dist' in [ x[0] for x in allGroupTups ]):
        trv = [ x for x in allGroupTroves
                    if x.getName() == 'group-gnome-dist' ][0]
    elif ('group-kde-dist' in [ x[0] for x in allGroupTups ]):
        trv = [ x for x in allGroupTroves
                    if x.getName() == 'group-kde-dist' ][0]
    if trv:
        if 'x86_64' in str(trv.getFlavor()):
            model.appendTroveOp(systemmodel.SearchTrove(
                    item = TroveSpec('group-world', fmtVer(trv.getVersion()),
                                     'is:x86' ) ))
        model.appendTroveOp(systemmodel.SearchTrove(
                item = TroveSpec('group-world', fmtVer(trv.getVersion()),
                                 str(trv.getFlavor()) ) ) )
        mainLabels.add(trv.getVersion().trailingLabel().asString())

    for trv in groupTroves:
        model.appendTroveOp(systemmodel.InstallTroveOperation(
                item = [ TroveSpec(trv.getName(),
                                   fmtVer(trv.getVersion()),
                                   str(trv.getFlavor())) ] ))

    return mainLabels

def initialRedHatModel(client, model):
    groupOs = findParent(client, "/etc/redhat-release", "group-os")
    groupRpath = findParent(client, "/usr/bin/conary", "group-rpath-packages",
			    latest = True)
    mainLabels = set()

    model.appendTroveOp(systemmodel.SearchTrove(
                                item = TroveSpec(groupOs[0],
                                                 fmtVer(groupOs[1]),
                                                 str(groupOs[2]))))
    mainLabels.add(groupOs[1].trailingLabel().asString())
    model.appendTroveOp(systemmodel.SearchTrove(
                                item = TroveSpec(groupRpath[0],
                                                 fmtVer(groupRpath[1]),
                                                 str(groupRpath[2]))))
    mainLabels.add(groupRpath[1].trailingLabel().asString())

    if 'rhel' in groupOs[1].asString():
        model.appendTroveOp(systemmodel.InstallTroveOperation(
                item = [ TroveSpec("group-rhel-standard",
                                   fmtVer(groupOs[1]),
                                   str(groupOs[2])) ] ))
    else:
        model.appendTroveOp(systemmodel.InstallTroveOperation(
                item = [ TroveSpec("group-standard",
                                   fmtVer(groupOs[1]),
                                   str(groupOs[2])) ] ))

    print "\t" + "\n\t".join(model.iterFormat())

    return mainLabels

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

    model = systemmodel.SystemModelText(cfg)
    if os.path.exists("/etc/redhat-release"):
        mainLabels = initialRedHatModel(client, model)
        componentPriorities = [ ( 'rpm', ),
                                ( 'runtime', ),
                                ( 'lib', ) ]
    else:
        mainLabels = initialForesightModel(installedTroves, model)
        componentPriorities = [ ( 'runtime', 'doc' ),
                                ( 'doc', ),
                                ( 'runtime', ),
                                ( 'user', 'group' ),
                                ( 'user', ),
                                ( 'group', ),
                                ( 'devel', ),
                                ( 'python', ),
                                ( 'java', ),
                                ( 'perl', ) ]

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
        for priorityList in componentPriorities:
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
    for searchItem in [x for x in model.systemItems
                       if isinstance(x, systemmodel.SearchTrove)]:
        finalModel.appendTroveOp(searchItem)

    searchTroveItems = []
    deferredItems = []
    specClass = None
    searchOps = (systemmodel.UpdateTroveOperation,
                 systemmodel.InstallTroveOperation)

    searchTroveSpecs = itertools.chain(
        *(op.item for op in model.systemItems if isinstance(op, searchOps))
    )
    searchNames = [x.name.split(':')[0] for x in searchTroveSpecs]
    searchNameCount = dict((x, searchNames.count(x)) for x in set(searchNames))

    def emitDeferred(specClass, deferredItems):
        if deferredItems:
            # list() to copy
            finalModel.appendTroveOp(specClass(item=list(deferredItems)))
            deferredItems[:] = []

    for op in model.systemItems:
        if specClass and specClass != op.__class__:
            # we can only combine items from the same class
            emitDeferred(specClass, deferredItems)

        specClass = op.__class__
        if isinstance(op, systemmodel.SearchTrove):
            # already handled above
            continue

        newSpecs = []
        simpleSpecs = [ (TrackFindAction.findMap.get(spec, spec), spec)
                        for spec in op ]
        for newSpec, spec in simpleSpecs:
            # any remaining versions belong by default in search items
            # if they did not come from the groups, so that they are
            # snapshotted in an updateall like other items.  This will
            # preserve the semantics of branch affinity relative to what
            # would have happened in the old update model.  Note that
            # what we really want to test for is whether the updates
            # came in through the previous search path, but we do not
            # have that information, so we assume that if they are on
            # the mainLabels they came in through the search path that
            # already exists and don't add another entry, but leave
            # the fixed version for the user to clarify.  Local cooks
            # don't really have anything to update to, so leave them
            # alone.
            if 'local@' in spec.version:
                # never simplify any local versions
                newSpecs.append(spec);
            elif specClass in searchOps and newSpec.version is not None:
                searchTroveName = newSpec.name.split(':')[0]
                searchTroveSpec = TroveSpec(searchTroveName,
                                            spec.version, newSpec.flavor)
                if searchTroveSpec not in searchTroveItems:
                    emitDeferred(specClass, deferredItems)
                    finalModel.appendTroveOp(
                        systemmodel.SearchTrove(item=searchTroveSpec))
                    searchTroveItems.append(searchTroveSpec)

                if searchNameCount.get(searchTroveName, 0) > 1:
                    # if there is more than one of this name, assume they
                    # might be differentiated by flavor
                    flavor = spec.flavor
                else:
                    # use the simplified flavor
                    flavor = newSpec.flavor

                newSpecs.append(TroveSpec(newSpec.name, None, flavor))
            else:
                newSpecs.append(newSpec);

        if len(set([x.name.split(':')[0] for x in newSpecs+deferredItems])) > 1:
            # newSpecs has trove names not mentioned in deferredItems,
            # so do not combine
            emitDeferred(specClass, deferredItems)

        deferredItems.extend(newSpecs)

        if set([':' in x.name for x in newSpecs]) != set((True,)):
            # something other than a component is listed; don't collapse
            # any more
            emitDeferred(specClass, deferredItems)

    if deferredItems:
        emitDeferred(specClass, deferredItems)

    TrackFindAction.remap = False
    finalJob, uJob = buildJobs(client, cache, finalModel)
    assert(set(finalJob) == set(candidateJob))

    # Add comments to the model itself
    for commentline in (
        'This file is an attempt to describe an existing system.',
        'It is intended to describe the "meaning" of the installed system.',
        '',
        'After this file is installed as /etc/conary/system-model any',
        'following conary update/install/erase operations will be done',
        'by modifying this file, then building a representation of the',
        'new desired state of your local system described in the modified',
        'file, and then updating your system to that new state.',
        '',
        'It is reasonable to edit this file with a text editor.',
        'Conary will preserve whole-line comments (like this one)',
        'when it edits this file, so you may use comments to describe',
        'the purpose of your modifications.',
        '',
        'After you edit this file, run the command',
        '  conary sync',
        'This command will move your system to the state you have',
        'described by your edits to this file, or will tell you',
        'about errors you have introduced so that you can fix them.',
        '',
        'The "install" and "update" lines are relative only to things',
        'mentioned earlier in this model, not relative to what has been',
        'previously installed on your system.',
        '',
        ):
        finalModel.appendNoOpByText('# %s' % commentline, modified=False)

    if finalJob:
        from conary.cmds import updatecmd
        sys.stdout.flush()
        outfd, outfn = tempfile.mkstemp()
        os.unlink(outfn)
        stdout = os.dup(sys.stdout.fileno())
        os.dup2(outfd, sys.stdout.fileno())
        fcntl.fcntl(stdout, fcntl.F_SETFD, 1)
        try:
            updatecmd.displayUpdateInfo(uJob, cfg)
        finally:
            sys.stdout.flush()
            os.dup2(stdout, sys.stdout.fileno())
            os.close(stdout)
        os.lseek(outfd, 0, 0)
        f = os.fdopen(outfd, 'r')
        jobData = f.read()
        f.close()
        for commentline in [
            'Some of the troves on this system are not represented in the',
            'following model, most likely because they appear to have been',
            'included only to satisfy dependencies.  Please review the',
            'following data and edit the system model to represent the',
            'troves that you wish to have installed on your system.',
            '',
            'The following additional operations would be needed to make the',
            'system match the model, and would be applied to the system by ',
            'a "conary sync" operation:'] + jobData.split('\n') + ['']:
            finalModel.appendNoOpByText('# %s' % commentline, modified=False)

    print "----"
    print "Final Model"
    print "\t" + "\n\t".join(finalModel.iterFormat())


    answer = getAnswer('Write model to disk? [y/N]:')
    if answer and answer[0].lower() == 'y':
        # SystemModelFile wants to parse -- don't let it, in case we are
        # testing on a system that already has a model defined...
        tempModel = systemmodel.SystemModelText(cfg)
        smf = systemmodel.SystemModelFile(tempModel)
        smf.model = finalModel
        try:
            smf.write()
            print 'model written to %s' % smf.fileName
        except:
            outfd, outfn = tempfile.mkstemp()
            smf.write(fileName=outfn)
            print 'model written to %s' % outfn


    # offer to send debugging data to rPath
    answer = getAnswer(
        'Send your system manifest, model, and unmodeled operation report\n'
        "to rPath for rPath's debugging use? [y/N]:")
    if answer and answer[0].lower() == 'y':
        mailhost = getAnswer('What is your mail host? (localhost):')
        if not mailhost:
            mailhost='localhost'
        sender = ''
        while not sender:
            sender = getAnswer('Send from what email address?')

        recipient = 'sysmodel-report@rpath.com'

        s = smtplib.SMTP(mailhost)
        msg = MIMEMultipart()
        msg['Subject'] = 'Conary sysmodel data report'
        msg['From'] = sender
        msg['To'] = recipient
        msg.preamble = 'System model genmodel report'

        manifest = '\n'.join(sorted('%s=%s[%s]' %x for x in db.iterAllTroves()))
        manifestText = MIMEText(manifest, 'plain')
        manifestText.add_header('Content-Disposition', 'attachment',
            filename='manifest')
        msg.attach(manifestText)

        modelText = MIMEText(finalModel.format(), 'plain')
        modelText.add_header('Content-Disposition', 'attachment',
            filename='model')
        msg.attach(modelText)

        if finalJob:
            jobText = MIMEText(jobData, 'plain')
            jobText.add_header('Content-Disposition', 'attachment',
                filename='finalJob')
            msg.attach(jobText)

        s.sendmail(sender, recipient, msg.as_string())
        s.quit()
