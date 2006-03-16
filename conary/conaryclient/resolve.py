#
# Copyright (c) 2004-2006 rPath, Inc.
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

import itertools

from conary import errors
from conary.repository import trovesource
from conary.deps import deps


class DependencySolver(object):

    def __init__(self, client, cfg, repos, db):
        self.client = client
        self.cfg = cfg
        self.repos = repos
        self.db = db

    def resolveDependencies(self, uJob, jobSet, split = False,
                            resolveDeps = True, useRepos = True):
        """
            Determine and possibly resolve dependency problems.
            @param uJob: update job we are resolving dependencies for
            @type uJob: local.database.UpdateJob
            @param jobSet: jobs that are to be applied
            @type jobSet: list of job tuples
            @param split: if True, find an ordering for the jobs in 
            jobSet plus any resolved jobs. 
            @type split: boolean
            @param resolveDeps: If True, try to resolve any dependency problems
            by modifying the given job.
            @type resolveDeps: boolean
            @param useRepos: If True, search for dependency solutions in the 
            repository after searching the update job search source.
        """

        troveSource = uJob.getSearchSource()
        if useRepos:
            troveSource = trovesource.stack(troveSource, self.repos)

        ineligible = set()

        pathIdx = 0
        (depList, cannotResolve, changeSetList, keepList, ineligible) \
                             = self.checkDeps(uJob, jobSet, troveSource,
                                              findOrdering = split,
                                              resolveDeps = resolveDeps,
                                              ineligible=ineligible)

        suggMap = {}

        if not resolveDeps:
            # we're not supposed to resolve deps here; just skip the
            # rest of this
            depList = []
            cannotResolve = []


        while (depList and self.cfg.installLabelPath
               and pathIdx < len(self.cfg.installLabelPath)):
            nextCheck = [ x[1] for x in depList ]
            sugg = troveSource.resolveDependencies(
                            self.cfg.installLabelPath[pathIdx], 
                            nextCheck)

            troves = set()

            for (troveName, depSet) in depList:
                if depSet in sugg:
                    suggList = set()
                    for choiceList in sugg[depSet]:
                        troveNames = set(x[0] for x in choiceList)

                        affTroveDict = \
                            dict((x, self.db.trovesByName(x))
                                              for x in troveNames)

                        # iterate over flavorpath -- use suggestions 
                        # from first flavor on flavorpath that gets a match 
                        for installFlavor in self.cfg.flavor:
                            choice = self.selectResolutionTrove(choiceList,
                                                                installFlavor,
                                                                affTroveDict)
                            if choice:
                                suggList.add(choice)
                                l = suggMap.setdefault(troveName, set())
                                l.add(choice)
                                break

                    troves.update([ (x[0], (None, None), x[1:], True)
                                    for x in suggList ])

            if troves:
                # We found good suggestions, merge in those troves. Items
                # which are being removed by the current job cannot be 
                # removed again.
                beingRemoved = set((x[0], x[1][0], x[1][1]) for x in
                                    jobSet if x[1][0] is not None )
                beingInstalled = set((x[0], x[2][0], x[2][1]) for x in
                                      jobSet if x[2][0] is not None )


                # add in foo if we are adding foo:lib.  That way 'conary
                # erase foo' will work as expected.
                if self.cfg.autoResolvePackages:
                    packageJobs = self.addPackagesForComponents(troves,
                                                                troveSource,
                                                                beingInstalled)
                    troves.update(packageJobs)

                newJob = self.client._updateChangeSet(troves, uJob,
                                                      keepExisting = False,
                                                      recurse = False,
                                                      ineligible = beingRemoved,
                                                      checkPrimaryPins = True)
                assert(not (newJob & jobSet))
                newJob = self.filterCrossBranchResolutions(newJob, troveSource)
                if not newJob:
                    # we had potential solutions, but they would have
                    # required implicitly switching the branch of a trove
                    # on a user, and we don't do that.
                    pathIdx += 1
                    continue

                jobSet.update(newJob)

                lastCheck = depList
                (depList, cannotResolve, changeSetList, newKeepList,
                 ineligible) =  self.checkDeps(uJob, jobSet, 
                                               uJob.getTroveSource(),
                                               findOrdering = split,
                                               resolveDeps = resolveDeps,
                                               ineligible = ineligible)
                keepList.extend(newKeepList)
                if lastCheck != depList:
                    pathIdx = 0
            else:
                # we didnt find any suggestions; go on to the next label
                # in the search path
                pathIdx += 1

        return (depList, suggMap, cannotResolve, changeSetList, keepList)

    def checkDeps(self, uJob, jobSet, trvSrc, findOrdering, 
                  resolveDeps, ineligible):
        """
            Given a jobSet, use its dependencies to determine an
            ordering, resolve problems with jobs that have difficult
            dependency problems immediately,
            and determine any missing dependencies.
        """

        keepList = []

        while True:
            (depList, cannotResolve, changeSetList) = \
                            self.db.depCheck(jobSet, uJob.getTroveSource(),
                                             findOrdering = findOrdering)
            if not resolveDeps or not cannotResolve:
                break
            # We have troves that are in the state cannotResolve:
            # This means that they are troves that are needed by
            # something else that we are trying to erase.

            # We attempt to solve them here in this inner loop because
            # they are more difficult to resolve, and resolving them may
            # affect resolving normal missing dependencies.

            changeMade = False

            # first: attempt to update packages that dependended on the missing
            # package.
            if self.cfg.resolveLevel > 1:
                cannotResolve, newJobSet = self.resolveEraseByUpdating(
                                                                trvSrc,
                                                                cannotResolve,
                                                                uJob, jobSet,
                                                                ineligible)
            if newJobSet:
                jobSet |= newJobSet
                changeMade = True

            if not changeMade and cannotResolve:
                # second: attempt to keep packages that were being erased
                cannotResolve, newKeepList = self.resolveEraseByKeeping(
                                                         trvSrc,
                                                         cannotResolve,
                                                         uJob, jobSet)
                if newKeepList:
                    changeMade = True
                    keepList += newKeepList

            if not changeMade:
                break

        return (depList, cannotResolve, changeSetList, keepList, ineligible)

    def resolveEraseByUpdating(self, trvSrc, cannotResolve, uJob, jobSet, 
                               ineligible):
        """
            Attempt to resolve broken erase dependencies by updating the 
            package that has the dependency on the trove that is being 
            erased.
            @param ineligible: Ineligible troves are troves that were
            involved in resolveEraseByUpdating before.
        """

        oldIdx = {}
        newIdx = {}
        for job in jobSet:
            if job[1][0] is not None:
                oldIdx[(job[0], job[1][0], job[1][1])] = job
            if job[2][0] is not None:
                newIdx[(job[0], job[2][0], job[2][1])] = job

        potentialUpdateList = []

        for resolveInfo in cannotResolve:
            (reqInfo, depSet, provInfoList) = resolveInfo

            found = False
            for provInfo in provInfoList:
                if provInfo in ineligible:
                    # The trove that we erased due to an earlier 
                    # resolveEraseByUpdating was required by other troves!
                    # We don't allow this process to recurse to avoid
                    # accidentally updating your entire system due to 
                    # a glibc update request, e.g.
                    found = True
                    break
            if found:
                continue

            for provInfo in provInfoList:
                if provInfo not in oldIdx:
                    continue

                job = oldIdx[provInfo]

                if not job[2][0]:
                    # this was an erase, not an update, we don't attempt
                    # use this method on erases.
                    continue

                potentialUpdateList.append((reqInfo, depSet, provInfoList))
                break

        # attempt to update the _package_ that has the requirement that
        # is now erased.
        updateJobs = [ (x[0][0].split(':')[0],
                        (None, None), (None, None), True)
                        for x in potentialUpdateList ]

        try:
            newJob, suggMap = self.client.updateChangeSet(updateJobs,
                                                     keepExisting=False,
                                                     resolveDeps=False,
                                                     split=False)
            newJobSet = newJob.getJobs()

            # ignore updates where updating this trove would update
            # to something that's already in the jobSet
            newJobSet = set(x for x in newJobSet[0]
                            if (x[0], x[2][0], x[2][1]) not in newIdx)

            # We were able to update some troves that required troves
            # that were in the cannot resolve list.
            # Calculate the new state of dependencies, and make sure
            # there is actually a change

            uJob.getTroveSource().merge(newJob.getTroveSource())
            (depList, newCannotResolve, changeSetList) = \
                    self.db.depCheck(jobSet | newJobSet,
                                     uJob.getTroveSource(),
                                     findOrdering = False)
            if cannotResolve != newCannotResolve:
                cannotResolve = newCannotResolve
                ineligible.update((x[0], x[1][0], x[1][1])
                                  for x in newJobSet)
            else:
                newJobSet = set()
        except (errors.ClientError, errors.TroveNotFound):
            newJobSet = set()

        return cannotResolve, newJobSet

    def resolveEraseByKeeping(self, trvSrc, cannotResolve, uJob, jobSet):
        oldIdx = {}
        for job in jobSet:
            if job[1][0] is not None:
                oldIdx[(job[0], job[1][0], job[1][1])] = job

        restoreSet = set()

        keepList = []
        for resolveInfo in cannotResolve[:]:
            (reqInfo, depSet, provInfoList) = resolveInfo
            # Modify/remove non-primary jobs that cause
            # irreconcilable dependency problems.
            for provInfo in provInfoList:
                if provInfo not in oldIdx: continue

                job = oldIdx[provInfo]
                if job in restoreSet:
                    break

                # if erasing this was a primary job, don't break
                # it up
                if (job[0], job[1], (None, None), False) in \
                        uJob.getPrimaryJobs():
                    continue

                if job[2][0] is None:
                    # this was an erasure implied by package changes;
                    # leaving it in place won't hurt anything
                    keepList.append((job, depSet, reqInfo))
                    cannotResolve.remove(resolveInfo)
                    restoreSet.add(job)
                    break

                oldTrv = self.db.getTrove(withFiles = False,
                                          *provInfo)
                newTrv = trvSrc.getTrove(job[0], job[2][0], job[2][1],
                                         withFiles = False)

                if oldTrv.compatibleWith(newTrv):
                    restoreSet.add(job)
                    keepList.append((job, depSet, reqInfo))
                    cannotResolve.remove(resolveInfo)
                    break

        if self.cfg.autoResolvePackages:
           # if we're keeping any components, keep the package as well.
           jobsByOld = dict(((x[0], x[1]), x) for x in jobSet 
                            if ':' not in x[0])
           for job in list(restoreSet):
               if ':' in job[0]:
                   pkgJob = jobsByOld.get((job[0].split(':')[0], job[1]), None)
                   if pkgJob:
                       restoreSet.add(pkgJob)

        for job in restoreSet:
            jobSet.remove(job)
            if job[2][0] is not None:
                # if there was an install portion of the job,
                # retain it
                jobSet.add((job[0], (None, None), job[2], False))

        return (cannotResolve, keepList)

    def selectResolutionTrove(self, troveTups, installFlavor, affFlavorDict):
        """ determine which of the given set of troveTups is the 
            best choice for installing on this system.  Because the
            repository didn't try to determine which flavors are best for 
            our system, we have to filter the troves locally.  
        """
        # we filter the troves in the following ways:
        # 1. remove trove tups that don't match this installFlavor
        #    (after modifying the flavor by any affinity flavor found
        #     in an installed trove by the same name)
        # 2. filter so that only the latest version of a trove is left
        #    for each name,branch pair. (this ensures that a really old
        #    version of a trove doesn't get preferred over a new one 
        #    just because its got a better flavor)
        # 3. pick the best flavor out of the remaining


        if installFlavor:
            flavoredList = []
            for troveTup in troveTups:
                f = installFlavor.copy()
                affFlavors = affFlavorDict[troveTup[0]]
                if affFlavors:
                    affFlavor = affFlavors[0][2]
                    flavorsMatch = True
                    for newF in [x[2] for x in affFlavors[1:]]:
                        if newF != affFlavor:
                            flavorsMatch = False
                            break
                    if flavorsMatch:
                        f.union(affFlavor,
                                mergeType=deps.DEP_MERGE_TYPE_PREFS)

                flavoredList.append((f, troveTup))
        else:
            flavoredList = [ (None, x) for x in troveTups ]

        trovesByNB = {}
        for installFlavor, (n,v,f) in flavoredList:
            b = v.branch()
            myTimeStamp = v.timeStamps()[-1]
            if installFlavor is None:
                myScore = 0
            else:
                myScore = installFlavor.score(f)
                if myScore is False:
                    continue

            if (n,b) in trovesByNB:
                curScore, curTimeStamp, curTup = trovesByNB[n,b]
                if curTimeStamp > myTimeStamp:
                    continue
                if curTimeStamp == myTimeStamp:
                    if myScore < curScore:
                        continue

            trovesByNB[n,b] = (myScore, myTimeStamp, (n,v,f))

        scoredList = sorted(trovesByNB.itervalues())
        if not scoredList:
            return None
        else:
            return scoredList[-1][-1]

    def addPackagesForComponents(self, troves, troveSource, beingInstalled):
        packages = {}
        for job in troves:
            if ':' in job[0]:
                pkgName = job[0].split(':', 1)[0]
                pkgInfo = (pkgName, job[2][0], job[2][1])
                if pkgInfo in beingInstalled:
                    continue
                packages[pkgName, job[2][0], job[2][1]] = (pkgName, job[1], job[2], True)
        toCheck = list(packages)
        hasTroves = troveSource.hasTroves(toCheck)
        if isinstance(hasTroves, list):
            hasTroves = dict(itertools.izip(toCheck, hasTroves))
        packageJobs = [x[1] for x in packages.iteritems() if hasTroves[x[0]]]
        return packageJobs

    def filterCrossBranchResolutions(self, jobSet, troveSource):
        # We can't resolve deps in a way that would cause conary to
        # switch the branch of a trove.
        crossBranchJobs = [ x for x in jobSet
                            if (x[1][0] and
                                x[1][0].branch() != x[2][0].branch()) ]
        if crossBranchJobs:
            jobSet.difference_update(crossBranchJobs)
            oldTroves = self.db.getTroves(
                  [ (x[0], x[1][0], x[1][1]) for x in crossBranchJobs ],
                  withFiles = False)
            newTroves = troveSource.getTroves(
                  [ (x[0], x[2][0], x[2][1]) for x in crossBranchJobs ],
                  withFiles = False)
            for job, oldTrv, newTrv in itertools.izip(crossBranchJobs,
                                                      oldTroves,
                                                      newTroves):
                if oldTrv.compatibleWith(newTrv):
                    jobSet.add((job[0], (None, None), job[2], False))
        return jobSet
