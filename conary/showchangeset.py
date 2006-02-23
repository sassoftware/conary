#
# Copyright (c) 2004-2005 rPath, Inc.
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
"""
Provides the output for the "conary showcs" command
"""

import itertools
import time
import sys

#conary
from conary import conaryclient
from conary.conaryclient import cmdline
from conary import display, query
from conary import files
from conary.lib import log
from conary.lib.sha1helper import sha1ToString
from conary.repository import repository, trovesource

def usage():
    print "conary showcs   <changeset> [trove[=version]]"
    print "  Accepts all common display options.  Also,"
    print "                --show-changes    For modifications, show the old "
    print "                                  file info below new"
    print "                --all             Combine tags to display most information about the changeset"
    print ""

def displayChangeSet(db, cs, troveSpecs, cfg,
                     # trove options
                     info = False, digSigs = False, deps = False,
                     showBuildReqs = False, all = False,
                     # file options
                     ls = False, lsl = False, ids = False, sha1s = False, 
                     tags = False, fileDeps = False, fileVersions = False,
                     # collection options
                     showTroves = False, recurse = None, showAllTroves = False,
                     weakRefs = False, showTroveFlags = False,
                     alwaysDisplayHeaders = False,
                     # job options
                     showChanges = False, asJob = False):

    if all:
        deps = tags = recurse = showTroveFlags = showAllTroves = True
        if ls:
            fileDeps = lsl = True

    if showChanges:
        lsl = True

    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if not asJob and not showChanges and cs.isAbsolute():
        changeSetSource = trovesource.ChangesetFilesTroveSource(None)
        changeSetSource.addChangeSet(cs)


        if not troveSpecs:
            troveTups = cs.getPrimaryTroveList()
            primary = True
            if not troveTups:
                log.warning('No primary troves in changeset, listing all troves')
                troveTups = [(x.getName(), x.getNewVersion(), x.getNewFlavor())\
                                            for x in cs.iterNewTroveList()]
        else:
            troveTups, primary  = query.getTrovesToDisplay(changeSetSource, 
                                                           troveSpecs)
        querySource = trovesource.stack(changeSetSource, client.getRepos())

        dcfg = display.DisplayConfig(querySource, client.db)
        dcfg.setTroveDisplay(deps=deps, info=info, fullFlavors=cfg.fullFlavors,
                             showLabels=cfg.showLabels, baseFlavors=cfg.flavor)
        dcfg.setFileDisplay(ls=ls, lsl=lsl, ids=ids, sha1s=sha1s, tags=tags, 
                            fileDeps=fileDeps, fileVersions=fileVersions)

        recurseOne = showTroves or showAllTroves or weakRefs
        if recurse is None and not recurseOne:
            # if we didn't explicitly set recurse and we're not recursing one
            # level explicitly 
            recurse = True in (ls, lsl, ids, sha1s, tags, deps, fileDeps,
                               fileVersions)

        dcfg.setChildDisplay(recurseAll = recurse, recurseOne = recurseOne,
                         showNotByDefault = showAllTroves,
                         showWeakRefs = weakRefs,
                         showTroveFlags = showTroveFlags,
                         displayHeaders = alwaysDisplayHeaders or showTroveFlags)

        if primary:
            dcfg.setPrimaryTroves(set(troveTups))
        formatter = display.TroveFormatter(dcfg)
        display.displayTroves(dcfg, formatter, troveTups)
    else:
        changeSetSource = trovesource.ChangeSetJobSource(repos, 
                                             trovesource.stack(db, repos))
        changeSetSource.addChangeSet(cs)

        jobs = getJobsToDisplay(changeSetSource, troveSpecs)

        dcfg = display.JobDisplayConfig(changeSetSource, client.db)

        dcfg.setJobDisplay(showChanges=showChanges,
                           compressJobs=not cfg.showComponents)

        dcfg.setTroveDisplay(deps=deps, info=info, fullFlavors=cfg.fullFlavors,
                             showLabels=cfg.showLabels, baseFlavors=cfg.flavor)


        dcfg.setFileDisplay(ls=ls, lsl=lsl, ids=ids, sha1s=sha1s, tags=tags,
                            fileDeps=fileDeps, fileVersions=fileVersions)

        recurseOne = showTroves or showAllTroves or weakRefs
        if recurse is None and not recurseOne:
            # if we didn't explicitly set recurse and we're not recursing one
            # level explicitly and we specified troves (so everything won't 
            # show up at the top level anyway), guess at whether to recurse
            recurse = True in (ls, lsl, ids, sha1s, tags, deps, fileDeps,
                               fileVersions)

        dcfg.setChildDisplay(recurseAll = recurse, recurseOne = recurseOne,
                         showNotByDefault = showAllTroves,
                         showWeakRefs = weakRefs,
                         showTroveFlags = showTroveFlags)

        formatter = display.JobFormatter(dcfg)
        display.displayJobs(dcfg, formatter, jobs)


def getJobsToDisplay(jobSource, jobSpecs):
    if jobSpecs:  
        jobSpecs = cmdline.parseChangeList(jobSpecs, allowChangeSets=False)
    else:
        jobSpecs = []

    if jobSpecs:
        results = jobSource.findJobs(jobSpecs)
        jobs = list(itertools.chain(*results.itervalues()))
    else:
        jobs = list(jobSource.iterAllJobs())

    return jobs
