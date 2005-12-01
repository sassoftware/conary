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
    print "showcs flags:   "
    print "                --full-versions   Print full version strings instead of "
    print "                                  attempting to shorten them" 
    print "                --deps            Print dependency information about the troves"
    print "                --ls              (Recursive) list file contents"
    print "                --show-changes    For modifications, show the old "
    print "                                  file version next to new one"
    print "                --tags            Show tagged files (use with ls to "
    print "                                  show tagged and untagged)"
    print "                --sha1s           Show sha1s for files"
    print "                --ids             Show fileids"
    print "                --all             Combine above tags"
    print ""

def displayChangeSet(db, cs, troveSpecs, cfg, ls = False, tags = False,  
                     showChanges=False,
                     all=False, deps=False, sha1s=False, ids=False,
                     asJob=False):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if not asJob and not showChanges and cs.isAbsolute():
        changeSetSource = trovesource.ChangesetFilesTroveSource(None)
        changeSetSource.addChangeSet(cs)


        if not troveSpecs:
            troveTups = cs.getPrimaryTroveList() 
            primary = True
            namesOnly = True
        else:
            troveTups, namesOnly, primary  = query.getTrovesToDisplay(
                                                         changeSetSource, 
                                                         troveSpecs)

        dcfg = display.DisplayConfig(changeSetSource, ls=ls, ids=ids, 
                             sha1s=sha1s, fullVersions=cfg.fullVersions, 
                             tags=tags, deps=deps, 
                             showFlavors=cfg.fullFlavors,
                             iterChildren=not namesOnly)
        if primary:
            dcfg.setPrimaryTroves(set(troveTups))
        formatter = display.TroveFormatter(dcfg)
        display.displayTroves(dcfg, formatter, troveTups)
    else:
        changeSetSource = trovesource.ChangeSetJobSource(repos, 
                                             trovesource.stack(db, repos))
        changeSetSource.addChangeSet(cs)

        jobs, namesOnly = getJobsToDisplay(changeSetSource, troveSpecs)

        dcfg = display.JobDisplayConfig(changeSetSource,
                                        ls=ls, ids=ids, sha1s=sha1s, 
                                        info=False, tags=tags, deps=deps,
                                        showChanges=showChanges,
                                        iterChildren=not namesOnly)

        formatter = display.JobFormatter(dcfg)
        display.displayJobs(dcfg, formatter, jobs)


def getJobsToDisplay(jobSource, jobSpecs):
    namesOnly = True
    if jobSpecs:  
        jobSpecs = cmdline.parseChangeList(jobSpecs, allowChangeSets=False)
    else:
        jobSpecs = []

    for jobSpec in jobSpecs:
        if jobSpec[1] != (None, None) or jobSpec[2] != (None, None):
            namesOnly = False

    if jobSpecs:
        results = jobSource.findJobs(jobSpecs)
        jobs = list(itertools.chain(*results.itervalues()))
    else:
        jobs = list(jobSource.iterAllJobs())

    return jobs, namesOnly
