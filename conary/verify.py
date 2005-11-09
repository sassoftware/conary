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
Provides the output for the "conary verify" command
"""

from deps import deps
import display
import files
from lib import log
import time
from repository import errors
import sys
import showchangeset
import versions
from local import update

from lib.sha1helper import sha1ToString


def usage():
    print "conary verify [--all] [trove[=version]]*"
    print ""

def verify(troveNameList, db, cfg, all=False):
    (troveNames, hasVersions, hasFlavors) = \
                    display.parseTroveStrings(troveNameList)
    if not troveNames and not all:
        usage()
        log.error("must specify either a trove or --all")
        return 1
    elif not troveNames:
	troveNames = [ (x, None, None) for x in db.iterAllTroveNames() \
                                                  if x.find(':') == -1 ]
	troveNames.sort()
    for (troveName, versionStr, flavor) in troveNames:
        try:
            troves = db.findTrove(None, (troveName, versionStr, flavor))
            troves = db.getTroves(troves)
            for trove in troves:
                verifyTrove(trove, db, cfg)
        except errors.TroveNotFound:
            if versionStr:
                if flavor:
                    flavorStr = deps.formatFlavor(flavor)
                    log.error("version %s with flavor '%s' of trove %s is not"
                              " installed", versionStr, flavorStr, troveName)
                else:
                    log.error("version %s of trove %s is not installed", 
                                                      versionStr, troveName)
            elif flavor:
                flavorStr = deps.formatFlavor(flavor)
                log.error("flavor '%s' of trove %s is not installed", 
                                                          flavorStr, troveName)
            else:
                log.error("trove %s is not installed", troveName)

def verifyTrove(trove, db, cfg):
    l = []
    for trove in db.walkTroveSet(trove):
        ver = trove.getVersion()
        origTrove = db.getTrove(trove.getName(), ver, trove.getFlavor(), 
                              pristine = True)
        ver = ver.createBranch(versions.LocalLabel(), withVerRel = 1)
        l.append((trove, origTrove, ver, 0))
	    
    try:
        result = update.buildLocalChanges(db, l, root = cfg.root, 
                                          withFileContents=False,
                                          forceSha1=True,
                                          ignoreTransient=True)
        if not result: return
        cs = result[0]

        cs.addPrimaryTrove(trove.getName(), 
                           trove.getVersion().createBranch(versions.LocalLabel(),
                                    withVerRel = 1),
                           trove.getFlavor())

        for (changed, fsTrove) in result[1]:
            if changed:
                break
        if not changed:
            return
        showchangeset.displayChangeSet(db, None, cs, [], cfg, ls=True, 
                                       showChanges=True)
    except OSError, err:
        if err.errno == 13:
            log.warning("Permission denied creating local changeset for"
                        " %s " % trove.getName())
