#
# Copyright (c) 2004 Specifix, Inc.
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

import display
import files
from lib import log
import time
from repository import repository
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
                    display.parseTroveStrings(troveNameList, cfg.flavor)
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
            for trove in db.findTrove(troveName, versionStr):
                if not flavor or trove.getFlavor().satisfies(flavor):
                    verifyTrove(trove, db, cfg)
        except repository.PackageNotFound:
            if versionStr:
                log.error("version %s of trove %s is not installed",
                          versionStr, troveName)
            else:
                log.error("trove %s is not installed", troveName)

def verifyTrove(trove, db, cfg):
    list = []
    for pkg in db.walkTroveSet(trove):
        ver = pkg.getVersion()
        origPkg = db.getTrove(pkg.getName(), ver, pkg.getFlavor(), 
                              pristine = True)
        ver = ver.fork(versions.LocalBranch(), sameVerRel = 1)
        list.append((pkg, origPkg, ver, 0))
	    
    try:
        result = update.buildLocalChanges(db, list, root = cfg.root, 
                                      withFileContents=False, forceSha1=True,
                                      ignoreTransient=True)
        if not result: return
        cs = result[0]

        cs.addPrimaryPackage(trove.getName(), 
                trove.getVersion().fork(
                versions.LocalBranch(), sameVerRel = 1),
                trove.getFlavor())

        for (changed, fsPkg) in result[1]:
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
