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
from repository import changeset
from local import database
from lib import log
import os
from repository import repository
from repository.filecontainer import BadContainer
import sys
from lib import util
import conaryclient

# FIXME client should instantiated once per execution of the command line 
# conary client

def doUpdate(repos, cfg, pkgList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False):
    client = conaryclient.ConaryClient(repos, cfg)

    applyList = []

    if type(pkgList) is str:
        pkgList = ( pkgList, )
    for pkgStr in pkgList:
        if os.path.exists(pkgStr) and os.path.isfile(pkgStr):
            try:
                cs = changeset.ChangeSetFromFile(pkgStr)
            except BadContainer, msg:
                log.error("'%s' is not a valid conary changset: %s" % (pkgStr, msg))
                sys.exit(1)
            applyList.append(cs)
        elif pkgStr.find("=") >= 0:
            l = pkgStr.split("=")
            if len(l) != 2:
                log.error("too many ='s in %s", pkgStr)
                return 1
            applyList.append((l[0], l[1]))
        else:
            applyList.append(pkgStr)
            
    try:
        client.updateTrove(applyList, replaceFiles, tagScript, keepExisting)
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)
    except database.MissingDependencies, e:
        print "Dependency check failure:"
        print "\t" + "\n\t".join(str(e).split("\n"))

def doErase(cfg, itemList, tagScript = None):
    troveList = []
    for item in itemList:
        l = item.split("=")
        if len(l) == 1:
            troveList.append((l[0], None))
        elif len(l) == 2:
            troveList.append((l[0], l[1]))
        else:
            log.error("too many ='s in %s", pkgStr)
            return 1

    client = conaryclient.ConaryClient(cfg=cfg)

    try:
        client.eraseTrove(troveList, tagScript)
    except repository.PackageNotFound, e:
        log.error(str(e))
