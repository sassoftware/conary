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
from lib import util
import conaryclient

# FIXME client should instantiated once per execution of the command line 
# conary client

def doUpdate(repos, cfg, pkgList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False):
    client = conaryclient.ConaryClient(repos, cfg)

    applyList = []

    for pkgStr in pkgList:
        if os.path.exists(pkgStr) and os.path.isfile(pkgStr):
            cs = changeset.ChangeSetFromFile(pkgStr)
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

def doErase(cfg, pkg, versionStr = None, tagScript = None):
    client = conaryclient.ConaryClient(cfg=cfg)
    
    try:
        client.eraseTrove(pkg, versionStr, tagScript)
    except repository.PackageNotFound:
        log.error("package not found: %s", pkg)
