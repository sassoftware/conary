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
import sys
import trove
import util
import versions
import conaryclient

# FIXME client should instantiated once per execution of the command line conary client

def doUpdate(repos, cfg, pkg, versionStr = None, replaceFiles = False,
                              tagScript = None, keepExisting = False):
    client = conaryclient.ConaryClient(repos, cfg)
    
    try:
        if os.path.exists(pkg) and os.path.isfile(pkg):
            if versionStr:
                log.error("Version should not be specified when a "
                          "Conary change set is being installed.")
                return
            else:
                client.applyChangeSet(pkg, replaceFiles, tagScript, keepExisting) 
        else:
            client.updateTrove(pkg, versionStr, replaceFiles, tagScript, keepExisting)
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)

def doErase(db, cfg, pkg, versionStr = None, tagScript = None):
    client = conaryclient.ConaryClient(None, cfg)
    
    try:
        client.eraseTrove(pkg, versionStr, tagScript)
    except repository.PackageNotFound:
        log.error("package not found: %s", pkg)
