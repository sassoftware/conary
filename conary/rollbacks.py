#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import sys

from conary.lib import log, util
from conary.local import database

def listRollbacks(db, cfg):
    return formatRollbacks(cfg, db.iterRollbacksList(), stream=sys.stdout)

def verStr(cfg, version, flavor):
    ret = util.verFormat(cfg, version)
    if cfg.fullFlavors:
        return "%s[%s]" % (ret, str(flavor))
    return ret

def formatRollbacks(cfg, rollbacks, stream=None):
    # Formatter function

    if stream is None:
        stream = sys.stdout

    # Display template
    templ = "\t%9s: %s %s\n"

    # Shortcut
    w_ = stream.write

    for (rollbackName, rb) in rollbacks:
        w_("%s:\n" % rollbackName)

        for cs in rb.iterChangeSets():
            newList = []
            for pkg in cs.iterNewTroveList():
                newList.append((pkg.getName(),
                                pkg.getOldVersion(), pkg.getOldFlavor(),
                                pkg.getNewVersion(), pkg.getNewFlavor()))
            oldList = [ x[0:3] for x in cs.getOldTroveList() ]

            newList.sort()
            oldList.sort()
            for (name, oldVersion, oldFlavor, newVersion, newFlavor) in newList:
                if newVersion.onLocalLabel():
                    # Don't display changes to local branch
                    continue
                if not oldVersion:
                    w_(templ % ('erased', name, 
                                verStr(cfg, newVersion, newFlavor)))
                else:
                    ov = oldVersion.trailingRevision()
                    nv = newVersion.trailingRevision()
                    if newVersion.onRollbackLabel() and ov == nv:
                        # Avoid displaying changes to rollback branch
                        continue
                    pn = "%s -> %s" % (verStr(cfg, newVersion, newFlavor),
                                       verStr(cfg, oldVersion, oldFlavor))
                    w_(templ % ('updated', name, pn))

            for (name, version, flavor) in oldList:
                w_(templ % ('installed', name, verStr(cfg, version, flavor)))

        w_('\n')

def apply(db, cfg, rollbackSpec, **kwargs):
    import warnings
    warnings.warn("rollbacks.apply is deprecated, use the client's "
                    "applyRollback call", DeprecationWarning)
    from conary import conaryclient
    client = conaryclient.ConaryClient(cfg)
    return applyRollback(client, rollbackSpec, **kwargs)

def applyRollback(client, rollbackSpec, **kwargs):
    client.checkWriteableRoot()

    log.syslog.command()

    defaults = { 'replaceFiles': False }
    defaults.update(kwargs)

    client.db.readRollbackStatus()
    rollbackList = client.db.getRollbackList()

    if rollbackSpec.startswith('r.'):
        try:
            i = rollbackList.index(rollbackSpec)
        except:
            log.error("rollback '%s' not present" % rollbackSpec)
            return 1

        rollbacks = rollbackList[i:]
        rollbacks.reverse()
    else:
        try:
            rollbackCount = int(rollbackSpec)
        except:
            log.error("integer rollback count expected instead of '%s'" %
                    rollbackSpec)
            return 1

        if rollbackCount < 1:
            log.error("rollback count must be positive")
            return 1
        elif rollbackCount > len(rollbackList):
            log.error("rollback count higher then number of rollbacks "
                      "available")
            return 1

        rollbacks = rollbackList[-rollbackCount:]
        rollbacks.reverse()

    try:
        client.db.applyRollbackList(client.getRepos(), rollbacks, **defaults)
    except database.RollbackError, e:
        log.error("%s", e)
        return 1

    log.syslog.commandComplete()

    return 0
