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

from conary import conaryclient
from conary.lib import log
from conary.local import database

def listRollbacks(db, cfg):
    def verStr(cfg, version):
	if version.onLocalLabel():
	    return "local"

	if version.branch().label() == cfg.installLabel:
	    return version.trailingRevision().asString()
	return version.asString()

    # Display template
    templ = "\t%9s: %s %s"

    for rollbackName in reversed(db.getRollbackList()):
        print "%s:" % rollbackName

        rb = db.getRollback(rollbackName)
        for cs in rb.iterChangeSets():
            newList = []
            for pkg in cs.iterNewTroveList():
                newList.append((pkg.getName(), pkg.getOldVersion(),
                                pkg.getNewVersion()))
            oldList = [ x[0:2] for x in cs.getOldTroveList() ]

            newList.sort()
            oldList.sort()
            for (name, oldVersion, newVersion) in newList:
                if newVersion.onLocalLabel():
                    # Don't display changes to local branch
                    continue
                if not oldVersion:
                    print templ % ('erased', name, verStr(cfg, newVersion))
                else:
                    ov = oldVersion.trailingRevision()
                    nv = newVersion.trailingRevision()
                    if newVersion.onRollbackLabel() and ov == nv:
                        # Avoid displaying changes to rollback branch
                        continue
                    pn = "%s -> %s" % (verStr(cfg, newVersion),
                                       verStr(cfg, oldVersion))
                    print templ % ('updated', name, pn)

            for (name, version) in oldList:
                print templ % ('installed', name, verStr(cfg, version))

        print

def apply(db, cfg, rollbackSpec, **kwargs):
    client = conaryclient.ConaryClient(cfg)
    client.checkWriteableRoot()

    log.syslog.command()

    defaults = { 'replaceFiles': False }
    defaults.update(kwargs)

    db.readRollbackStatus()
    rollbackList = db.getRollbackList()

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
	db.applyRollbackList(client.getRepos(), rollbacks, **defaults)
    except database.RollbackError, e:
	log.error("%s", e)
	return 1

    log.syslog.commandComplete()

    return 0
