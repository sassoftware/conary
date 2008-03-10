#
# Copyright (c) 2004-2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
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

def versionFormat(cfg, version, defaultLabel = None):
    """Format the version according to the options in the cfg object"""
    if cfg.fullVersions:
        return str(version)

    if cfg.showLabels:
        ret = "%s/%s" % (version.branch().label(), version.trailingRevision())
        return ret

    if defaultLabel and (version.branch().label() == defaultLabel):
        return str(version.trailingRevision())

    ret = "%s/%s" % (version.branch().label(), version.trailingRevision())
    return ret

def verStr(cfg, version, flavor, defaultLabel = None):
    if defaultLabel is None:
        defaultLabel = cfg.installLabel

    ret = versionFormat(cfg, version, defaultLabel = defaultLabel)
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

            # looks for components-of-packages and collapse those into the
            # package itself (just like update does)
            compByPkg = {}

            for info in newList:
                name = info[0]
                if ':' in name:
                    pkg, component = name.split(':')
                    pkgInfo = (pkg,) + info[1:]
                else:
                    pkgInfo = info
                    component = None
                l = compByPkg.setdefault(pkgInfo, [])
                l.append(component)

            oldList.sort()
            for info in newList:
                (name, oldVersion, oldFlavor, newVersion, newFlavor) = info
                if ':' in name:
                    pkgInfo = (name.split(':')[0],) + info[1:]
                    if None in compByPkg[pkgInfo]:
                        # this component was displayed with its package
                        continue

                if info in compByPkg:
                    comps = [":" + x for x in compByPkg[info] if x is not None]
                    if comps:
                        name += '(%s)' % " ".join(comps)

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
                                       verStr(cfg, oldVersion, oldFlavor,
                                              defaultLabel =
                                                newVersion.branch().label()))
                    w_(templ % ('updated', name, pn))

            compByPkg = {}

            for name, version, flavor in oldList:
                if ':' in name:
                    pkg, component = name.split(':')
                else:
                    pkg = name
                    component = None
                l = compByPkg.setdefault((pkg, version, flavor), [])
                l.append(component)

            for (name, version, flavor) in oldList:
                if ':' in name:
                    pkgInfo = (name.split(':')[0], version, flavor)
                    if None in compByPkg[pkgInfo]:
                        # this component was displayed with its package
                        continue

                if (name, version, flavor) in compByPkg:
                    comps = [ ":" + x 
                                for x in compByPkg[(name, version, flavor)]
                                if x is not None ]
                    if comps:
                        name += '(%s)' % " ".join(comps)
                w_(templ % ('installed', name, verStr(cfg, version, flavor)))

        w_('\n')

def apply(db, cfg, rollbackSpec, **kwargs):
    import warnings
    warnings.warn("rollbacks.apply is deprecated, use the client's "
                    "applyRollback call", DeprecationWarning)
    from conary import conaryclient
    client = conaryclient.ConaryClient(cfg)
    return applyRollback(client, rollbackSpec, returnOnError = True, **kwargs)

def applyRollback(client, rollbackSpec, returnOnError = False, **kwargs):
    """
    Apply a rollback.

    See L{conary.conaryclient.ConaryClient.applyRollback} for a description of
    the arguments for this function.
    """
    client.checkWriteableRoot()
    # Record the transaction counter, to make sure the state of the database
    # didn't change while we were computing the rollback list.
    transactionCounter = client.db.getTransactionCounter()

    log.syslog.command()

    defaults = dict(replaceFiles = False,
                    transactionCounter = transactionCounter)
    defaults.update(kwargs)

    client.db.readRollbackStatus()
    rollbackList = client.db.getRollbackList()

    if rollbackSpec.startswith('r.'):
        try:
            i = rollbackList.index(rollbackSpec)
        except ValueError:
            log.error("rollback '%s' not present" % rollbackSpec)
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)

        rollbacks = rollbackList[i:]
        rollbacks.reverse()
    else:
        try:
            rollbackCount = int(rollbackSpec)
        except ValueError:
            log.error("integer rollback count expected instead of '%s'" %
                    rollbackSpec)
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)

        if rollbackCount < 1:
            log.error("rollback count must be positive")
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)
        elif rollbackCount > len(rollbackList):
            log.error("rollback count higher then number of rollbacks "
                      "available")
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)

        rollbacks = rollbackList[-rollbackCount:]
        rollbacks.reverse()

    try:
        client.db.applyRollbackList(client.getRepos(), rollbacks, **defaults)
    except database.RollbackError, e:
        log.error("%s", e)
        if returnOnError:
            return 1
        raise

    log.syslog.commandComplete()

    return 0
