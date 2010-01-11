#
# Copyright (c) 2004-2009 rPath, Inc.
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
"""
Provides the output for the "conary verify" command
"""
from conary import showchangeset, trove
from conary import versions
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log
from conary.local import defaultmap, update
from conary.repository import changeset
from conary import errors

def LocalChangeSetCommand(db, cfg, item, changeSetPath = None):
    cs = _verify([ item ], db, cfg, display = False)

    if not [ x for x in cs.iterNewTroveList() ]:
        log.error("there have been no local changes")
    else:
        cs.writeToFile(changeSetPath)
    return

def verify(troveNameList, db, cfg, all=False, changesetPath = None,
           forceHashCheck = False):
    cs = _verify(troveNameList, db, cfg, all=all,
                 forceHashCheck = forceHashCheck,
                 display = (changesetPath is None))

    if changesetPath:
        # verify doesn't display changes in collections because those, by
        # definition, match the database
        for trvCs in list(cs.iterNewTroveList()):
            if trove.troveIsCollection(trvCs.getName()):
                cs.delNewTrove(*trvCs.getNewNameVersionFlavor())

        cs.writeToFile(changesetPath)


def _verify(troveNameList, db, cfg, all=False, forceHashCheck = False,
            display = False):
    if display:
        # save memory by not keeping the changeset around; this is
        # particularly useful when all=True
        cs = None
    else:
        cs = changeset.ReadOnlyChangeSet()

    troveNames = [ cmdline.parseTroveSpec(x) for x in troveNameList ]
    if all:
        assert(not troveNameList)
        client = conaryclient.ConaryClient(cfg)
        troveInfo = client.getUpdateItemList()
        troveInfo.sort()
    else:
        troveInfo = []

        for (troveName, versionStr, flavor) in troveNames:
            try:
                troveInfo += db.findTrove(None, (troveName, versionStr, flavor))
            except errors.TroveNotFound:
                if versionStr:
                    if flavor is not None and not flavor.isEmpty():
                        flavorStr = deps.formatFlavor(flavor)
                        log.error("version %s with flavor '%s' of trove %s is "
                                  "not installed", versionStr, flavorStr,
                                  troveName)
                    else:
                        log.error("version %s of trove %s is not installed",
                                                      versionStr, troveName)
                elif flavor is not None and not flavor.isEmpty():
                    flavorStr = deps.formatFlavor(flavor)
                    log.error("flavor '%s' of trove %s is not installed",
                                                          flavorStr, troveName)
                else:
                    log.error("trove %s is not installed", troveName)

    defaultMap = defaultmap.DefaultMap(db, troveInfo)
    troves = db.getTroves(troveInfo, withDeps = False, withFileObjects = True,
                          pristine = False)

    seen = set()
    for trv in troves:
        newCs = _verifyTrove(trv, db, cfg, defaultMap, display,
                             forceHashCheck = forceHashCheck,
                             duplicateFilterSet = seen)
        if cs and newCs:
            cs.merge(newCs)
            cs.addPrimaryTrove(trv.getName(),
                               trv.getVersion().createShadow(
                                   versions.LocalLabel()),
                               trv.getFlavor())
    return cs

def _verifyTroveList(db, troveList, cfg, display = True,
                     forceHashCheck = False):
    log.info('Verifying %s' % " ".join(x[1].getName() for x in troveList))
    changedTroves = set()

    try:
        result = update.buildLocalChanges(db, troveList, root = cfg.root,
                                          #withFileContents=False,
                                          forceSha1=forceHashCheck,
                                          ignoreTransient=True,
                                          updateContainers=True)
        if not result: return
        cs = result[0]
        changed = False
        for (changed, trv) in result[1]:
            if changed:
                changedTroves.add(trv.getNameVersionFlavor())
    except OSError, err:
        if err.errno == 13:
            log.warning("Permission denied creating local changeset for"
                        " %s " % str([ x[0].getName() for x in l ]))
        return

    trovesChanged = [ x.getNameVersionFlavor() for (changed, x) in
                        result[1] if changed ]
    if not trovesChanged:
        return None

    if display and trovesChanged:
        troveSpecs = [ '%s=%s[%s]' % x for x in trovesChanged ]
        showchangeset.displayChangeSet(db, cs, troveSpecs, cfg, ls=True,
                                       showChanges=True, asJob=True)

    return cs

def _verifyTrove(trv, db, cfg, defaultMap, display = True,
                 forceHashCheck = False, duplicateFilterSet = None,
                 allMachineChanges = False):
    collections = []
    if trove.troveIsCollection(trv.getName()):
        collections.append(trv)

    cs = changeset.ReadOnlyChangeSet()
    verifyList = []

    queue = [ trv ]
    duplicateFilterSet.add(trv.getNameVersionFlavor())
    for thisTrv in db.walkTroveSet(trv):
        if verifyList and (verifyList[-1][0].getName().split(':')[0] !=
                           thisTrv.getName().split(':')[0]):
            # display output as soon as we're done processing one named
            # trove; this works because walkTroveSet is guaranteed to
            # be depth first
            subCs = _verifyTroveList(db, verifyList, cfg, display = display)
            if subCs:
                cs.merge(subCs)

            verifyList = []

        if allMachineChanges:
            origTrv = db.getTrove(*thisTrv.getNameVersionFlavor(),
                                  pristine = True)
        else:
            origTrv = thisTrv

        ver = thisTrv.getVersion().createShadow(versions.LocalLabel())
        verifyList.append((thisTrv, thisTrv, ver, update.UpdateFlags()))

    subCs = _verifyTroveList(db, verifyList, cfg, display = display,
                             forceHashCheck = forceHashCheck)
    if subCs:
        cs.merge(subCs)

    return cs
