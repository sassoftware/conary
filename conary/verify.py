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
"""
Provides the output for the "conary verify" command
"""
from conary import showchangeset, trove
from conary import versions
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log
from conary.local import defaultmap, update
from conary.repository import changeset
from conary import errors

def usage():
    print "conary verify [--all] [trove[=version]]*"
    print ""

def verify(troveNameList, db, cfg, all=False, changesetPath=None):
    if changesetPath:
        cs = changeset.ReadOnlyChangeSet()
    else:
        cs = None

    troveNames = [ cmdline.parseTroveSpec(x) for x in troveNameList ]
    if not troveNames and not all:
        usage()
        log.error("must specify either a trove or --all")
        return 1
    elif not troveNames:
	troveNames = [ (x, None, None) for x in db.iterAllTroveNames() \
                                                  if x.find(':') == -1 ]
	troveNames.sort()

    troveInfo = []

    for (troveName, versionStr, flavor) in troveNames:
        try:
            troveInfo += db.findTrove(None, (troveName, versionStr, flavor))
        except errors.TroveNotFound:
            if versionStr:
                if flavor is not None and not flavor.isEmpty():
                    flavorStr = deps.formatFlavor(flavor)
                    log.error("version %s with flavor '%s' of trove %s is not"
                              " installed", versionStr, flavorStr, troveName)
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
    troves = db.getTroves(troveInfo)

    for trove in troves:
        newCs = verifyTrove(trove, db, cfg, defaultMap, display = (cs == None))
        if cs and newCs:
            cs.merge(newCs)

    if changesetPath:
        cs.writeToFile(changesetPath)

def _verifyTroveList(db, troveList, cfg, display = True):
    log.info('Verifying %s' % " ".join(x[1].getName() for x in troveList))
    changedTroves = set()

    try:
        result = update.buildLocalChanges(db, troveList, root = cfg.root,
                                          withFileContents=False,
                                          forceSha1=True,
                                          ignoreTransient=True)
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

    troveSpecs = []
    for item in troveList:
        trv = item[0]
        ver = trv.getVersion().createShadow(versions.LocalLabel())
        nvf = (trv.getName(), ver, trv.getFlavor())
        trvCs = cs.getNewTroveVersion(*nvf)
        if trvCs.hasChangedFiles():
            troveSpecs.append('%s=%s[%s]' % nvf)

    for (changed, fsTrove) in result[1]:
        if changed:
            break

    if not changed:
        return None

    if display:
        showchangeset.displayChangeSet(db, cs, troveSpecs, cfg, ls=True,
                                       showChanges=True, asJob=True)

    return cs

def verifyTrove(trv, db, cfg, defaultMap, display = True):
    collections = []
    if trove.troveIsCollection(trv.getName()):
        collections.append(trv)

    cs = changeset.ReadOnlyChangeSet()
    troveList = []

    for subTrv in db.walkTroveSet(trv):
        if trove.troveIsCollection(subTrv.getName()):
            collections.append(subTrv)
        else:
            if troveList and (troveList[-1][0].getName().split(':')[0] !=
                              subTrv.getName().split(':')[0]):
                subCs = _verifyTroveList(db, troveList, cfg, display = display)
                if subCs:
                    cs.merge(subCs)

                troveList = []

            origTrove = db.getTrove(pristine = False,
                                    *subTrv.getNameVersionFlavor())
            ver = subTrv.getVersion().createShadow(versions.LocalLabel())
            troveList.append((subTrv, origTrove, ver, update.UpdateFlags()))

    subCs = _verifyTroveList(db, troveList, cfg, display = display)
    if subCs:
        cs.merge(subCs)

    return cs
