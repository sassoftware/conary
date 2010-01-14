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
import sys

from conary import showchangeset, trove
from conary import versions
from conary import conaryclient
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log
from conary.local import defaultmap, update
from conary.repository import changeset, trovesource
from conary import errors

DISPLAY_NONE = 0
DISPLAY_DIFF = 1
DISPLAY_CS = 2

class _FindLocalChanges(object):

    def __init__(self, db, cfg, display = True, forceHashCheck = False,
                 changeSetPath = None, allMachineChanges = False,
                 asDiff = False, repos = None):
        self.db = db
        self.cfg = cfg
        self.display = display
        self.forceHashCheck = forceHashCheck
        self.changeSetPath = changeSetPath
        self.allMachineChanges = allMachineChanges
        self.asDiff = asDiff
        self.repos = repos

        if asDiff:
            self.diffTroveSource = trovesource.SourceStack(db, self.repos)

    def _simpleTroveList(self, troveList):
        log.info('Verifying %s' % " ".join(x[1].getName() for x in troveList))
        changedTroves = set()

        try:
            result = update.buildLocalChanges(self.db, troveList,
                                              root=self.cfg.root,
                                              forceSha1=self.forceHashCheck,
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
        if trovesChanged:
            self._handleChangeSet(trovesChanged, cs)

    def _handleChangeSet(self, trovesChanged, cs):
        if self.display == DISPLAY_DIFF:
            for x in cs.gitDiff(self.diffTroveSource):
                sys.stdout.write(x)
        elif self.display == DISPLAY_CS:
            troveSpecs = [ '%s=%s[%s]' % x for x in trovesChanged ]
            showchangeset.displayChangeSet(self.db, cs, troveSpecs,
                                           self.cfg, ls=True,
                                           showChanges=True, asJob=True)

        if trovesChanged and self.finalCs:
            self.finalCs.merge(cs)

    def _recurseTrove(self, trv, duplicateFilterSet = None):
        verifyList = []

        duplicateFilterSet.add(trv.getNameVersionFlavor())
        for thisTrv in self.db.walkTroveSet(trv):
            if verifyList and (verifyList[-1][0].getName().split(':')[0] !=
                               thisTrv.getName().split(':')[0]):
                # display output as soon as we're done processing one named
                # trove; this works because walkTroveSet is guaranteed to
                # be depth first
                self._simpleTroveList(verifyList)

                verifyList = []

            if self.allMachineChanges:
                origTrv = self.db.getTrove(*thisTrv.getNameVersionFlavor(),
                                      pristine = True)
            else:
                origTrv = thisTrv

            ver = thisTrv.getVersion().createShadow(versions.LocalLabel())
            verifyList.append((thisTrv, thisTrv, ver, update.UpdateFlags()))

        self._simpleTroveList(verifyList)

    def generateChangeSet(self, troveNameList, all=False):
        if self.display != DISPLAY_NONE:
            # save memory by not keeping the changeset around; this is
            # particularly useful when all=True
            self.finalCs = None
        else:
            self.finalCs = changeset.ReadOnlyChangeSet()

        troveNames = [ cmdline.parseTroveSpec(x) for x in troveNameList ]
        if all:
            assert(not troveNameList)
            client = conaryclient.ConaryClient(self.cfg)
            troveInfo = client.getUpdateItemList()
            troveInfo.sort()
        else:
            troveInfo = []

            for (troveName, versionStr, flavor) in troveNames:
                try:
                    troveInfo += self.db.findTrove(None,
                                    (troveName, versionStr, flavor))
                except errors.TroveNotFound:
                    if versionStr:
                        if flavor is not None and not flavor.isEmpty():
                            flavorStr = deps.formatFlavor(flavor)
                            log.error("version %s with flavor '%s' of "
                                      "trove %s is not installed",
                                      versionStr, flavorStr, troveName)
                        else:
                            log.error("version %s of trove %s is not installed",
                                      versionStr, troveName)
                    elif flavor is not None and not flavor.isEmpty():
                        flavorStr = deps.formatFlavor(flavor)
                        log.error("flavor '%s' of trove %s is not installed",
                                  flavorStr, troveName)
                    else:
                        log.error("trove %s is not installed", troveName)

        troves = self.db.getTroves(troveInfo, withDeps = False,
                                   withFileObjects = True, pristine = False)

        seen = set()
        for trv in troves:
            self._recurseTrove(trv, duplicateFilterSet = seen)
            if self.finalCs:
                self.finalCs.addPrimaryTrove(
                         trv.getName(),
                         trv.getVersion().createShadow(versions.LocalLabel()),
                         trv.getFlavor())

        return self.finalCs

    def run(self, troveNameList, all=False):
        cs = self.generateChangeSet(troveNameList, all=all)
        if self.changeSetPath:
            cs.writeToFile(self.changeSetPath)

        return cs

class DiffObject(_FindLocalChanges):

    def __init__(self, troveNameList, db, cfg, all = False,
                 changesetPath = None, forceHashCheck = False,
                 asDiff=False, repos=None):
        if asDiff:
            display = DISPLAY_DIFF
        elif changesetPath:
            display = DISPLAY_NONE
        else:
            display = DISPLAY_CS

        verifier = _FindLocalChanges.__init__(self, db, cfg,
                        display=display,
                        forceHashCheck=forceHashCheck,
                        changeSetPath=changesetPath,
                        asDiff=asDiff, repos=repos)
        self.run(troveNameList, all=all)

class verify(DiffObject):

    def generateChangeSet(self, *args, **kwargs):
        cs = DiffObject.generateChangeSet(self, *args, **kwargs)
        if cs is not None:
            # verify doesn't display changes in collections because those, by
            # definition, match the database
            for trvCs in list(cs.iterNewTroveList()):
                if trove.troveIsCollection(trvCs.getName()):
                    cs.delNewTrove(*trvCs.getNewNameVersionFlavor())

        return cs

class LocalChangeSetCommand(_FindLocalChanges):

    def __init__(self, db, cfg, item, changeSetPath = None):
        changeObj = _FindLocalChanges.__init__(self, db, cfg,
                                               display=DISPLAY_NONE,
                                               allMachineChanges=True)
        cs = self.run([item])

        if not [ x for x in cs.iterNewTroveList() ]:
            log.error("there have been no local changes")
        else:
            cs.writeToFile(changeSetPath)
