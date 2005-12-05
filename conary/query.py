#
# Copyright (c) 2004-2005 rPath, Inc.
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
Provides the output for the "conary query" command
"""


import itertools
import os
import time

from conary import display
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import util

def displayTroves(db, cfg, troveSpecs = [], pathList = [],
                  ls = False, ids = False, sha1s = False, 
                  tags = False, info = False, deps = False, 
                  showBuildReqs = False, showDiff = False,
                  digSigs = False):
    """Displays troves after finding them on the local system

       @param db: Database instance to search for troves in
       @type db: local.database.Database
       @param cfg: conary config
       @type cfg: conarycfg.ConaryConfiguration
       @param troveSpecs: troves to search for
       @type troveSpecs: list of troveSpecs (n[=v][[f]])
       @param pathList: paths to match up to troves
       @type pathList: list of strings
       @param ls: If true, list files in the trove
       @type ls: bool
       @param ids: If true, list pathIds for files in the troves
       @type ids: bool
       @param sha1s: If true, list sha1s for files in the troves
       @type sha1s: bool
       @param tags: If true, list tags for files in the troves
       @type tags: bool
       @param info: If true, display general information about the trove
       @type info: bool
       @param deps: If true, display provides and requires information 
       for the trove.
       @type deps: bool
       @param showBuildReqs: If true, display the versions and flavors of the
       build requirements that were used to build the given troves
       @type deps: bool
       @param showDiff: If true, display the difference between the local and
       pristine versions of the trove
       @type showDiff: bool
       @param digSigs: If true, display digital signatures for a trove.
       @type digSigs: bool
       @rtype: None
    """

    troveTups, namesOnly, primary = getTrovesToDisplay(db, troveSpecs, pathList)

    iterChildren = not namesOnly 

    dcfg = LocalDisplayConfig(db, ls, ids, sha1s, digSigs, cfg.fullVersions,
                              tags, info, deps, showBuildReqs, cfg.fullFlavors,
                              iterChildren, cfg.showComponents)
    dcfg.setPrintDiff(showDiff)

    formatter = LocalTroveFormatter(dcfg)
    if primary:
        dcfg.setPrimaryTroves(set(troveTups))

    display.displayTroves(dcfg, formatter, troveTups)


def getTrovesToDisplay(db, troveSpecs, pathList=[]):
    """ Finds the given trove and path specifiers, and returns matching
        (n,v,f) tuples.
        @param db: database to search
        @type db: local.database.Database
        @param troveSpecs: troves to search for
        @type troveSpecs: list of troveSpecs (n[=v][[f]])
        @param pathList: paths which should be linked to some trove in this 
                         database.
        @type pathList: list of strings
        @rtype: troveTupleList (list of (name, version, flavor) tuples)
                and a boolean that is true if all troveSpecs passed in do not 
                specify version or flavor
    """

    namesOnly = True
    primary = True

    if troveSpecs:
        troveSpecs = [ cmdline.parseTroveSpec(x) for x in troveSpecs ]
    else:
        troveSpecs = []

    for troveSpec in troveSpecs:
        if troveSpec[1:] != (None, None):
            namesOnly = False

    pathList = [os.path.abspath(util.normpath(x)) for x in pathList]

    troveTups = []
    for path in pathList:
        for trove in db.iterTrovesByPath(path):
            troveTups.append((trove.getName(), trove.getVersion(), 
                              trove.getFlavor()))
    
    if not (troveSpecs or pathList):
	troveSpecs = [ (x, None, None) for x in sorted(db.iterAllTroveNames()) ]
        primary = False

    results = db.findTroves(None, troveSpecs)

    for troveSpec in troveSpecs:
        troveTups.extend(results.get(troveSpec, []))

    return troveTups, namesOnly, primary


class LocalDisplayConfig(display.DisplayConfig):
    def __init__(self, *args, **kw):
        display.DisplayConfig.__init__(self, *args, **kw)
        self.showDiff = False

    def setPrintDiff(self, b):
        self.showDiff = b

    def printDiff(self):
        return self.showDiff

    def needTroves(self):
        return self.showDiff or display.DisplayConfig.needTroves(self)

    def printSimpleHeader(self):
        return self.showDiff or display.DisplayConfig.printSimpleHeader(self)

class LocalTroveFormatter(display.TroveFormatter):

    def printTroveHeader(self, trove, n, v, f, indent):
        if self.dcfg.printDiff():
            self.printDiff(trove, n, v, f, indent)
        else:
            display.TroveFormatter.printTroveHeader(self, trove, n, v, f, 
                                                    indent)
        

    def printDiff(self, trv, n, v, f, indent):
        troveSource = self.dcfg.getTroveSource()

        localTrv = troveSource.getTrove(n,v,f, pristine=False)

        changes = localTrv.diff(trv)[2]
        changesByOld = dict(((x[0], x[1][0], x[1][1]), x) for x in changes)
        troveList = itertools.chain(trv.iterTroveList(),
               [ (x[0], x[2][0], x[2][1]) for x in changes if x[1][0] is None ])
        for (troveName, ver, fla) in sorted(troveList):
            change = changesByOld.get((troveName, ver, fla), None)
            if change: 
                newVer, newFla = change[2]

            self.printNVF(troveName, ver, fla)

            if change: 
                if newVer is None:
                    tups = troveSource.trovesByName(troveName)
                    if not tups:
                        print '  --> (Deleted or Not Installed)'
                    else:
                        print ('  --> Not linked to parent trove - potential'
                               ' replacements:')
                        for (dummy, newVer, newFla) in tups:
                            self.printNVF(troveName, newVer, newFla,
                                          format=display._chgFormat)
                else:
                    self.printNVF(troveName, newVer, newFla, 
                                  format=display._chgFormat)
