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
"""
Provides output methods for displaying troves
"""

import itertools
import time

#conary
from conary import files
from conary import trove
from conary.deps import deps
from conary.lib import log
from conary.lib.sha1helper import sha1ToString, md5ToString
from conary import metadata
from conary.repository import errors

def displayTroves(dcfg, formatter, troveTups):
    """
    display the given list of source troves 

    @param dcfg: stores information about the type of display to perform
    @type dcfg: display.DisplayConfig
    @param formatter: contains methods to display the troves
    @type formatter: display.TroveFormatter
    @param troveTups: the trove tuples to display
    @type troveTups: ordered list of (n,v,f) tuples
    """
    troveTups = list(filterComponents(troveTups, dcfg.getPrimaryTroves()))

    iter = iterTroveList(dcfg.getTroveSource(),
                         troveTups,
                         recurseAll=dcfg.recurseAll,
                         recurseOne=dcfg.recurseOne,
                         recursePackages=not dcfg.hideComponents(),
                         needTroves = dcfg.needTroves(),
                         getPristine = dcfg.getPristine(),
                         showNotByDefault = dcfg.showNotByDefault,
                         showWeakRefs = dcfg.showWeakRefs,
                         checkExists = dcfg.checkExists,
                         showNotExists = dcfg.showNotExists,
                         showFlags = dcfg.showTroveFlags,
                         primaryTroves = dcfg.getPrimaryTroves())

    allTups = list(iter)
    # let the formatter know what troves are going to be displayed
    # in order to determine what parts of the version/flavor to display
    troveTups = formatter.prepareTuples([x[0] for x in allTups])

    for (n,v,f), trv, troveFlags, indent in allTups:
        if dcfg.printTroveHeader():
            for ln in formatter.formatTroveHeader(trv, n, v, f, troveFlags, indent):
                print ln
            indent += 1
        else:
            indent = 0

        if not trv:
            # don't bother trying to print files for non-existant troves.
            continue

        if dcfg.printFiles():
            for ln in formatter.formatTroveFiles(trv, n, v, f, indent):
                print ln

def filterComponents(tupList, primaryTroves=[]):
    tups = set()
    for item in tupList:
        n, v, f = item
        if item not in primaryTroves and (n.split(':')[0], v, f) in tups:
            continue
        yield item
        tups.add((n, v, f))


def _sortTroves(a, b):
    aPkg, aComp = (a[0].split(':') + [None])[0:2]
    bPkg, bComp = (b[0].split(':') + [None])[0:2]
    aIsSource = int(aComp == 'source')
    bIsSource = int(bComp == 'source')

    return cmp((aPkg, a[1], a[2], aIsSource, aComp),
               (bPkg, b[1], b[2], bIsSource, bComp))

TROVE_BYDEFAULT = 1 << 0
TROVE_STRONGREF = 1 << 1
TROVE_HASTROVE  = 1 << 2

def iterTroveList(troveSource, troveTups, recurseAll=False,
                  recurseOne=False, recursePackages=False,
                  needTroves=False, getPristine=True,
                  showNotByDefault=False, showWeakRefs=False,
                  checkExists=False, showNotExists=False,
                  showFlags=False, primaryTroves=[]):
    """
    Given a troveTup list, iterate over those troves and their child troves
    as specified by parameters

    @param troveSource: place to retrieve the trove instances matching troveTups
    @type troveSource: display.DisplayConfig
    @param recurseAll: if true, recursively descend through the listed troves
    @type recurseAll: bool
    @param recurseOne: if True, include just the first level of troves below
    the listed troves (but do not recurse)
    @param needTroves: if True, return trove objects.  Otherwise, return None
    as each trove object
    @type needTroves: bool
    @param getPristine: if True, get pristine trove objects
    @type getPristine: bool
    @param showNotByDefault: if True, yield not bydefault troves 
    @type showNotByDefault: bool
    @param showWeakRefs: if True, yield troves that are weak references
    @type showWeakRefs: bool
    @param checkExists: if True, add flag MISSING for troves that do not
    exist (but are referenced) in this troveSource
    @type checkExists: bool
    @param showNotExists: if True, show troves that do not exist (but are 
    referenced) in this troveSource
    @type showNotExists: bool

    @rtype: yields (troveTup, troveObj, flags, indent) tuples
    """
    if not getPristine:
        kw = {'pristine' : False}
    else:
        kw = {}

    if recurseOne: # when we recurse one level deep, always recurse packages
                   # otherwise you might try conary q tmpwatch --troves and
                   # have that give no result.
        recursePackages = True

    if needTroves or showFlags:
        troves = troveSource.getTroves(troveTups, withFiles=False, **kw)
        troveCache = dict(itertools.izip(troveTups, troves))
    elif recurseAll or recurseOne or recursePackages:
        if recursePackages:
            if recurseOne or recurseAll:
                colls = [ x for x in troveTups if trove.troveIsCollection(x[0]) ]
            else:
                colls = [ x for x in troveTups if trove.troveIsPackage(x[0])]
        else:
            colls = [ x for x in troveTups if trove.troveIsCollection(x[0])
                                           and not trove.troveIsPackage(x[0])]
        troves = troveSource.getTroves(colls, withFiles=False, **kw)
        troveCache = dict(itertools.izip(colls, troves))
    else:
        troves = [None] * len(troveTups)
        troveCache = {}

    hasTrovesCache = {}
    if recurseAll or recurseOne or recursePackages:
        # we're recursing, we can cache a lot of information -
        # troves we'll need, hasTroves info we'll need.
        # If we cache this now, we cut down significantly on the
        # number of function calls we need.
        childTups = list(itertools.chain(*( x.iterTroveList(strongRefs=True) 
                         for x in troves if x)))
        if recurseAll:
            if recursePackages:
                _check = lambda x: trove.troveIsCollection(x[0])
            else:
                _check = lambda x: (trove.troveIsCollection(x[0])
                                    and not trove.troveIsPackage(x[0]))
            colls = set(x for x in troveTups if _check(x))
            childColls = [ x for x in childTups if _check(x)]
            troves = troveSource.getTroves(childColls, withFiles=False, **kw)
            troveCache.update(itertools.izip(childColls, troves))
        allTups = troveTups + childTups
        if checkExists:
            hasTroves = troveSource.hasTroves(allTups)
            hasTrovesCache = dict(itertools.izip(allTups, hasTroves))
        troves = [ troveCache.get(x, None) for x in troveTups ]

    indent = 0
    seen = set()  # cached info about what troves we've called hasTrove on.

    for troveTup, trv in itertools.izip(troveTups, troves):
        if recurseAll:
            # recurse all troves, depth first.
            topTrove = trv

            troves = [(trv, (troveTup, trv, 
                    TROVE_BYDEFAULT | TROVE_STRONGREF | TROVE_HASTROVE, 0))]

            while troves:
                topTrove, info = troves.pop(0)
                yield info

                troveTup, trv, flags, depth = info
                if not flags & TROVE_HASTROVE:
                    # we can't recurse this trove, it doesn't exist.
                    continue
                if not trove.troveIsCollection(troveTup[0]):
                    # this could have been one of the troves we specified
                    # initially, in which case trying to recurse it will
                    # not work.
                    continue
                if trove.troveIsPackage(troveTup[0]) and not recursePackages:
                    continue

                newTroveTups = trv.iterTroveList(strongRefs=True,
                                                 weakRefs=showWeakRefs)

                newTroveTups = sorted(newTroveTups)
                if needTroves or trv.isRedirect():
                    # might as well grab all the troves, we're supposed
                    # to yield them all.
                    neededTroveTups = [ x for x in newTroveTups \
                                                    if x not in troveCache ]

                    newTroves = troveSource.getTroves(neededTroveTups,
                                                      withFiles=False)

                    troveCache.update(x for x \
                        in itertools.izip(neededTroveTups, newTroves) if x[1])
                    seen.update(neededTroveTups)
                else:
                    newColls = [ x for x in trv.iterTroveList(weakRefs=True,
                                                              strongRefs=True)
                                             if trove.troveIsCollection(x[0])
                                            and x not in troveCache ]
                    newTroves = troveSource.getTroves(newColls,
                                                      withFiles=False)
                    troveCache.update(x for x in itertools.izip(newColls, newTroves))
                    seen.update(newColls)

                if checkExists:
                    toCheck = set(x for x in trv.iterTroveList(True, True))
                    alsoToCheck = {}
                    for newTrove in newTroves:
                        if newTrove is None:
                            continue
                        alsoToCheck.update(dict((x, newTrove) for x in \
                                        newTrove.iterTroveList(True, True) 
                                        if x not in toCheck and x not in seen))

                    if not showNotByDefault:
                        newToCheck = []
                        for tup in toCheck:
                            if topTrove.hasTrove(*tup):
                                if topTrove.includeTroveByDefault(*tup):
                                    newToCheck.append(tup)
                            elif trv.includeTroveByDefault(*tup):
                                newToCheck.append(tup)
                        for tup, parent in alsoToCheck.iteritems():
                            if topTrove.hasTrove(*tup):
                                if top.includeTroveByDefault(*tup):
                                    newToCheck.append(tup)
                            elif trv.hasTrove(*tup):
                                if trv.includeTroveByDefault(*tup):
                                    newToCheck.append(tup)
                            elif parent.includeTroveByDefault(*tup):
                                newToCheck.append(tup)
                        toCheck = newToCheck
                    else:
                        toCheck = list(toCheck)

                    if toCheck:
                        seen.update(toCheck)
                        toCheck = [ x for x in toCheck if x not in hasTrovesCache ]
                        hasTroves = troveSource.hasTroves(toCheck)
                        hasTrovesCache.update(x for x in itertools.izip(toCheck, hasTroves))

                trovesToAdd = []
                depth += 1
                for troveTup in newTroveTups:
                    if not topTrove.hasTrove(*troveTup):
                        topTrove = trv
                    if not recursePackages and trove.troveIsComponent(troveTup[0]):
                        continue
                    installByDefault = topTrove.includeTroveByDefault(*troveTup)


                    if not installByDefault and not showNotByDefault:
                        continue

                    flags = TROVE_STRONGREF
                    if installByDefault:
                        flags |= TROVE_BYDEFAULT
                    if not checkExists or hasTrovesCache[troveTup]:
                        flags |= TROVE_HASTROVE
                    elif not showNotExists:
                        continue

                    newTrove = troveCache.get(troveTup, None)
                    if trove.troveIsCollection(troveTup[0]):
                        trovesToAdd.append((topTrove,
                                (troveTup, newTrove, flags, depth)))
                    else:
                        yield (troveTup, newTrove, flags, depth)

                troves = trovesToAdd + troves
        else:
            # recurse one level or recurse no levels.
            yield troveTup, trv, TROVE_STRONGREF | TROVE_BYDEFAULT | TROVE_HASTROVE, 0

            if (trv and 
                (recurseOne 
                 or recursePackages and (trove.troveIsPackage(trv.getName())))):
                newTroveTups = trv.iterTroveListInfo()

                if not showWeakRefs:
                    newTroveTups = (x for x in newTroveTups if x[2])
                if not showNotByDefault:
                    newTroveTups = (x for x in newTroveTups if x[1])

                newTroveTups = sorted(newTroveTups)

                if needTroves or trv.isRedirect():
                    newTroves = troveSource.getTroves(
                                                [x[0] for x in newTroveTups],
                                                withFiles=False)
                else:
                    newTroves = [None] * len(newTroveTups)

                if checkExists:
                    toAdd = [ x[0] for x in newTroveTups if x[0] not in hasTrovesCache ]
                    hasTroves = troveSource.hasTroves(toAdd)
                    hasTrovesCache.update(itertools.izip(toAdd, hasTroves))
                    hasTroves = [ hasTrovesCache[x[0]] for x in newTroveTups ]
                else:
                    hasTroves = [True] * len(newTroveTups)

                for (troveTup, byDefault, strongRef), trv, hasTrove \
                        in itertools.izip(newTroveTups, newTroves, hasTroves):
                    flags = 0
                    if strongRef:
                        flags |= TROVE_STRONGREF
                    if byDefault:
                        flags |= TROVE_BYDEFAULT
                    if hasTrove:
                        flags |= TROVE_HASTROVE
                    elif not showNotExists:
                        continue
                    yield troveTup, trv, flags, 1


class DisplayConfig:
    """ Configuration for a display command.  Contains both specified
        parameters as well as information about some derived parameters 
        (such as whether or not a particular display command will need
        file lists)
    """

    def __init__(self, troveSource=None, affinityDb=None):
        self.troveSource = troveSource
        self.affinityDb = affinityDb
        self.setTroveDisplay()
        self.setFileDisplay()
        self.setChildDisplay()
        self.setPrimaryTroves(set())

    def setTroveDisplay(self, deps=False, info=False, showBuildReqs=False,
                        digSigs=False, showLabels=False, fullVersions=False,
                        fullFlavors=False, baseFlavors=[],
                        showComponents=False):
        self.deps = deps
        self.info = info
        self.digSigs = digSigs
        self.showBuildReqs = showBuildReqs
        self.fullVersions = fullVersions
        self.showLabels = showLabels
        self.fullFlavors = fullFlavors
        self.baseFlavors = baseFlavors
        # FIXME: showComponents should really be in setChildDisplay.
        self.showComponents = showComponents

    def setFileDisplay(self, ls = False, lsl = False, ids = False, 
                       sha1s = False, tags = False, fileDeps = False,
                       fileVersions = False):
        ls = ls or lsl
        self.ls = ls or lsl
        self.lsl = lsl
        self.ids = ids
        self.sha1s = sha1s
        self.tags = tags
        self.fileDeps = fileDeps
        self.fileVersions = fileVersions

    def setChildDisplay(self, recurseAll = False, recurseOne = False,
                        showNotByDefault = False, showWeakRefs = False,
                        showTroveFlags = False, displayHeaders = False, 
                        checkExists = False, showNotExists = False):
        self.recurseAll = recurseAll
        self.recurseOne = recurseOne
        self.showNotByDefault = showNotByDefault
        self.showWeakRefs = showWeakRefs
        self.showTroveFlags = showTroveFlags
        self.displayHeaders = displayHeaders
        self.showNotExists = showNotExists
        self.checkExists = checkExists

    def setPrimaryTroves(self, pTroves):
        self.primaryTroves = pTroves

    def getPrimaryTroves(self):
        return self.primaryTroves

    #### Accessors 
    #### Methods to grab information passed into the DisplayConfig

    def getTroveSource(self):
        return self.troveSource

    def printBuildReqs(self):
        return self.showBuildReqs

    def printTroveHeader(self):
        return self.info or self.showBuildReqs or self.deps or not self.printFiles() or self.displayHeaders

    def printSimpleHeader(self):
        return not self.info and (not self.showBuildReqs or self.displayHeaders)

    def printDeps(self):
        return self.deps

    def printPathIds(self):
        return self.ids

    def printSha1s(self):
        return self.sha1s

    def printDigSigs(self):
        return self.digSigs

    def printInfo(self):
        return self.info

    def printFiles(self):
        return self.ls or self.ids or self.sha1s or self.tags or self.fileDeps or self.fileVersions

    def isVerbose(self):
        return self.lsl

    def useFullVersions(self):
        return self.fullVersions

    def useFullFlavors(self):
        return self.fullFlavors

    def hideComponents(self):
        return (not self.showComponents
                and not self.printFiles()
                and not self.deps)

    #### Needed Data
    #### What the given display configuration implies that we need

    def needTroves(self):
        # we need the trove 
        return self.info or self.showBuildReqs or self.digSigs or self.deps or self.printFiles()

    def needFiles(self):
        return self.printFiles()

    def needFileObjects(self):
        return self.needFiles()

    def getPristine(self):
        return True


class TroveTupFormatter:
    """ Formats (n,v,f) troveTuples taking into account the other
        tuples that have and will be formatted
    """
    
    def __init__(self, dcfg=None):
        self._vCache = {}
        self._fCache = {}

        if not dcfg:
            dcfg = DisplayConfig()

        self.dcfg = dcfg

    def prepareTuples(self, troveTups):
        for (n,v,f) in troveTups:
            self._vCache.setdefault(n, set()).add(v)
            self._fCache.setdefault(n, set()).add(f)

    def getTupleStrings(self, n, v, f):
        """
            returns potentially shortened display versions and flavors for
            a trove tuple.

            @param n: trove name
            @type n: str
            @param v: trove version
            @type v: versions.Version
            @param f: trove flavor
            @type f: deps.deps.Flavor
            @rtype: (vStr, fStr) where vStr is the version string to display
            for this trove and fStr is the flavor string (may be empty)
        """

        if self.dcfg.useFullVersions():
            vStr = str(v)
        elif self.dcfg.showLabels:
            vStr = '%s/%s' % (v.branch().label(), v.trailingRevision())
        elif len(self._vCache.get(n, [])) > 1:
            # print the trailing revision unless there
            # are two versions that are on different
            # branches.

            vers = self._vCache[n]
            branch = v.branch()

            vStr = None
            for ver in vers:
                if ver.branch() != branch:
                    vStr = str(v)

            if not vStr:
                vStr = str(v.trailingRevision())
        else:
            vStr = str(v.trailingRevision())

        if self.dcfg.useFullFlavors():
            fStr = str(f)
        else:
            # print only the flavor differences between
            # the two troves.

            # FIXME Document all this!
            if self.dcfg.affinityDb:
                installed = set()
                #installed = set([x[2] for x in self.dcfg.affinityDb.trovesByName(n)])
                installed.discard(f)
                installed = list(installed)
            else:
                installed = []

            flavors = list(self._fCache.get(n, []))
            if installed:
                for baseFlavor in self.dcfg.baseFlavors:
                    flavors += [ deps.overrideFlavor(baseFlavor, x) for x in installed ]
            else:
                flavors += self.dcfg.baseFlavors


            if len(flavors) > 1:
                fStr = deps.flavorDifferences(
                                    list(flavors) + installed + [f],
                                    strict=False)[f]

                ISD = deps.InstructionSetDependency
                if f.hasDepClass(ISD) and fStr.hasDepClass(ISD):
                    allDeps = set(x.name for x in f.iterDepsByClass(ISD))
                    allDeps.difference_update(x.name for x in
                                              fStr.iterDepsByClass(ISD))
                    for depName in allDeps:
                        fStr.addDep(ISD, deps.Dependency(depName))
                fStr = str(fStr)
            else:
                fStr = ''

        return vStr, fStr

    def formatNVF(self, name, version, flavor, indent=False, format=None):
        """ Print a name, version, flavor tuple
        """
        vStr, fStr = self.getTupleStrings(name, version, flavor)
        if format:
            return format % (name, vStr, fStr)
        format = '' # reuse it for indentation
        if indent:
            format = '  ' * indent
        if fStr:
            return '%s%s=%s[%s]' % (format, name, vStr, fStr)
        else:
            return '%s%s=%s' % (format, name, vStr)

class TroveFormatter(TroveTupFormatter):
    """ Formats trove objects (displaying more than just NVF)
    """

    def formatInfo(self, trove):
        """ returns iterator of format lines about this local trove """
        # TODO: it'd be nice if this were set up to do arbitrary
        # formats...

        n, v, f = trove.getName(), trove.getVersion(), trove.getFlavor()
        dcfg = self.dcfg
        troveSource = dcfg.getTroveSource()

        sourceName = trove.getSourceName()
        sourceTrove = None

        if sourceName:
            try:
                sourceVer = v.getSourceVersion()
                if sourceVer.isOnLocalHost():
                    sourceVer = sourceVer.parentVersion()

                sourceTrove = troveSource.getTrove(
                    sourceName, sourceVer, deps.Flavor(), withFiles = False)
                # FIXME: all trove sources should return TroveMissing
                # on failed getTrove calls 
            except errors.TroveMissing:
                pass

        elif n.endswith(':source'):
            sourceTrove = trove

        if trove.getBuildTime():
            buildTime = time.strftime("%c",
                                time.localtime(trove.getBuildTime()))
        else:
            buildTime = "(unknown)"

        if trove.getSize():
            size = "%s" % trove.getSize()
        else:
            size = "(unknown)"

        yield "%-30s %s" % \
            (("Name      : %s" % trove.getName(),
             ("Build time: %s" % buildTime)))

        if dcfg.fullVersions:
            yield "Version   : %s" %v
            yield "Label     : %s" % v.branch().label().asString()
        else:
            yield "%-30s %s" % \
                (("Version   : %s" % 
                            v.trailingRevision().asString()),
                 ("Label     : %s" % 
                            v.branch().label().asString()))

        yield '%-30s' % ("Size      : %s" % size)
        if hasattr(troveSource, 'trovesArePinned'):
            yield "Pinned    : %s" % troveSource.trovesArePinned(
                                                            [ (n, v, f) ])[0]

        yield "%-30s" % ("Flavor    : %s" % deps.formatFlavor(f))

        if sourceTrove:
            if not n.endswith(':source'):
                yield 'Source    : %s' % trove.getSourceName()
            if hasattr(troveSource, 'getMetadata'):
                for ln in metadata.formatDetails(troveSource, None, n, 
                                                 v.branch(), sourceTrove):
                    yield ln

            cl = sourceTrove.getChangeLog()
            if cl:
                yield "Change log: %s (%s)" % (cl.getName(), cl.getContact())
                lines = cl.getMessage().split("\n")[:-1]
                for l in lines:
                    yield "    " + l

        if log.getVerbosity() <= log.DEBUG:
            yield "%-30s %s" % (("Incomp.   : %s" %
                                 bool(trove.troveInfo.incomplete())),
                                ("TroveVer  : %s" %
                                            trove.troveInfo.troveVersion()))


    def formatTroveHeader(self, trove, n, v, f, flags, indent):
        """ Print information about this trove """

        dcfg = self.dcfg

        if dcfg.printSimpleHeader():
            ln =  self.formatNVF(n, v, f, indent)
            if dcfg.showTroveFlags:
                fmtFlags = []
                if not flags & TROVE_HASTROVE:
                    fmtFlags.append('Missing')
                if not flags & TROVE_BYDEFAULT:
                    fmtFlags.append('NotByDefault')
                if not flags & TROVE_STRONGREF:
                    fmtFlags.append('Weak')
                if trove and trove.isRedirect():
                    for rName, rBranch, rFlavor in trove.iterRedirects():
                        if rFlavor is None:
                            flag = 'Redirect -> %s=%s' % (rName, rBranch)
                        else:
                            flag = 'Redirect -> %s=%s[%s]' % (rName, rBranch, 
                                                              rFlavor)
                        fmtFlags.append(flag)

                if fmtFlags:
                    ln += ' [%s]' % ','.join(fmtFlags)
            yield ln
            indent += 1
        else:
            indent = 0
        if not trove:
            # don't bother trying to print extra info for non-existant
            # troves (they might have had flag info printed though)
            return

        if dcfg.printInfo():
            for line in self.formatInfo(trove):
                yield line
        if dcfg.printDigSigs():
            for line in self.formatDigSigs(trove, indent):
                yield line
        if dcfg.printBuildReqs():
            for buildReq in sorted(trove.getBuildRequirements()):
                yield '  ' * (indent) + self.formatNVF(*buildReq)
        elif dcfg.printDeps():
            for line in self.formatDeps(trove.getProvides(), 
                                        trove.getRequires(),
                                        indent=indent):
                yield line

    def formatDeps(self, provides, requires, indent=0, showEmpty=True):
        for name, dep in (('Provides', provides),
                              ('Requires', requires)):
            if not dep and not showEmpty:
                continue

            spacer = '  ' * indent

            yield '%s%s:' %(spacer, name)
            if not dep:
                yield '%s   None' % (spacer)
            else:
                lines = str(dep).split('\n')
                for l in lines:
                    yield '%s  %s' % (spacer, l)
            yield ''

    def formatDigSigs(self, trv, indent=0):
        sigList = [x for x in trv.troveInfo.sigs.digitalSigs.iter()]
        for fingerprint, timestamp, sigTuple \
                in trv.troveInfo.sigs.digitalSigs.iter():
            yield 2*indent*' ' + 'Digital Signature:'
            yield 2*indent*' ' + '    %s:' %fingerprint + ' ' + time.ctime(timestamp)

    def formatTroveFiles(self, trove, n, v, f, indent=0):
        """ Print information about the files associated with this trove """
        dcfg = self.dcfg
        needFiles = dcfg.needFileObjects()

        troveSource = dcfg.getTroveSource()

        iter = troveSource.iterFilesInTrove(n, v, f, 
                                            sortByPath = True, 
                                            withFiles = needFiles)
        if needFiles:
            for (pathId, path, fileId, version, file) in iter:
                for ln in self.formatFile(pathId, path, fileId, version, file,
                                          indent=indent):
                    yield ln
        else:
            for (pathId, path, fileId, version) in iter:
                yield path

    def formatFile(self, pathId, path, fileId, version, fileObj=None, 
                   prefix='', indent=0):
        taglist = ''
        sha1 = ''
        id = ''

        dcfg = self.dcfg
        verbose = dcfg.isVerbose()

        if verbose and isinstance(fileObj, files.SymbolicLink):
            name = "%s -> %s" % (path, fileObj.target())
        else:
            name = path

        if dcfg.tags:
            tags = []
            if fileObj.tags:
                tags.extend(fileObj.tags)
            if fileObj.flags.isInitialContents():
                tags.append('initialContents')
            if fileObj.flags.isConfig():
                tags.append('config')
            if tags:
                taglist = ' [' + ' '.join(tags) + ']' 
        if dcfg.sha1s:
            if hasattr(fileObj, 'contents') and fileObj.contents:
                sha1 = sha1ToString(fileObj.contents.sha1()) + ' '
            else:
                sha1 = ' '*41

        if dcfg.ids and pathId:
            id = md5ToString(pathId) + ' ' + sha1ToString(fileObj.fileId()) + ', '
        if dcfg.fileVersions:
            if dcfg.useFullVersions():
                verStr = '    %s' % version
            elif dcfg.showLabels:
                verStr = '    %s/%s' % (version.branch().label(), version.trailingRevision())
            else:
                verStr = '    %s' % version.trailingRevision()
        else:
            verStr = ''

        spacer = '  ' * indent

        if verbose: 
            ln = "%s%s%s%s%s    1 %-8s %-8s %s %s %s%s%s" % \
              (spacer,
               prefix, id, sha1, fileObj.modeString(), fileObj.inode.owner(), 
               fileObj.inode.group(), fileObj.sizeString(), 
               fileObj.timeString(), name, taglist, verStr)
        else:
            ln = "%s%s%s%s%s%s" % (spacer, id, sha1, path, taglist, verStr)

        yield ln

        if dcfg.fileDeps:
            for ln in self.formatDeps(fileObj.provides(), fileObj.requires(),
                                      indent + 1, showEmpty = False):
                yield ln



#######################################################
#
# Job display functions
#
#######################################################

def displayJobLists(dcfg, formatter, jobLists, prepare=True):
    formatter.prepareJobLists(jobLists)

    for index, jobList in enumerate(jobLists):
        displayJobs(dcfg, formatter, jobList, prepare=False, jobNum=index,
                                                             total=totalJobs)

def displayJobs(dcfg, formatter, jobs, prepare=True, jobNum=0, total=0):
    """
    display the given list of jobs

    @param dcfg: stores information about the type of display to perform
    @type dcfg: display.DisplayConfig
    @param formatter: contains methods to display the troves
    @type formatter: display.TroveFormatter
    @param jobs: the job tuples to display
    @type jobs: ordered list of (n,v,f) job
    """
    if prepare:
        formatter.prepareJobs(jobs)

    if jobNum and total:
        print formatter.formatJobNum(index, totalJobs)
    
    for job, comps in formatter.compressJobList(sorted(jobs)):
        if dcfg.printTroveHeader():
            for ln in formatter.formatJobHeader(job, comps):
                print ln

        if dcfg.printFiles():
            for ln in formatter.formatJobFiles(job):
                print ln


class JobDisplayConfig(DisplayConfig):

    def setJobDisplay(self, showChanges=False, compressJobs=False):
        self.showChanges = showChanges
        self.compressJobList = compressJobs

    def __init__(self, *args, **kw):
        DisplayConfig.__init__(self, *args, **kw)
        self.setJobDisplay()

    def compressJobs(self):
        """ compress jobs so that updates of components are displayed with 
            the update of their package
        """
        return self.compressJobList and not self.needFiles()

    def needOldFileObjects(self):
        """ If true, we're going to display information about the old  
            versions of troves.
        """
        return self.showChanges

    def needTroves(self):
        """ 
            If true, we're going to display information about the 
            new versions of troves.
        """
        return self.showChanges or DisplayConfig.needTroves(self)

    def printFiles(self):
        """  
             If true, we're going to display file lists associated with
             this job.
        """
        return self.showChanges or DisplayConfig.printFiles(self)

    def printTroveHeader(self):
        return self.showChanges or DisplayConfig.printTroveHeader(self)


class JobTupFormatter(TroveFormatter):

    def __init__(self, dcfg=None, **kw):
        if not dcfg:
            dcfg = JobDisplayConfig(**kw)
        TroveFormatter.__init__(self, dcfg)

    def prepareJobLists(self, jobLists):
        """ 
            Store information about the jobs that are planned to be 
            displayed, so that the correct about of version/flavor
            will be displayed.
        """
        tups = []
        for jobList in jobLists:
            for job in jobList:
                (name, (oldVer, oldFla), (newVer, newFla)) = job[:3]
                if oldVer:
                    tups.append((name, oldVer, oldFla))

                if newVer:
                    tups.append((name, newVer, newFla))
        self.prepareTuples(tups)

    def prepareJobs(self, jobs):
        self.prepareJobLists([jobs])

    def compressJobList(self, jobTups):
        """ Compress component display
        """
        compressJobs = self.dcfg.compressJobs()
        if not compressJobs:
            for jobTup in jobTups:
                yield jobTup, []
            return

        compsByJob = {}
        for jobTup in jobTups:  
            name = jobTup[0]
            if ':' in name:
                pkg, comp = jobTup[0].split(':')
                pkgJob = (pkg, jobTup[1], jobTup[2])
                compsByJob.setdefault(pkgJob, [False, []])[1].append(comp)
            else:
                compsByJob.setdefault(jobTup[:3], [False, []])[0] = True

        for jobTup in jobTups:  
            name = jobTup[0]
            if ':' in name:
                pkg, comp = jobTup[0].split(':')
                pkgJob = (pkg, jobTup[1], jobTup[2])
                if not compsByJob[pkgJob][0]:
                    yield jobTup, []
            else:
                yield jobTup, compsByJob[jobTup[:3]][1]
        
    def getJobStrings(self, job):
        """Get the version/flavor strings to display for this job"""
        # format a single job entry
        (name, (oldVer, oldFla), (newVer, newFla)) = job

        if newVer:
            newVer, newFla = self.getTupleStrings(name, newVer, newFla)

        if oldVer:
            oldVer, oldFla = self.getTupleStrings(name, oldVer, oldFla)

        return oldVer, oldFla, newVer, newFla

    def formatJobTups(self, jobs, indent=''):
        if self.dcfg.compressJobs():
            iter = self.compressJobList(sorted(jobs))
        else:
            iter = ((x, []) for x in sorted(jobs))

        for job, comps in iter:
            yield self.formatJobTup(job, components=comps, indent=indent)

    def formatJobTup(self, job, components=[], indent=''):
        job = job[:3]
        (name, (oldVer, oldFla), (newVer, newFla)) = job
        oldInfo, oldFla, newInfo, newFla = self.getJobStrings(job)

        if components:
            name = '%s(:%s)' % (name, ' :'.join(sorted(components)))

        if oldInfo:
            if oldFla:
                oldInfo += '[%s]' % oldFla

        if newInfo:
            if newFla:
                newInfo += '[%s]' % newFla

        if not oldInfo:
            return '%sInstall %s=%s' % (indent, name, newInfo)
        elif not newInfo:
            return '%sErase   %s=%s' % (indent, name, oldInfo)
        else:
            return '%sUpdate  %s (%s -> %s)' % (indent, name, oldInfo, newInfo)

class JobFormatter(JobTupFormatter):

    def formatJobNum(self, jobNum, total):
        return 'Job %d of %d:' %(num + 1, totalJobs)

    def formatJobHeader(self, job, comps):
        dcfg = self.dcfg
        yield self.formatJobTup(job, comps)
        if dcfg.printInfo():
            trove = dcfg.troveSource.getTrove(job[0], *job[2])
            for ln in self.formatInfo(trove):
                yield ln
        elif dcfg.printDeps():
            trvCs = dcfg.troveSource.getTroveChangeSet(job)
            for ln in self.formatDeps(trvCs.getProvides(), trvCs.getRequires()):
                yield ln

    def formatJobFiles(self, job):
        """ Print information about the files associated with this trove """
        dcfg = self.dcfg
        needFiles = dcfg.needFileObjects()
        needOldFiles = dcfg.needOldFileObjects()

        troveSource = dcfg.getTroveSource()

        iter = troveSource.iterFilesInJob(job,
                                          sortByPath = True, 
                                          withFiles = needFiles, 
                                          withOldFiles = needOldFiles)
        if needFiles:
            if needOldFiles:
                for (pathId, path, fileId, version, fileObj, 
                     oldPath, oldFileId, oldVersion, oldFileObj, modType) in iter:

                    if modType == troveSource.NEW_F:
                        prefix = ' New '
                        for ln in self.formatFile(pathId, path, fileId,
                                                  version, fileObj,
                                                  prefix=prefix, indent=1):
                            yield ln

                    elif modType == troveSource.MOD_F:
                        for ln in self.formatFileChange(pathId, path, fileId, 
                                           version, fileObj, oldPath, 
                                           oldFileId, oldVersion, oldFileObj, 
                                           indent=1):
                            yield ln

                    elif modType == troveSource.OLD_F:
                        prefix = ' Del '
                        for ln in self.formatFile(pathId, oldPath, oldFileId,
                                                  oldVersion, oldFileObj,
                                                  prefix=prefix, indent=1):
                            yield ln


            else:
                for (pathId, path, fileId, version, fileObj, modType) in iter:
                    if modType == troveSource.NEW_F:
                        prefix = ' New '
                    elif modType == troveSource.MOD_F:
                        prefix = ' Mod '
                    elif modType == troveSource.OLD_F:
                        prefix = ' Del '

                    for ln in self.formatFile(pathId, path, fileId, version, 
                                              fileObj, prefix=prefix,
                                              indent=1):
                        yield ln

        else:
            for (pathId, path, fileId, version) in iter:
                yield path

    def formatFileChange(self, pathId, path, fileId, version, 
                         fileObj, oldPath, oldFileId, oldVersion, oldFileObj,
                         indent=1):
        for ln in self.formatFile(pathId, oldPath, oldFileId, oldVersion,
                                  oldFileObj, prefix=' Mod ', indent=indent):
            yield ln

        dcfg = self.dcfg
        #only print out data that has changed on the second line
        #otherwise, print out blank space
        mode = owner = group = size = time = name = ''
        if oldPath != path:
            if isinstance(fileObj, files.SymbolicLink):
                name = "%s -> %s" % (path, fileObj.target())
            else:
                name = path
        elif isinstance(fileObj, files.SymbolicLink):
            if not isinstance(oldFileObj, files.SymbolicLink):
                name = "%s -> %s" % (oldPath, fileObj.target())
            elif fileObj.target() != oldFileObj.target():
                    name = "%s -> %s" % (oldPath, fileObj.target())

        space = ''
        if dcfg.printPathIds() and pathId:
            space += ' '*33
        if dcfg.printSha1s():
            if hasattr(oldFileObj, 'contents') and oldFileObj.contents:
                oldSha1 = oldFileObj.contents.sha1()
            else:
                sha1 = None

            if hasattr(fileObj, 'contents') and fileObj.contents:
                sha1 = fileObj.contents.sha1()
            else:
                sha1 = None

            if sha1 and sha1 != oldSha1:
                sha1 = sha1ToString(sha1) + ' '
            else:
                sha1 = ' '*41
        else:
            sha1 = ''

        if fileObj.modeString() != oldFileObj.modeString():
            mode = fileObj.modeString()
        if fileObj.inode.owner() != oldFileObj.inode.owner():
            owner = fileObj.inode.owner()
        if fileObj.inode.group() != oldFileObj.inode.group():
            group = fileObj.inode.group()
        if fileObj.sizeString() != oldFileObj.sizeString():
            size = fileObj.sizeString()
        if fileObj.timeString() != oldFileObj.timeString():
            time = fileObj.timeString()
        if not dcfg.tags or not fileObj.tags:
            taglist = ''
        else:
            taglist = ' [' + ' '.join(fileObj.tags) + ']' 

        spacer = '  ' * indent
        yield "%s---> %s%s%-10s      %-8s %-8s %8s %11s %s%s" % \
          (spacer, space, sha1, mode, owner, group, size, time, name, taglist)
