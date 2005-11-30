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
Provides output methods for displaying troves
"""

import itertools
import os
import time

#conary
from conary import files
from conary.deps import deps
from conary.lib import log
from conary.lib import util
from conary.lib.sha1helper import sha1ToString, md5ToString
from conary import metadata
from conary.repository import errors

_troveFormat  = "%-39s %s"
_troveFormatWithFlavor  = "%-39s %s[%s]"
_fileFormat = "    %-35s %s"
_grpFormat  = "  %-37s %s"
_grpFormatWithFlavor  = "  %-37s %s[%s]"
_chgFormat  = "  --> %-33s %s[%s]"

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
    # let the formatter know what troves are going to be displayed
    # in order to display the troves
    formatter.prepareTuples(troveTups)

    iter = iterTroveList(dcfg.getTroveSource(), 
                         troveTups, 
                         walkTroves=dcfg.walkTroves(),
                         iterTroves=dcfg.iterTroves(),
                         needTroves = dcfg.needTroves(),
                         needFiles = dcfg.needFiles())
    if dcfg.hideComponents():
        iter = skipComponents(iter, dcfg.getPrimaryTroves())

    for (n,v,f), trv, indent in iter:
        if dcfg.printTroveHeader():
            for ln in formatter.formatTroveHeader(trv, n, v, f, indent):
                print ln

        if not indent and dcfg.printFiles():
            for ln in formatter.formatTroveFiles(trv, n, v, f, indent):
                print ln

def skipComponents(tupList, primaryTroves=[]):
    tups = set()
    for item in tupList:
        n, v, f = item[0]
        if item[0] not in primaryTroves and (n.split(':')[0], v, f) in tups:
            continue
        yield item
        tups.add((n, v, f))


def iterTroveList(troveSource, troveTups, walkTroves=False, 
                  iterTroves=False, needTroves=False, 
                  needFiles=False):
    """
    Given a troveTup list, iterate over those troves and their child troves
    as specified by parameters

    @param troveSource: place to retrieve the trove instances matching troveTups
    @type troveSource: display.DisplayConfig
    @param walkTroves: if true, recursively descend through the listed troves
    @type bool
    @param iterTroves: if True, include just the first level of troves below
    the listed troves (but do not recurse)
    @param needTroves: if True, return trove objects.  Otherwise, return None
    as each trove objec.t
    @type bool
    @param needFiles: True if the returned trove objects should contain files
    @type bool
    @rtype: yields (troveTup, troveObj, indent) tuples
    """
    assert(not (walkTroves and iterTroves))

    if needTroves or walkTroves or iterTroves:
        troves = troveSource.getTroves(troveTups, withFiles=needFiles)
    else:
        troves = [None] * len(troveTups)
    
    indent = 0

    for troveTup, trv in itertools.izip(troveTups, troves):
        if walkTroves:
            # walk troveSet yields troves that are children of these troves
            # and the trove itself.
            iter = troveSource.walkTroveSet(trv, ignoreMissing=True,
                                            withFiles = needFiles)
            newTroves = sorted(iter, key=lambda y: y.getName())

            for trv in newTroves:
                troveTup = trv.getName(), trv.getVersion(), trv.getFlavor()
                yield troveTup, trv, indent

        else:
            yield troveTup, trv, 0

            if iterTroves:
                newTroveTups = sorted(trv.iterTroveList())
                if needTroves:
                    newTroves = troveSource.getTroves(newTroveTups, 
                                                      withFiles=needFiles)
                else:
                    newTroves = [None] * len(newTroveTups)

                for troveTup, trv in itertools.izip(newTroveTups, newTroves):
                    yield troveTup, trv, 1



class DisplayConfig:
    """ Configuration for a display command.  Contains both specified
        parameters as well as information about some derived parameters 
        (such as whether or not a particular display command will need
        file lists)
    """

    # NOTE: The way that display configuration options interact is 
    # confusing.  That's at least partially because the semantics for
    # displaying data is confusing.  For example, we display components
    # _if_ a package was selected and it was specified using a version
    # or a flavor (not just the name).  
    # A next step should be to redefine these rules.
        
    def __init__(self, troveSource=None, ls = False, ids = False, 
                 sha1s = False, 
                 fullVersions = False, tags = False, info = False, 
                 deps = False, showBuildReqs = False, showFlavors = False, 
                 iterChildren = False, showComponents = False):
        self.troveSource = troveSource

        self.ls = ls
        self.ids = ids
        self.sha1s = sha1s
        self.fullVersions = fullVersions
        self.fullFlavors = showFlavors
        self.showComponents = showComponents
        self.tags = tags
        self.info = info
        self.deps = deps
        self.showBuildReqs = showBuildReqs
        self.primaryTroves = set()

        self.iterChildren = True in (iterChildren, ls, ids, sha1s, tags) 
        self.iterChildren = self.iterChildren and not (info or showBuildReqs)

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
        return self.info or self.showBuildReqs or self.deps or self.iterTroves() or not self.needFiles()

    def printSimpleHeader(self):
        return not self.info and not self.showBuildReqs
 
    def printDeps(self):
        return self.deps

    def printPathIds(self):
        return self.ids

    def printSha1s(self):
        return self.sha1s

    def printInfo(self):
        return self.info

    def printFiles(self):
        return self.ls or self.ids or self.sha1s or self.tags or self.iterTroves()

    def isVerbose(self):
        return not self.iterFiles()

    def useFullVersions(self):  
        return self.fullVersions

    def useFullFlavors(self):  
        return self.fullFlavors

    def hideComponents(self):
        return (not self.showComponents 
                and not (self.iterTroves() or self.walkTroves()))

    #### Needed Data
    #### What the given display configuration implies that we need

    def needTroves(self):
        # we need the trove 
        return self.info or self.showBuildReqs

    def needFiles(self):
        return self.printFiles()

    def needFileObjects(self):
        return self.needFiles() and self.isVerbose()

    #### Recursion
    #### Whether to recursively descend into child troves, iterate
    #### only over child troves, or just display the listed troves.

    def walkTroves(self):
        return self.ls or self.ids or self.sha1s or self.tags or self.deps

    def iterTroves(self):
        return self.iterChildren and not self.walkTroves()

    def iterFiles(self):
        return self.iterTroves()


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
            @type f: deps.deps.DependencySet (flavor)
            @rtype: (vStr, fStr) where vStr is the version string to display
            for this trove and fStr is the flavor string (may be empty)
        """

        if self.dcfg.useFullVersions():
            vStr = str(v)
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
            flavors = self._fCache.get(n, [])
            if len(flavors) > 1:
                fStr = deps.flavorDifferences(list(flavors))[f]
            else:
                fStr = ''

        return vStr, fStr

    def formatNVF(self, name, version, flavor, indent=False, format=None):
        """ Print a name, version, flavor tuple
        """
        vStr, fStr = self.getTupleStrings(name, version, flavor)

        if format:
            return format % (name, vStr, fStr)
            
        if indent:
            if fStr:
                return _grpFormatWithFlavor % (name, vStr, fStr)
            else:
                return _grpFormat % (name, vStr)
        elif fStr:
            return _troveFormatWithFlavor % (name, vStr, fStr)
        else:
            return _troveFormat % (name, vStr)


class TroveFormatter(TroveTupFormatter):
    """ 
        Formats trove objects (displaying more than just NVF)
    """

    def formatInfo(self, trove):
        """ returns iteratore of format lines about this local trove """
        # TODO: it'd be nice if this were set up to do arbitrary
        # formats...

        n, v, f = trove.getName(), trove.getVersion(), trove.getFlavor()
        dcfg = self.dcfg
        troveSource = dcfg.getTroveSource()

        sourceName = trove.getSourceName()
        sourceTrove = None

        if sourceName:
            try:
                sourceTrove = troveSource.getTrove(sourceName, 
                                v.getSourceVersion(), deps.DependencySet(),
                                withFiles = False)
                # FIXME: all trove sources should return TroveMissing
                # on failed getTrove calls 
            except (errors.TroveMissing, KeyError):
                pass

        elif troveName.endswith(':source'):
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
            yield "Version   :", v
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

    def formatTroveHeader(self, trove, n, v, f, indent):
        """ Print information about this trove """

        dcfg = self.dcfg

        if dcfg.printSimpleHeader():
            yield self.formatNVF(n, v, f, indent)
        if dcfg.printInfo():
            for line in self.formatInfo(trove):
                yield line
        if dcfg.printBuildReqs():
            for buildReq in sorted(trove.getBuildRequirements()):
                yield self.formatNVF(*buildReq)
        elif dcfg.printDeps():
            for line in self.formatDeps(trove):
                yield line

    def formatDeps(self, trove):
        for name, dep in (('Provides', trove.getProvides()),
                              ('Requires', trove.getRequires())):
            yield '  %s:' %name
            if not dep:
                yield '     None'
            else:
                lines = str(dep).split('\n')
                for l in lines:
                    yield '    ' + l
            yield '' 

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
                yield self.formatFile(pathId, path, fileId, version, file)
        else:
            for (pathId, path, fileId, version) in iter:
                yield _fileFormat % (path, version.trailingRevision())

    def formatFile(self, pathId, path, fileId, version, fileObj=None, 
                    prefix=''):
        taglist = ''
        sha1 = ''
        id = ''

        dcfg = self.dcfg
        verbose = dcfg.isVerbose()

        if verbose and isinstance(fileObj, files.SymbolicLink):
            name = "%s -> %s" % (path, fileObj.target())
        else:
            name = path
        if dcfg.tags and fileObj.tags:
            taglist = ' [' + ' '.join(fileObj.tags) + ']' 
        if dcfg.sha1s:
            if hasattr(fileObj, 'contents') and fileObj.contents:
                sha1 = sha1ToString(fileObj.contents.sha1()) + ' '
            else:
                sha1 = ' '*41

        if dcfg.ids and pathId:
            id = md5ToString(pathId) + ' ' + sha1ToString(fileObj.fileId()) + ', '
        if verbose: 
            return "%s%s%s%s    1 %-8s %-8s %s %s %s%s" % \
              (prefix, id, sha1, fileObj.modeString(), fileObj.inode.owner(), 
               fileObj.inode.group(), fileObj.sizeString(), 
               fileObj.timeString(), name, taglist)
        else:
            return "%s%s%s%s" % (id, sha1,path, taglist)



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
    
    else:
        for job, comps in formatter.compressJobList(jobs):
            if dcfg.printTroveHeader():
                for ln in formatter.formatJobHeader(job, comps):
                    print ln

            if dcfg.printFiles():
                for ln in formatter.formatJobFiles(job):
                    print ln


class JobDisplayConfig(DisplayConfig):
    
    def __init__(self, *args, **kw):
        self.showChanges = kw.pop('showChanges', False)
        DisplayConfig.__init__(self, *args, **kw)

    def compressJobs(self):
        """ compress jobs so that updates of components are displayed with 
            the update of their package
        """
        return True

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


class JobTupFormatter(TroveFormatter):

    def __init__(self, dcfg=None):
        if not dcfg:
            dcfg = JobDisplayConfig()
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
            for ln in self.formatInfo(trove):
                yield ln
        elif dcfg.printDeps():
            for ln in self.formatDeps(trove):
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
                        yield self.formatFile(pathId, path, fileId, version, 
                                              fileObj, prefix=prefix)

                    elif modType == troveSource.MOD_F:
                        for ln in self.formatFileChange(pathId, path, fileId, 
                                           version, fileObj, oldPath, oldFileId, 
                                           oldVersion, oldFileObj):
                            yield ln

                    elif modType == troveSource.OLD_F:
                        prefix = ' Del '
                        yield self.formatFile(pathId, oldPath, oldFileId, 
                                              oldVersion, oldFileObj, 
                                              prefix=prefix)

            else:
                for (pathId, path, fileId, version, fileObj, modType) in iter:
                    if modType == troveSource.NEW_F:
                        prefix = ' New '
                    elif modType == troveSource.MOD_F:
                        prefix = ' Mod '
                    elif modType == troveSource.OLD_F:
                        prefix = ' Del '

                    yield self.formatFile(pathId, path, fileId, version, 
                                          fileObj, prefix=prefix)

        else:
            for (pathId, path, fileId, version) in iter:
                yield path

    def formatFileChange(self, pathId, path, fileId, version, 
                         fileObj, oldPath, oldFileId, oldVersion, oldFileObj,
                         indent=''):
        yield self.formatFile(pathId, oldPath, oldFileId, oldVersion, 
                              oldFileObj, prefix=' Mod ')

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
        yield "%s---> %s%s%-10s      %-8s %-8s %8s %11s %s%s" % \
          (indent, space, sha1, mode, owner, group, size, time, name, taglist)
