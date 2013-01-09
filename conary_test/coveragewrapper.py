#!/usr/bin/python
#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import imp
import os
import sys
import re
import subprocess

conaryPath = os.environ.get('CONARY_PATH', None)
if not conaryPath:
    print >>sys.stderr, 'please set CONARY_PATH'
    sys.exit(1)

if conaryPath not in sys.path:
    sys.path.insert(0, conaryPath)

from conary.lib import util

class CoverageWrapper(object):
    def __init__(self, coveragePath, dataPath, annotatePath, baseDirs):
        self._coveragePath = coveragePath
        self._executable = coveragePath + '/coverage.py'
        self._dataPath = dataPath
        self._annotatePath = annotatePath
        self._baseDirs = baseDirs
        os.environ['COVERAGE_DIR'] = dataPath # yuck - this is needed by
        # coverage.py to determine the cache directory when gathering the
        # results
        util.mkdirChain(dataPath)
        self.environ = dict(COVERAGE_DIR = dataPath, PATH = '/bin:/usr/bin',
                            COVERAGE_PATH = coveragePath)
        if 'CONARY_PATH' in os.environ:
            self.environ['CONARY_PATH'] = os.environ['CONARY_PATH']
        self.environ['CONARY_POLICY_PATH'] = os.environ.get('CONARY_POLICY_PATH', '/usr/lib/conary/policy')
        self.coverage = None

    def reset(self):
        if os.path.exists(self._dataPath):
            util.rmtree(self._dataPath)
        if os.path.exists(self._annotatePath):
            util.rmtree(self._annotatePath)

    def execute(self, argv):
        interp = "/usr/bin/python"
        if isinstance(argv, str):
            cmd = '%s %s' % (interp, argv)
        else:
            cmd = [interp] + argv
        retval = subprocess.call(cmd, env=os.environ)
        return retval

    def getCoverage(self):
        if self.coverage:
            return self.coverage
        coverage = imp.load_source('coverage', self._executable)
        coverage = coverage.the_coverage
        coverage.cacheDir = self._dataPath
        coverage.get_ready(restoreDir=True)
        return coverage

    def compress(self):
        coverage = self.getCoverage()
        if os.path.exists(self._dataPath):
            util.rmtree(self._dataPath)
        util.mkdirChain(self._dataPath)
        coverage.save()

    def displayReport(self, files, displayMissingLines=False):
        assert(not displayMissingLines)
        missingSortKey = lambda x: (len(x[2]), len(x[1]))
        sortFn = lambda a,b: cmp(missingSortKey(a), missingSortKey(b))
        coverage = self.getCoverage()
        coverage.report(files, show_missing=False, sortFn=sortFn, 
                        baseDirs=self._baseDirs)

    def writeAnnotatedFiles(self, files):
        if os.path.exists(self._annotatePath):
            util.rmtree(self._annotatePath)
        annotatePath = self._annotatePath
        coverage = self.getCoverage()
        coverage.annotate(files, self._annotatePath, baseDirs = self._baseDirs)
        if annotatePath.startswith(os.getcwd() + '/'):
            annotatePath = '.' + annotatePath[len(os.getcwd()):]
        print
        print '*** %s file(s) annotated in %s' % (len(files), annotatePath)

    def iterAnalysis(self, paths):
        coverage = imp.load_source('coverage', self._executable)
        coverage = coverage.the_coverage
        coverage.cache = self._dataPath
        coverage.restore()
        for path in paths:
            _, statements, excluded, missing, missingFmted = coverage.analysis2(path)
            total = len(statements)
            covered = total - len(missing)
            if covered < total:
                percentCovered = 100.0 * covered / total
            else:
                percentCovered = 100.0
            yield path, total, covered, percentCovered, missingFmted

    def getTotals(self, paths):
        totalLines = 0
        totalCovered = 0
        for _, pathLines, coveredLines, _, _ in self.iterAnalysis(paths):
            totalLines += pathLines
            totalCovered += coveredLines

        if totalCovered != totalLines:
            percentCovered = 1.0 - 100.0 * totalCovered / totalLines
        else:
            percentCovered = 100.0
        return totalLines, totalCovered, percentCovered

def getEnviron():
    conaryPath = os.environ.get('CONARY_PATH', None)
    coveragePath = os.environ.get('COVERAGE_PATH', None)
    policyPath = os.environ.get('CONARY_POLICY_PATH', '/usr/lib/conary/policy')
    if not coveragePath:
        print "Please set COVERAGE_PATH to the dir in which coverage.py is located"
        sys.exit(1)
    elif not policyPath:
        print "Please set CONARY_POLICY_PATH"
        sys.exit(1)
    return {'conary'   : conaryPath,
            'policy'   : policyPath,
            'coverage' : coveragePath}

def _isPythonFile(fullPath, posFilters=[r'\.py$'], negFilters=['sqlite']):
    found = False
    for posFilter in posFilters:
        if re.search(posFilter, fullPath):
            found = True
            break
    if not found:
        return False

    foundNeg = False
    for negFilter in negFilters:
        if re.search(negFilter, fullPath):
            foundNeg = True
            break
    if foundNeg:
        return False
    return True

def getFilesToAnnotate(baseDirs=[], filesToFind=[], exclude=[]):

    notExists = set(filesToFind)
    addAll = not filesToFind

    allFiles = set(x for x in filesToFind if os.path.exists(x))
    filesToFind = [ x for x in filesToFind if x not in allFiles ]

    if not isinstance(exclude, list):
        exclude = [ exclude ]
    negFilters = ['sqlite'] + exclude

    baseDirs = [ os.path.realpath(x) for x in baseDirs ]
    for baseDir in baseDirs:
        for root, dirs, pathList in os.walk(baseDir):
            for path in filesToFind:
                if os.path.exists(os.path.join(root, path)):
                    allFiles.add(os.path.join(root, path))
                    notExists.discard(path)

            if addAll:
                for path in pathList:
                    fullPath = '/'.join((root, path))
                    if _isPythonFile(fullPath, negFilters=negFilters):
                        allFiles.add(fullPath)
    return list(allFiles), notExists


def getFilesToAnnotateByPatch(baseDir, exclude, patchInput, filesDict=None):
    if filesDict is None:
        files = {}
    else:
        files = filesDict
    cwd = os.getcwd()
    try:
        os.chdir(baseDir)
        patchReaderPath = os.environ['COVERAGE_PATH'] + '/patchreader'
        stdin, output = os.popen2('%s -p' % (patchReaderPath,), 'w')
        stdin.write(patchInput.read())
        stdin.flush()
        stdin.close()
    finally:
        os.chdir(cwd)
    while True:
        fileName = output.readline().strip()
        if not fileName:
            break
        lines = [ int(x) for x in output.readline().split()]
        files[fileName] = lines
    for fileName in files.keys():
        for excludePattern in exclude:
            if re.search(excludePattern, fileName):
                del files[fileName]

def getFilesToAnnotateFromPatchFile(path, baseDirs=[], exclude=[]):
    return _getFilesToAnnotateFromFn(baseDirs, exclude, open, path)

def getFilesToAnnotateFromHg(baseDirs=[], exclude=[]):
    return _getFilesToAnnotateFromFn(baseDirs, exclude, os.popen, "hg diff")

def getFilesToAnnotateFromHgOut(baseDirs=[], exclude=[]):
    return _getFilesToAnnotateFromFn(baseDirs, exclude, os.popen, "hg diff -r $(hg parents -r $(hg outgoing | awk '/changeset/ { print $2}' | head -1  | cut -d: -f1)  | awk '/changeset/ { print $2}' | cut -d: -f1)")

def _getFilesToAnnotateFromFn(baseDirs, exclude, fn, *args, **kw):
    files = {}
    curDir = os.getcwd()
    try:
        for baseDir in baseDirs:
            if not os.path.isdir(os.path.join(baseDir, '.hg')) and not os.path.isdir(os.path.join(baseDir, '../.hg')):
                # This is not an hg repo (for instance, if policy is not run
                # from mercurial)
                continue
            os.chdir(baseDir)
            output = fn(*args, **kw)
            getFilesToAnnotateByPatch(baseDir, exclude, output, files)
    finally:
        os.chdir(curDir)

    for fullPath in files.keys():
        if not _isPythonFile(fullPath):
            del files[fullPath]

    return files, []
