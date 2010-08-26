#
# Copyright (c) 2010 rPath, Inc.
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
Implements the abstract system model, including the canonical
file representation of the model.  This system model is written
explicitly in terms of labels and versions, and is interpreted
relative to system configuration items such as installLabelPath,
flavor, pinTroves, excludeTroves, and so forth.

If an installLabelPath is provided in the configuration, it is
implicitly added to the end of the search path.
"""

import os
import shlex
import tempfile

import conary.errors
from conary.conaryclient.update import UpdateError

from conary import conaryclient
from conary import trovetup
from conary import versions
from conary.lib import log, util
from conary.repository import searchsource

# The schema for a system model is, roughly:
#
# searchPath := list of troveTuples|labels
# systemItems := list of troveOperations
# troveOperations := updateTroves | eraseTroves | installTroves | replaceTroves
# updateTroves := list of troveTuples
# eraseTroves := list of troveTuples
# installTroves := list of troveTuples
# replaceTroves := list of troveTuples


def shellStr(s):
    if len(shlex.split(s)) > 1:
        return "'%s'" % s
    return s


class SystemModelError(UpdateError):
    pass


class _SystemModelItem:
    def __init__(self, text=None, item=None, modified=True, index=None):
        self.modified = modified
        self.index = index
        assert(text is not None or item is not None)
        assert(not(text is None and item is None))
        if item:
            self.item = item
        else:
            self.parse(text=text)

    def update(self, item, modified=True):
        self.parse(item)
        self.modified = modified

    def parse(self, text=None):
        raise NotImplementedError

    def __str__(self):
        return self.key + ' ' + self.asString()

    def __repr__(self):
        return "%s(text='%s', modified=%s, index=%s)" % (
            str(self.__class__).split('.')[-1],
            self.asString(), self.modified, self.index)

    def __eq__(self, other):
        # index and modified explicitly not compared, because this is
        # used to compare new items to previously-existing items
        return self.item == other.item

class SearchElement(_SystemModelItem):
    key = 'search'

class SearchTrove(SearchElement):
    def parse(self, text):
        self.item = trovetup.TroveSpec(text)

    def asString(self):
        return shellStr(str(self.item))

class SearchLabel(SearchElement):
    def parse(self, text):
        self.item = versions.Label(text)

    def asString(self):
        return shellStr(str(self.item))

class TroveOperation(_SystemModelItem):
    def parse(self, text):
        if isinstance(text, str):
            text = [text]
        self.item = [trovetup.TroveSpec(x) for x in text]

    def __repr__(self):
        return "%s(text=['%s'], modified=%s, index=%s)" % (
            str(self.__class__).split('.')[-1],
            "', '".join(str(x) for x in self.item),
            self.modified, self.index)

    def __iter__(self):
        return iter(self.item)

    def asString(self):
        return ' '.join(shellStr(str(x)) for x in self.item)

class UpdateTroveOperation(TroveOperation):
    key = 'update'

class EraseTroveOperation(TroveOperation):
    key = 'erase'

class InstallTroveOperation(TroveOperation):
    key = 'install'

class ReplaceTroveOperation(TroveOperation):
    key = 'replace'

troveOpMap = {
    UpdateTroveOperation.key  : UpdateTroveOperation,
    EraseTroveOperation.key   : EraseTroveOperation,
    InstallTroveOperation.key : InstallTroveOperation,
    ReplaceTroveOperation.key : ReplaceTroveOperation,
}

class SystemModel:
    # Make the operation objects available via models, avoiding the
    # need to import this module when a model is provided
    SearchTrove = SearchTrove
    SearchLabel = SearchLabel
    UpdateTroveOperation = UpdateTroveOperation
    EraseTroveOperation = EraseTroveOperation
    InstallTroveOperation = InstallTroveOperation
    ReplaceTroveOperation = ReplaceTroveOperation

    def __init__(self, cfg):
        self.searchPath = []
        self.systemItems = []
        self.indexes = {}
        self.cfg = cfg

    def _addIndex(self, item):
        # normally, this list is one item long except for index None
        l = self.indexes.setdefault(item.index, [])
        if item not in l:
            l.append(item)

    def _removeIndex(self, item):
        l = self.indexes.get(item.index, [])
        while item in l:
            l.remove(item)
        if not l:
            self.indexes.pop(item.index)

    def modified(self):
        return bool([x for x in self.searchPath + self.systemItems
                     if x.modified])

    def appendToSearchPath(self, item):
        self.searchPath.append(item)
        self._addIndex(item)

    def appendTroveOp(self, op):
        # First, remove trivially obvious duplication -- more
        # complex duplicates may be removed after building the graph
        if isinstance(op, EraseTroveOperation) and self.systemItems:
            otherOp = self.systemItems[-1]
            if op == otherOp:
                if isinstance(otherOp, (UpdateTroveOperation,
                                        InstallTroveOperation)):
                    # erasing exactly the immediately-previous
                    # update or install item should remove that
                    # immediately-previous item, rather than add
                    # an explicit "erase" trove operation to the list
                    self.systemItems.pop()
                    self._removeIndex(op)
                    return
                elif (isinstance(otherOp, EraseTroveOperation)):
                    # do not add identical adjacent erase operations
                    return

        self.systemItems.append(op)
        self._addIndex(op)

    def removeTroveOp(self, op):
        self._removeIndex(op)
        while op in self.systemItems:
            self.systemItems.remove(op)

    def appendTroveOpByName(self, key, *args, **kwargs):
        op = troveOpMap[key](*args, **kwargs)
        self.appendTroveOp(op)

    def refreshSearchPath(self):
        cfg = self.cfg
        cclient = conaryclient.ConaryClient(cfg)
        repos = cclient.getRepos()

        # Find SearchTroves with any version specified, and remove any 
        # trailingRevision
        # {oldTroveKey: index}
        searchItemsOld = dict((y.item, x) for x, y in enumerate(self.searchPath)
                           if isinstance(y, SearchTrove)
                              and y.item[1] is not None)
        # {newTroveKey: oldTroveKey}
        searchItemsNew = dict(((x[0], x[1].rsplit('/', 1)[0], x[2]), x)
                              for x in searchItemsOld.keys())
        searchTroves = searchItemsOld.keys() + searchItemsNew.keys()

        foundTroves = repos.findTroves(cfg.installLabelPath, 
            searchTroves, defaultFlavor = cfg.flavor)

        for troveKey in foundTroves.keys():
            if troveKey in searchItemsNew:
                oldTroveKey = searchItemsNew[troveKey]
                if foundTroves[troveKey] != foundTroves[oldTroveKey]:
                    # found a new version, replace
                    foundTrove = foundTroves[troveKey][0]
                    newVersion = foundTrove[1]
                    newverstr = '%s/%s' %(newVersion.trailingLabel(),
                                          newVersion.trailingRevision())
                    item = (oldTroveKey[0], newverstr, oldTroveKey[2])
                    index = searchItemsOld[oldTroveKey]
                    self.searchPath[index].update(item)


class SystemModelText(SystemModel):
    '''
    Implements the abstract system model persisting in a text format,
    which is intended to be human-readable and human-editable.

    The format is::
        search troveSpec|label
        update troveSpec+
        erase troveSpec+
        install troveSpec+
        replace troveSpec+

    C{search} lines take a single troveSpec or label, which B{may} be
    enclosed in single or double quote characters.  Each C{search}
    item B{appends} to the search path.  The C{installLabelPath}}
    configuration item is implicitly appended to the specified
    C{searchPath}.

    C{update}, C{erase}, C{install}, and C{replace} lines take
    one or more troveSpecs, which B{may} be enclosed in single
    or double quote characters, unless they contain characters
    that may be specially interpreted by a POSIX shell, in
    which case they B{must} be enclosed in quotes.  Each item
    updated, installed, or replaced is C{prepended} to the search
    path used for C{subsequent} items, if it is not found explicitly
    via previous search path items.

    Whole-line comments are retained, and ordering is preserved
    with respect to non-comment lines.

    Partial-line comments are ignored, and not retained when a
    line is modified.
    '''

    def __init__(self, cfg):
        SystemModel.__init__(self, cfg)
        self.commentLines = []
        self.filedata = []

    def parse(self, fileData=None, fileName='(internal)'):
        if fileData is not None:
            self.filedata = fileData

        for index, line in enumerate(self.filedata):
            line = line.strip()

            if line.startswith('#') or not line:
                # empty lines are handled just like comments, and empty
                # lines and comments are always looked up in the
                # unmodified filedata, so we store only the index
                self.commentLines.append(index)
                continue

            # non-empty, non-comment lines must be parsed 
            try:
                verb, nouns = line.split(None, 1)
            except:
                raise SystemModelError('%s: Invalid statement on line %d' %(
                                       fileName, index))

            if verb == 'search':
                if self.systemItems:
                    # If users provide a "search" line after a trove
                    # operation, they may expect it to be evaluated
                    # later.  Warn them that this is not actually
                    # going to happen.  (When adding "include", then
                    # this warning should apply only to the outmost
                    # file, not to included files.)
                    log.syslog('warning: %s line %d:'
                        ' "search" entry follows operations:'
                        ' but will still be honored for prior'
                        ' operations' %fileName, index)
                # Handle it if quoted, but it doesn't need to be
                nouns = ' '.join(shlex.split(nouns, comments=True))
                try:
                    searchItem = SearchLabel(text=nouns,
                                             modified=False, index=index)
                except conary.errors.ParseError:
                    searchItem = SearchTrove(text=nouns,
                                             modified=False, index=index)
                self.appendToSearchPath(searchItem)

            elif verb in troveOpMap:
                self.appendTroveOpByName(verb,
                    text=shlex.split(nouns, comments=True),
                    modified=False, index=index)

            else:
                raise SystemModelError(
                    '%s: Unrecognized command "%s" on line %d' %(
                    fileName, verb, index))

    def iterFormat(self):
        '''
        Serialize the current model, including preserved comments.
        '''
        commentLines = list(self.commentLines) # copy

        lastSearchLine = max([x.index for x in self.searchPath] + [0])
        lastOpLine = max([x.index for x in self.systemItems] + [0])
        newSearchItems = [x for x in self.searchPath if x.index is None]
        newOperations = [x for x in self.systemItems if x.index is None]
        lastIndexLine = max(lastSearchLine, lastOpLine, max(commentLines + [0]))

        for i in range(lastIndexLine+1):
            # First, emit all prior comments in order
            while commentLines and commentLines[0] <= i:
                yield self.filedata[commentLines.pop(0)]

            if i in self.indexes:
                # Next, emit all the specified lines
                for item in self.indexes[i]:
                    # normally, this list is one item long
                    if item.modified:
                        yield str(item) + '\n'
                    else:
                        yield self.filedata[i]
                        # handle models lacking trailing newlines
                        if self.filedata[i] and self.filedata[i][-1] != '\n':
                            yield '\n'

            # Last, emit any remaining lines
            if i == lastSearchLine:
                for item in (x for x in self.searchPath
                             if x.index is None):
                    yield str(item) + '\n'
            if i == lastOpLine:
                for item in (x for x in self.systemItems
                             if x.index is None):
                    yield str(item) + '\n'

        # Finally, emit any trailing comments
        while commentLines:
            yield self.filedata[commentLines.pop(0)]

    def format(self):
        return ''.join(self.iterFormat())

#class SystemModelXML(SystemModel):
#    '''
#    Implements the abstract system model persisting in an XML format,
#    which is intended to be machine-readable and machine-editable.
#    ...
#    '''

class SystemModelFile(object):
    def __init__(self, model, fileName='/etc/conary/system-model'):
        self.fileName = fileName
        self.root = model.cfg.root
        self.model = model

        self.fileFullName = self.root+fileName
        if self.exists():
            self.parse()

    def exists(self):
        return util.exists(self.fileFullName)

    def read(self):
        self.model.filedata = open(self.fileFullName, 'r').readlines()
        return self.model.filedata

    def parse(self, fileData=None):
        if fileData is None:
            self.read()
        else:
            self.model.filedata = fileData
        self.model.parse(fileData=self.model.filedata,
                         fileName=self.fileFullName)

    def write(self, fileName=None):
        '''
        Writes the current system model to the specified file (relative
        to the configured root), or overwrites the previously-specified
        file if no filename is provided.
        '''
        if fileName == None:
            fileName = self.fileName
        fileFullName = self.model.cfg.root+fileName

        dirName = os.path.dirname(fileFullName)
        fd, tmpName = tempfile.mkstemp(prefix='system-model', dir=dirName)
        f = os.fdopen(fd, 'w')
        f.write(self.model.format())
        os.rename(tmpName, fileFullName)
