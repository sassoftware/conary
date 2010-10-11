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
import stat
import tempfile

import conary.errors
from conary.conaryclient.update import UpdateError

from conary import conaryclient
from conary import trovetup
from conary import versions
from conary.lib import log, util

# The schema for a system model is, roughly:
#
# searchPath := list of troveTuples|labels
# systemItems := list of troveOperations
# troveOperations := updateTroves | eraseTroves | installTroves | patchTroves
# updateTroves := list of troveTuples
# eraseTroves := list of troveTuples
# installTroves := list of troveTuples
# patchTroves := list of troveTuples


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

class PatchTroveOperation(TroveOperation):
    key = 'patch'

troveOpMap = {
    UpdateTroveOperation.key  : UpdateTroveOperation,
    EraseTroveOperation.key   : EraseTroveOperation,
    InstallTroveOperation.key : InstallTroveOperation,
    PatchTroveOperation.key   : PatchTroveOperation,
}

class SystemModel:
    # Make the operation objects available via models, avoiding the
    # need to import this module when a model is provided
    SearchTrove = SearchTrove
    SearchLabel = SearchLabel
    UpdateTroveOperation = UpdateTroveOperation
    EraseTroveOperation = EraseTroveOperation
    InstallTroveOperation = InstallTroveOperation
    PatchTroveOperation = PatchTroveOperation

    def __init__(self, cfg):
        self.cfg = cfg
        self.reset()

    def reset(self):
        self.searchPath = []
        self.systemItems = []
        self.indexes = {}
        # Keep track of modifications that do not involve setting
        # an operation as modified
        self.modelModified = False

    def _addIndex(self, item):
        # normally, this list is one item long except for index None
        l = self.indexes.setdefault(item.index, [])
        if item not in l:
            l.append(item)

    def _removeIndex(self, item):
        l = self.indexes.get(item.index, [])
        while item in l:
            l.remove(item)
            self.modelModified = True
        if not l:
            self.indexes.pop(item.index)

    def modified(self):
        return (self.modelModified or
                bool([x for x in self.searchPath + self.systemItems
                      if x.modified]))

    def appendToSearchPath(self, item):
        self.searchPath.append(item)
        self._addIndex(item)

    def appendTroveOp(self, op, deDup=True):
        # First, remove trivially obvious duplication -- more
        # complex duplicates may be removed after building the graph
        if isinstance(op, EraseTroveOperation) and self.systemItems and deDup:
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
        deDup = kwargs.pop('deDup', True)
        op = troveOpMap[key](*args, **kwargs)
        self.appendTroveOp(op, deDup=deDup)
        return op

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
        patch troveSpec+

    C{search} lines take a single troveSpec or label, which B{may} be
    enclosed in single or double quote characters.  Each C{search}
    item B{appends} to the search path.  The C{installLabelPath}
    configuration item is implicitly appended to the specified
    C{searchPath}.

    C{update}, C{erase}, C{install}, and C{patch} lines take
    one or more troveSpecs, which B{may} be enclosed in single
    or double quote characters, unless they contain characters
    that may be specially interpreted by a POSIX shell, in
    which case they B{must} be enclosed in quotes.  Each item
    updated, installed, or patch is C{prepended} to the search
    path used for C{subsequent} items, if it is not found explicitly
    via previous search path items.

    Whole-line comments are retained, and ordering is preserved
    with respect to non-comment lines.

    Partial-line comments are ignored, and not retained when a
    line is modified.
    '''

    def __init__(self, cfg):
        SystemModel.__init__(self, cfg)
        self.reset()

    def reset(self):
        SystemModel.reset(self)
        self.commentLines = []
        self.filedata = []

    def parse(self, fileData=None, fileName='(internal)'):
        self.reset()

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
                    log.warning('%s line %d:'
                        ' "search %s" entry follows operations,'
                        ' though it applies to earlier operations'
                        %(fileName, index, nouns))
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
                    modified=False, index=index,
                    deDup=False)

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

    def format(self):
        return ''.join(self.iterFormat())

    def write(self, f):
        f.write(self.format())


class SystemModelFile(object):
    '''
    Implements file manipulation of a system model file.  This includes
    snapshot files, which are used to store the target state while the
    system is in transition.
    '''

    def __init__(self, model, fileName='/etc/conary/system-model',
            snapshotExt='.next'):
        self.fileName = fileName
        self.snapName = fileName + snapshotExt
        self.root = model.cfg.root
        self.model = model

        self.fileFullName = self.root+fileName
        self.snapFullName = self.fileFullName + snapshotExt

        if self.exists():
            self.parse()

    def snapshotExists(self):
        return util.exists(self.snapFullName)

    def exists(self):
        return util.exists(self.fileFullName)

    def read(self, fileName=None):
        if fileName is None:
            if self.snapshotExists():
                fileName = self.snapFullName
            else:
                fileName = self.fileFullName
        self.model.filedata = open(fileName, 'r').readlines()
        return self.model.filedata, fileName

    def parse(self, fileName=None, fileData=None):
        if fileData is None:
            fileData, _ = self.read(fileName=fileName)
        else:
            fileName = None
            self.model.filedata = fileData
        self.model.parse(fileData=self.model.filedata,
                         fileName=fileName)

    def write(self, fileName=None):
        '''
        Writes the current system model to the specified file (relative
        to the configured root), or overwrites the previously-specified
        file if no filename is provided.
        '''
        if fileName == None:
            fileName = self.fileName
        fileFullName = self.model.cfg.root+fileName
        if util.exists(fileFullName):
            fileMode = stat.S_IMODE(os.stat(fileFullName)[stat.ST_MODE])
        else:
            fileMode = 0644

        dirName = os.path.dirname(fileFullName)
        fd, tmpName = tempfile.mkstemp(prefix='system-model', dir=dirName)
        f = os.fdopen(fd, 'w')
        self.model.write(f)
        os.chmod(tmpName, fileMode)
        os.rename(tmpName, fileFullName)

    def writeSnapshot(self):
        '''
        Write the current state of the model to the snapshot file
        '''
        self.write(fileName=self.snapName)

    def closeSnapshot(self):
        '''
        Indicate that a model has been fully applied to the system by
        renaming the snapshot, if it exists, over the previous model file.
        '''
        if self.snapshotExists():
            os.rename(self.snapFullName, self.fileFullName)

    def deleteSnapshot(self):
        '''
        Remove any snapshot without applying it to a system; normally
        as part of rolling back a partially-applied update.
        '''
        if self.snapshotExists():
            os.unlink(self.snapFullName)
