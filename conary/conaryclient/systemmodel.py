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
relative to system configuration items such as flavor, pinTroves,
excludeTroves, and so forth.
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
# searchItem := troveTuples or label
# systemItem := searchItem or list of troveOperations
# troveOperations := updateTroves | eraseTroves | installTroves | patchTroves
#                    | searchItem
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
        if item is not None:
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

class SearchOperation(_SystemModelItem):
    key = 'search'

    def asString(self):
        return shellStr(str(self.item))

class SearchTrove(SearchOperation):
    def parse(self, text):
        self.item = trovetup.TroveSpec(text)

class SearchLabel(SearchOperation):
    def parse(self, text):
        self.item = versions.Label(text)

class _TextItem(_SystemModelItem):
    def parse(self, text):
        self.item = text

    def asString(self):
        return self.item

    def __repr__(self):
        return "%s(text='%s', modified=%s, index=%s)" % (
            str(self.__class__).split('.')[-1],
            self.item, self.modified, self.index)

class NoOperation(_TextItem):
    'Represents comments and blank lines'
    __str__ = _TextItem.asString

class VersionOperation(_TextItem):
    key = 'version'

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
    SearchOperation = SearchOperation
    NoOperation = NoOperation
    UpdateTroveOperation = UpdateTroveOperation
    EraseTroveOperation = EraseTroveOperation
    InstallTroveOperation = InstallTroveOperation
    PatchTroveOperation = PatchTroveOperation
    VersionOperation = VersionOperation

    def __init__(self, cfg):
        self.cfg = cfg
        self.reset()

    def reset(self):
        self.systemItems = []
        self.noOps = []
        self.indexes = {}
        self.version = None
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
                bool([x for x in self.systemItems + self.noOps
                      if x.modified]))

    def setVersion(self, item):
        self.version = item
        self._addIndex(item)

    def getVersion(self):
        if self.version is None:
            return self.version
        return self.version.asString()

    def appendNoOperation(self, item):
        self.noOps.append(item)
        self._addIndex(item)

    def appendNoOpByText(self, text, **kwargs):
        self.appendNoOperation(NoOperation(text, **kwargs))

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
        searchItemsOld = dict((y.item, x)
                              for x, y in enumerate(self.systemItems)
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
                    self.systemItems[index].update(item)


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
    enclosed in single or double quote characters.  Each of these
    lines represents a place to search for troves to install on
    or make available to the system.

    C{update}, C{erase}, C{install}, and C{patch} lines take
    one or more troveSpecs, which B{may} be enclosed in single
    or double quote characters, unless they contain characters
    that may be specially interpreted by a POSIX shell, in
    which case they B{must} be enclosed in quotes.  Each of
    these lines represents a modification of the set of troves
    to be installed or available on the system after the model
    has been executed.

    The lines are processed in order, except that adjacent lines
    that can be executed at the same time are executed in parallel.
    Each line makes some change to the model, and the most recent
    change wins.  When looking up troves for trove operations (but
    not for C{search} lines), they are sought first in the troves
    that have already been added to the install or optional set
    by previous lines; if they are not found there, they are sought
    in the search path as created by C{search} lines, looking first
    in the most recent previous C{search} line and working back to
    the first C{search} line.

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
                self.appendNoOpByText(line, modified=False, index=index)
                continue

            # non-empty, non-comment lines must be parsed 
            try:
                verb, nouns = line.split(None, 1)
            except:
                raise SystemModelError('%s: Invalid statement on line %d' %(
                                       fileName, index))

            if verb == 'version':
                self.setVersion(
                    VersionOperation(text=line, modified=False, index=index))

            elif verb == 'search':
                # Handle it if quoted, but it doesn't need to be
                nouns = ' '.join(shlex.split(nouns, comments=True))
                try:
                    searchItem = SearchLabel(text=nouns,
                                             modified=False, index=index)
                except conary.errors.ParseError:
                    searchItem = SearchTrove(text=nouns,
                                             modified=False, index=index)
                self.appendTroveOp(searchItem)

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
        lastNoOpLine = max([x.index for x in self.noOps] + [0])
        lastOpLine = max([x.index for x in self.systemItems] + [0])
        # can only be one version
        if self.version is not None:
            verLine = self.version.index
        else:
            verLine = 0
        lastIndexLine = max(lastOpLine, lastNoOpLine, verLine)

        # First, emit all comments without an index as "header"
        for item in (x for x in self.noOps if x.index is None):
            yield str(item)

        # Now, emit the version if it is new (has no index)
        if self.version is not None and self.version.index is None:
            yield str(self.version)

        for i in range(lastIndexLine+1):
            if i in self.indexes:
                # Emit all the specified lines
                for item in self.indexes[i]:
                    # normally, this list is one item long
                    if item.modified:
                        yield str(item)
                    else:
                        yield self.filedata[i].rstrip('\n')

            # Last, emit any remaining lines
            if i == lastOpLine:
                for item in (x for x in self.systemItems if x.index is None):
                    yield str(item)

    def format(self):
        return '\n'.join([x for x in self.iterFormat()] + [''])

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
