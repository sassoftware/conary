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
Implements the abstract Conary Model, as well as the Conary Model
Language (CML) serialization of the abstract model.  This conary
model is written explicitly in terms of labels and versions, and is
interpreted relative to system configuration items such as flavor,
pinTroves, excludeTroves, and so forth.
"""

import shlex
import weakref

from conary.conaryclient.update import UpdateError
from conary import conaryclient
from conary import errors
from conary import trovetup
from conary import versions

from conary.lib.compat import namedtuple as _namedtuple

# The schema for a system model is, roughly:
#
# searchOp := troveTuples or label
# systemOp := searchOp or list of troveOperations
# troveOperations := updateTroves | eraseTroves | installTroves | patchTroves
#                    | offerTroves | searchOp
# updateTroves := list of troveTuples
# eraseTroves := list of troveTuples
# installTroves := list of troveTuples
# patchTroves := list of troveTuples
# offerTroves := list of troveTuples


# There are four kinds of string formatting used in these objects:
# * __str__() is the most minimal representation of the contents as
#   a python string
# * __repr__() is used only for good representation in debugging contexts
# * asString() is the string representation as it will be consumed,
#   with shlex if appropriate for that object type
# * format() (defined for types that represent file contents) has the
#   CML file representation, including type/key


def shellStr(s):
    if len(shlex.split(s)) > 1:
        return "'%s'" % s
    return s


class CMError(UpdateError):
    pass


class CMLocation(_namedtuple('CMLocation', 'line context op')):
    """
    line: line number (should be 1-indexed)
    context: file name or other similar context, or C{None}
    op: weakref to containing operation, or C{None}
    """
    def __repr__(self):
        return "%s(line=%r, context=%r)" % (
            self.__class__.__name__, self.line, self.context)

    def __str__(self):
        if self.context is not None:
            return ':'.join((str(self.context), str(self.line)))
        return str(self.line)
    asString = __str__


class CMTroveSpec(trovetup.TroveSpec):
    '''
    Exactly like L{trovetup.TroveSpec} except that it also implements
    an optional C{location} which defaults to C{None}; if C{location}
    is not C{None}, then C{__str__} prepends the location.
    '''
    def __new__(cls, name, version=None, flavor=None, location=None, **kwargs):
        name, version, flavor = trovetup.TroveSpec(
            name, version, flavor, **kwargs)
        return tuple.__new__(cls, (name, version, flavor))

    def __init__(self, *args, **kwargs):
        if len(args) == 4:
            self.location = args[3]
        elif 'location' in kwargs:
            self.location = kwargs['location']
        else:
            self.location = None

    def __str__(self):
        if self.location is not None:
            return ': '.join((str(self.location), self.asString()))
        return self.asString()

    format = trovetup.TroveSpec.asString

    def __eq__(self, other):
        # location is not considered, and need to use indices so
        # that we can compare to pure tuples, as well as to
        # trovetup.TroveSpec and to CMTroveSpec
        return self[0:3] == other[0:3]

    # CMTroveSpec objects are pickled into the model cache, but there
    # only the TroveSpec parts are used, so we just leave out the location
    def __getnewargs__(self):
        return (self.name, self.version, self.flavor)
    def __getstate__(self):
        return None
    def __setstate__(self, state):
        pass


class _CMOperation(object):
    def __init__(self, text=None, item=None, modified=True,
                 index=None, context=None):
        self.modified = modified
        self.index = index
        if index is not None:
            self.location = CMLocation(line=index, context=context,
                                       op=weakref.ref(self))
        else:
            self.location = None
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

    def format(self):
        return self.key + ' ' + self.asString()

    def __str__(self):
        return str(self.item)

    def __repr__(self):
        return "%s(text='%s', modified=%s, index=%s)" % (
            self.__class__.__name__,
            self.asString(), self.modified, self.index)

    def __eq__(self, other):
        # index and modified explicitly not compared, because this is
        # used to compare new items to previously-existing items
        return self.item == other.item

class SearchOperation(_CMOperation):
    key = 'search'

    def asString(self):
        return shellStr(self.item.asString())

class SearchTrove(SearchOperation):
    def parse(self, text):
        self.item = CMTroveSpec(text, location=self.location)

class SearchLabel(SearchOperation):
    def parse(self, text):
        self.item = versions.Label(text)


class _TextOp(_CMOperation):
    def parse(self, text):
        self.item = text

    def __str__(self):
        return self.item
    asString = __str__

    def __repr__(self):
        return "%s(text='%s', modified=%s, index=%s)" % (
            self.__class__.__name__, self.item, self.modified, self.index)

class NoOperation(_TextOp):
    'Represents comments and blank lines'
    format = _TextOp.__str__

class VersionOperation(_TextOp):
    '''
    Version string for this model.  This is not a schema version;
    it is a version identifier for the contents of the model.
    This must be a legal conary upstream version, because it is
    used to provide the conary upstream version when building the
    model into a group.
    '''
    key = 'version'
    def parse(self, text):
        # ensure that this is a legal conary upstream version
        rev = versions.Revision(text + '-1')
        if rev.buildCount != None:
            raise errors.ParseError('%s: not a conary upstream version' % text)
        _TextOp.parse(self, text)


class TroveOperation(_CMOperation):
    def parse(self, text):
        if isinstance(text, str):
            text = [text]
        self.item = [CMTroveSpec(x, location=self.location) for x in text]

    def __repr__(self):
        return "%s(text=%s, modified=%s, index=%s)" % (
            self.__class__.__name__,
            str([x.asString() for x in self.item]),
            self.modified, self.index)

    def __str__(self):
        return ' '.join(x.asString() for x in self.item)

    def __iter__(self):
        return iter(self.item)

    def asString(self):
        return ' '.join(shellStr(x.asString()) for x in self.item)

class UpdateTroveOperation(TroveOperation):
    key = 'update'

class EraseTroveOperation(TroveOperation):
    key = 'erase'

class InstallTroveOperation(TroveOperation):
    key = 'install'

class OfferTroveOperation(TroveOperation):
    key = 'offer'

class PatchTroveOperation(TroveOperation):
    key = 'patch'

troveOpMap = {
    UpdateTroveOperation.key  : UpdateTroveOperation,
    EraseTroveOperation.key   : EraseTroveOperation,
    InstallTroveOperation.key : InstallTroveOperation,
    OfferTroveOperation.key   : OfferTroveOperation,
    PatchTroveOperation.key   : PatchTroveOperation,
}

class CM:
    # Make the operation objects available via models, avoiding the
    # need to import this module when a model is provided
    SearchTrove = SearchTrove
    SearchLabel = SearchLabel
    SearchOperation = SearchOperation
    NoOperation = NoOperation
    UpdateTroveOperation = UpdateTroveOperation
    EraseTroveOperation = EraseTroveOperation
    InstallTroveOperation = InstallTroveOperation
    OfferTroveOperation = OfferTroveOperation
    PatchTroveOperation = PatchTroveOperation
    VersionOperation = VersionOperation

    def __init__(self, cfg, context=None):
        '''
        @type cfg: L{conarycfg.ConaryConfiguration}
        @param context: optional description of source of data (e.g. filename)
        @type context: string
        '''
        self.cfg = cfg
        self.context = context
        self.reset()

    def reset(self):
        self.modelOps = []
        self.noOps = []
        self.indexes = {}
        self.version = None
        # Keep track of modifications that do not involve setting
        # an operation as modified
        self.modelModified = False

    def _addIndex(self, op):
        # normally, this list is one item long except for index None
        l = self.indexes.setdefault(op.index, [])
        if op not in l:
            l.append(op)

    def _removeIndex(self, op):
        l = self.indexes.get(op.index, [])
        while op in l:
            l.remove(op)
            self.modelModified = True
        if not l:
            self.indexes.pop(op.index)

    def modified(self):
        return (self.modelModified or
                bool([x for x in self.modelOps + self.noOps
                      if x.modified]))

    def setVersion(self, op):
        self.version = op
        self._addIndex(op)

    def getVersion(self):
        return self.version

    def appendNoOperation(self, op):
        self.noOps.append(op)
        self._addIndex(op)

    def appendNoOpByText(self, text, **kwargs):
        self.appendNoOperation(NoOperation(text, **kwargs))

    def appendOp(self, op, deDup=True):
        # First, remove trivially obvious duplication -- more
        # complex duplicates may be removed after building the graph
        if isinstance(op, EraseTroveOperation) and self.modelOps and deDup:
            otherOp = self.modelOps[-1]
            if op == otherOp:
                if isinstance(otherOp, (UpdateTroveOperation,
                                        InstallTroveOperation)):
                    # erasing exactly the immediately-previous
                    # update or install item should remove that
                    # immediately-previous item, rather than add
                    # an explicit "erase" trove operation to the list
                    self.modelOps.pop()
                    self._removeIndex(otherOp)
                    return
                elif (isinstance(otherOp, EraseTroveOperation)):
                    # do not add identical adjacent erase operations
                    return

        self.modelOps.append(op)
        self._addIndex(op)

    def removeOp(self, op):
        self._removeIndex(op)
        while op in self.modelOps:
            self.modelOps.remove(op)

    def appendTroveOpByName(self, key, *args, **kwargs):
        deDup = kwargs.pop('deDup', True)
        op = troveOpMap[key](*args, **kwargs)
        self.appendOp(op, deDup=deDup)
        return op

    def refreshSearchPath(self):
        cfg = self.cfg
        cclient = conaryclient.ConaryClient(cfg)
        repos = cclient.getRepos()

        # Find SearchTroves with any version specified, and remove any 
        # trailingRevision
        # {oldTroveKey: index}
        searchOpsOld = dict((y.item[0:3], x)
                            for x, y in enumerate(self.modelOps)
                            if isinstance(y, SearchTrove)
                            and y.item[1] is not None)
        # {newTroveKey: oldTroveKey}
        searchOpsNew = dict(((x[0], x[1].rsplit('/', 1)[0], x[2]), x)
                            for x in searchOpsOld.keys())
        searchTroves = searchOpsOld.keys() + searchOpsNew.keys()

        foundTroves = repos.findTroves(cfg.installLabelPath, 
            searchTroves, defaultFlavor = cfg.flavor)

        for troveKey in foundTroves.keys():
            if troveKey in searchOpsNew:
                oldTroveKey = searchOpsNew[troveKey]
                if foundTroves[troveKey] != foundTroves[oldTroveKey]:
                    # found a new version, replace
                    foundTrove = foundTroves[troveKey][0]
                    newVersion = foundTrove[1]
                    newverstr = '%s/%s' %(newVersion.trailingLabel(),
                                          newVersion.trailingRevision())
                    item = (oldTroveKey[0], newverstr, oldTroveKey[2])
                    index = searchOpsOld[oldTroveKey]
                    self.modelOps[index].update(item)


class CML(CM):
    '''
    Implements the abstract system model persisting in a text format,
    called CML, which is intended to be human-readable and human-editable.

    The format is::
        search troveSpec|label
        update troveSpec+
        erase troveSpec+
        install troveSpec+
        offer troveSpec+
        patch troveSpec+

    C{search} lines take a single troveSpec or label, which B{may} be
    enclosed in single or double quote characters.  Each of these
    lines represents a place to search for troves to install on
    or make available to the system.

    C{update}, C{erase}, C{install}, C{offer}, and C{patch} lines take
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

    Partial-line comments are ignored, and are not retained when a
    line is modified.
    '''

    def reset(self):
        CM.reset(self)
        self.commentLines = []
        self.filedata = []

    def parse(self, fileData=None, context=None):
        self.reset()
        if context is not None:
            self.context = context

        if fileData is not None:
            self.filedata = fileData

        for index, line in enumerate(self.filedata):
            line = line.strip()
            # Use 1-indexed line numbers that users will recognize
            index = index + 1

            if line.startswith('#') or not line:
                # empty lines are handled just like comments, and empty
                # lines and comments are always looked up in the
                # unmodified filedata, so we store only the index
                self.appendNoOpByText(line,
                    modified=False, index=index, context=self.context)
                continue

            # non-empty, non-comment lines must be parsed 
            try:
                verb, nouns = line.split(None, 1)
            except:
                raise CMError('%s: Invalid statement on line %d' %(
                                       self.context, index))

            if verb == 'version':
                nouns = nouns.split('#')[0].strip()
                self.setVersion(VersionOperation(text=nouns,
                    modified=False, index=index, context=self.context))

            elif verb == 'search':
                # Handle it if quoted, but it doesn't need to be
                nouns = ' '.join(shlex.split(nouns, comments=True))
                try:
                    searchOp = SearchLabel(text=nouns,
                       modified=False, index=index, context=self.context)
                except errors.ParseError:
                    searchOp = SearchTrove(text=nouns,
                       modified=False, index=index, context=self.context)
                self.appendOp(searchOp)

            elif verb in troveOpMap:
                self.appendTroveOpByName(verb,
                    text=shlex.split(nouns, comments=True),
                    modified=False, index=index, context=self.context,
                    deDup=False)

            else:
                raise CMError(
                    '%s: Unrecognized command "%s" on line %d' %(
                    self.context, verb, index))

    def iterFormat(self):
        '''
        Serialize the current model, including preserved comments.
        '''
        lastNoOpLine = max([x.index for x in self.noOps] + [1])
        lastOpLine = max([x.index for x in self.modelOps] + [1])
        # can only be one version
        if self.version is not None:
            verLine = self.version.index
        else:
            verLine = 1
        lastIndexLine = max(lastOpLine, lastNoOpLine, verLine)

        # First, emit all comments without an index as "header"
        for item in (x for x in self.noOps if x.index is None):
            yield item.format()

        # Now, emit the version if it is new (has no index)
        if self.version is not None and self.version.index is None:
            yield self.version.format()

        for i in range(lastIndexLine+1):
            if i in self.indexes:
                # Emit all the specified lines
                for item in self.indexes[i]:
                    # normally, this list is one item long
                    if item.modified:
                        yield item.format()
                    else:
                        yield self.filedata[i-1].rstrip('\n')

            # Last, emit any remaining lines
            if i == lastOpLine:
                for item in (x for x in self.modelOps if x.index is None):
                    yield item.format()

    def format(self):
        return '\n'.join([x for x in self.iterFormat()] + [''])

    def write(self, f):
        f.write(self.format())
