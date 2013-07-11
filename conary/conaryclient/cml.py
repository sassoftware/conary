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


"""
Implements the abstract Conary Model, as well as the Conary Model
Language (CML) serialization of the abstract model.  This conary
model is written explicitly in terms of labels and versions, and is
interpreted relative to system configuration items such as flavor,
pinTroves, excludeTroves, and so forth.
"""

import shlex

from conary import errors
from conary import trovetup
from conary import versions
from conary.conaryclient import cmdline
from conary.conaryclient.update import UpdateError

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
# includeTrove := troveTuple


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


class CMLocation(_namedtuple('CMLocation', 'line context op spec')):
    """
    line: line number (should be 1-indexed)
    context: file name or other similar context, or C{None}
    op: containing operation, or C{None}
    spec: containing operation, or C{None}
    """

    def __new__(cls, line, context=None, op=None, spec=None):
        if isinstance(line, cls):
            if context is None:
                context = line.context
            else:
                context = context
            if op is None:
                op = line.op
            else:
                op = op
            if spec is None:
                spec = line.spec
            else:
                spec = spec
            line = line.line
        return tuple.__new__(cls, (line, context, op, spec))

    def __repr__(self):
        op = None
        if self.op:
            op = self.op
        spec = None
        if self.spec:
            spec = self.spec
        return "%s(line=%r, context=%r, op=%r, spec=%r)" % (
            self.__class__.__name__, self.line, self.context, op, spec)

    def __str__(self):
        if self.context:
            context = str(self.context)
        else:
            context = ''
        if self.spec:
            spec = self.spec.asString()
        else:
            spec = ''
        if self.line is None:
            line = 'new-line'
        else:
            line = str(self.line)
        return ':'.join((x for x in (context, line, spec) if x))
    asString = __str__


class CMTroveSpec(trovetup.TroveSpec):
    '''
    Like parent class L{trovetup.TroveSpec} except that:
     - Parses a version separator of C{==} to be like C{=} but sets
       the C{pinned} member to C{True} (defaults to C{False}).
     - Has a C{snapshot} member that determines whether the version
       should be updated to latest, and a C{labelSpec()} method
       used to get the label on which to look for the latest version.
    Note that equality is tested only on name, version, and flavor,
    and that it is acceptable to test equality against an instance of
    C{trovetup.TroveSpec} or a simple C{(name, version, flavor)}
    tuple.
    '''
    def __new__(cls, name, version=None, flavor=None, **kwargs):
        if isinstance(name, (tuple, list)):
            name = list(name)
            name[0] = name[0].replace('==', '=')
        else:
            name = name.replace('==', '=')
        name, version, flavor = trovetup.TroveSpec(
            name, version, flavor, **kwargs)

        newTuple = tuple.__new__(cls, (name, version, flavor))
        if newTuple.version:
            newTuple.pinned = '==' in newTuple.version
            newTuple.local = '@local' in newTuple.version
            newTuple._has_branch = '/' in newTuple.version[1:]
        else:
            newTuple.pinned = False
            newTuple._has_branch = False
            newTuple.local = False

        newTuple.snapshot = (not newTuple.pinned and not newTuple.local
                             and newTuple._has_branch)

        return newTuple

    def __init__(self, *args, **kwargs):
        self.pinned = '==' in args[0]
        if self.version is not None:
            self._has_branch = '/' in self.version[1:]
            self.local = '@local' in self.version
        else:
            self._has_branch = False
            self.local = False
        self.snapshot = not self.pinned and not self.local and self._has_branch

    def labelSpec(self):
        # This is used only to look up newest versions on a label
        assert(self._has_branch)
        return self.name, self.version.rsplit('/', 1)[0], self.flavor

    def asString(self, withTimestamp=False):
        s = trovetup.TroveSpec.asString(self, withTimestamp=withTimestamp)
        if self.pinned:
            s = s.replace('=', '==', 1)
        return s

    __str__ = asString

    format = asString

    def __eq__(self, other):
        # We need to use indices so that we can compare to pure tuples,
        # as well as to trovetup.TroveSpec and to CMTroveSpec
        if not isinstance(other, tuple):
            return False

        return self[0:3] == other[0:3]

    # CMTroveSpec objects are pickled into the model cache, but there
    # only the TroveSpec parts are used
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
        self.context = context
        assert(text is not None or item is not None)
        assert(not(text is None and item is None))
        if item is not None:
            self.item = item
        else:
            self.parse(text=text)

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.index == other.index and
                self.modified == other.modified and
                self.context == other.context and
                self.item == other.item)

    def __iter__(self):
        yield self.item

    def getLocation(self, spec = None):
        return CMLocation(self.index, context = self.context, op = self,
                          spec = spec)

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

class SearchOperation(_CMOperation):
    key = 'search'

    def asString(self):
        return shellStr(self.item.asString())

class SearchTrove(SearchOperation):
    def parse(self, text):
        self.item = CMTroveSpec(text)

class SearchLabel(SearchOperation):
    def parse(self, text):
        self.item = versions.Label(text)


class IncludeOperation(_CMOperation):
    key = 'include'

    def asString(self):
        return shellStr(self.item.asString())

    def parse(self, text):
        self.item = CMTroveSpec(text)


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
        self.item = [CMTroveSpec(x) for x in text]

    def isEmpty(self):
        return not(self.item)

    def removeSpec(self, spec):
        self.item.remove(spec)
        self.modified = True

    def replaceSpec(self, spec, newSpec):
        i = self.item.index(spec)
        self.item[i] = newSpec
        self.modified = True

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

opMap = {
    UpdateTroveOperation.key  : UpdateTroveOperation,
    EraseTroveOperation.key   : EraseTroveOperation,
    InstallTroveOperation.key : InstallTroveOperation,
    OfferTroveOperation.key   : OfferTroveOperation,
    PatchTroveOperation.key   : PatchTroveOperation,
    IncludeOperation.key      : IncludeOperation,
}

class CM:
    # Make the operation objects available via models, avoiding the
    # need to import this module when a model is provided
    SearchTrove = SearchTrove
    SearchLabel = SearchLabel
    SearchOperation = SearchOperation
    IncludeOperation = IncludeOperation
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

    def copy(self):
        new = self.__class__(self.cfg, self.context)
        new.modelOps = list(self.modelOps)
        new.noOps = list(self.noOps)
        new.indexes = dict(self.indexes)
        new.version = self.version
        new.modelModified = self.modelModified
        return new

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

    def appendOp(self, op):
        self.modelOps.append(op)
        self._addIndex(op)

    def removeOp(self, op):
        self._removeIndex(op)
        while op in self.modelOps:
            self.modelOps.remove(op)
        self.modelModified = True

    def removeSpec(self, op, spec):
        op.removeSpec(spec)
        if op.isEmpty():
            self.removeOp(op)

    def replaceOp(self, op, newOp):
        self.modelModified = True
        self._removeIndex(op)
        i = self.modelOps.index(op)
        self.modelOps[i] = newOp
        self._addIndex(newOp)

    def appendOpByName(self, key, *args, **kwargs):
        op = opMap[key](*args, **kwargs)
        self.appendOp(op)
        return op

    def _iterOpTroveItems(self):
        for op in self.modelOps:
            if isinstance(op, (SearchTrove, TroveOperation, IncludeOperation)):
                for item in op:
                    yield item

    def refreshVersionSnapshots(self):
        from conary import conaryclient
        cfg = self.cfg
        cclient = conaryclient.ConaryClient(cfg)
        repos = cclient.getRepos()

        origOps = set()
        newOps = {}  # {TroveSpec: [CMTroveSpec, ...]}
        for item in self._iterOpTroveItems():
            if isinstance(item, CMTroveSpec) and item.snapshot:
                l = origOps.add(item)
                newSpec = item.labelSpec()
                l = newOps.setdefault(newSpec, [])
                l.append(item)

        allOpSpecs = list(origOps) + newOps.keys()

        if not allOpSpecs:
            return

        foundTroves = repos.findTroves(cfg.installLabelPath, 
            allOpSpecs, defaultFlavor=cfg.flavor, allowMissing=True)
        # origOps may be missing, newOps may not
        missing = set(newOps) - set(foundTroves)
        if missing:
            missing = sorted('%s=%s' % x[:2] for x in missing)
            if len(missing) == 1:
                raise errors.TroveNotFound("Trove not found: " + missing[0])
            else:
                raise errors.TroveNotFound("%d troves not found:\n%s" % (
                    len(missing), "\n".join(missing)))

        # Calculate the appropriate replacements from the lookup
        replaceSpecs = {} # CMTroveSpec: TroveSpec
        for troveKey in foundTroves:
            if troveKey in newOps:
                for oldTroveKey in newOps[troveKey]:
                    if foundTroves[troveKey] != foundTroves.get(oldTroveKey):
                        # found a new version, create replacement troveSpec
                        foundTrove = foundTroves[troveKey][0]
                        newVersion = foundTrove[1]
                        newverstr = '%s/%s' %(newVersion.trailingLabel(),
                                              newVersion.trailingRevision())
                        troveTup = (oldTroveKey[0], newverstr, oldTroveKey[2])
                        replaceSpecs[oldTroveKey] = troveTup

        # Apply the replacement specs to the model
        for op in self.modelOps:
            if isinstance(op, TroveOperation):
                newItem = [replaceSpecs.get(x, x) for x in op.item]
                if newItem != op.item:
                    # at least one spec was replaced; update the line
                    op.update(newItem)
            elif isinstance(op, (SearchTrove, IncludeOperation)):
                if op.item in replaceSpecs:
                    op.update(replaceSpecs[op.item])

    class InstallEraseSimplification(object):

        oldOpClass = InstallTroveOperation
        newOpClass = EraseTroveOperation

        @staticmethod
        def check(troveCache, g, oldOp, oldSpec, newOp, newSpec):
            oldSet = g.matchesByIndex(oldOp.getLocation(oldSpec))
            newSet = g.matchesByIndex(newOp.getLocation(newSpec))
            if (oldSet & newSet) != oldSet:
                return False

            if g.installIsNoop(troveCache, oldOp.getLocation(oldSpec)):
                return (EraseTroveOperation, newSpec)

            return None

    class UpdateEraseSimplification(object):

        oldOpClass = UpdateTroveOperation
        newOpClass = EraseTroveOperation

        @staticmethod
        def check(troveCache, g, oldOp, oldSpec, newOp, newSpec):
            oldSet = g.matchesByIndex(oldOp.getLocation(oldSpec))
            newSet = g.matchesByIndex(newOp.getLocation(newSpec))
            assert(oldSet is not None)
            assert(newSet is not None)
            if (oldSet & newSet) != oldSet:
                return False

            updateMap = g.getUpdateMapping(oldOp.getLocation())
            if not updateMap:
                # the update was a noop
                return (EraseTroveOperation, newSpec)

            newInstall = True
            for troveTup in oldSet:
                if updateMap[troveTup] is not None:
                    newInstall = False

            if newInstall:
                # this is a fresh install; we can cancel the update and
                # erase operations
                return None

            return (EraseTroveOperation, CMTroveSpec(oldSpec.name, None, None))

    class InstallUpdateSimplification(object):

        oldOpClass = InstallTroveOperation
        newOpClass = UpdateTroveOperation

        @staticmethod
        def check(troveCache, g, oldOp, oldSpec, newOp, newSpec):
            return (InstallTroveOperation, newSpec)

    class UpdateUpdateSimplification(object):

        oldOpClass = UpdateTroveOperation
        newOpClass = UpdateTroveOperation

        @staticmethod
        def check(troveCache, g, oldOp, oldSpec, newOp, newSpec):
            return (UpdateTroveOperation, newSpec)

    def _simplificationCandidate(self, l):
        types = ( self.InstallEraseSimplification,
                  self.UpdateEraseSimplification,
                  self.InstallUpdateSimplification,
                  self.UpdateUpdateSimplification )

        i = len(l) - 1
        while i > 0:
            for simpClass in types:
                if not isinstance(l[i][0], simpClass.newOpClass):
                    continue

                match = False
                for j, (op, spec) in enumerate(reversed(l[0:i])):
                    if isinstance(op, simpClass.oldOpClass):
                        match = True
                        # this i - j - 1 returns the index of this (op, spec)
                        # in l, undoing the reversed() and the slicing
                        yield (i - j - 1, i, simpClass)

                if match:
                    return

            i -= 1

        return

    def suggestSimplifications(self, troveCache, g):
        byName = {}
        changed = False
        for op in self.modelOps:
            if (isinstance(op, TroveOperation)):
                for spec in op:
                    byName.setdefault(spec.name, []).append((op, spec))

        for troveName, opList in byName.iteritems():
            for (oldIdx, newIdx, simplifyClass) in \
                                        self._simplificationCandidate(opList):
                oldOp, oldSpec = opList[oldIdx]
                newOp, newSpec = opList[newIdx]
                if newOp.index != None:
                    # We may later add aggressive simplification:
                    # if not simplifyAllLocalOps and newOp.index != None:
                    continue

                result = simplifyClass.check(troveCache, g, oldOp, oldSpec,
                                             newOp, newSpec)
                if result is False:
                    continue

                self.removeSpec(oldOp, oldSpec)

                if result is None:
                    self.removeSpec(newOp, newSpec)
                else:
                    (replaceOpClass, replaceSpec) = result
                    if isinstance(newOp, replaceOpClass):
                        if newSpec != replaceSpec:
                            newOp.replaceSpec(newSpec, replaceSpec)
                    else:
                        replaceOp = replaceOpClass(item = [ replaceSpec ],
                                                   index = newOp.index)
                        if len([ x for x in newOp]) == 1:
                            self.replaceOp(newOp, replaceOp)
                        else:
                            self.removeSpec(newOp, newSpec)
                            self.appendOp(replaceOp)

                changed = True
                break

        return changed

    def getMissingLocalTroves(self, troveCache, troveSet):
        troveTups = set(troveTup
                        for troveTup, inInstall, isExplicit
                        in troveSet._walk(troveCache, recurse = True)
                        if inInstall)

        localTroveTups = set()
        for op in self.modelOps:
            if not isinstance(op, TroveOperation):
                continue
            for spec in op:
                if spec.local:
                    localTroveTups.update(
                        troveSet.g.matchesByIndex(op.getLocation(spec)))

        return localTroveTups - troveTups


class CML(CM):
    '''
    Implements the abstract system model persisting in a text format,
    called CML, which is intended to be human-readable and human-editable.

    The format is::
        search troveSpec|label
        include troveSpec
        update troveSpec+
        erase troveSpec+
        install troveSpec+
        offer troveSpec+
        patch troveSpec+

    C{search} lines take a single troveSpec or label, which B{may} be
    enclosed in single or double quote characters.  Each of these
    lines represents a place to search for troves to install on
    or make available to the system.

    The C{troveSpec} entries in a model are nearly identical to
    a C{troveSpec} on the comand line, except that a single C{=}
    beween the name and the version means that the version can be
    updated by a C{conary updateall} operation, and a double C{==}
    between the name and the version means that updateall should
    not modify the version.

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

    C{include} lines take a single troveSpec, which B{may} be enclosed
    in single or double quote characters.  Each of these lines
    references a single trove which contains an additional block of
    CML that is executed at this point in the current model.  Any
    C{search} lines in the included CML apply only while executing
    the included CML.  Otherwise, the included CML is processed as
    if it were literally included.

    Whole-line comments are retained, and ordering is preserved
    with respect to non-comment lines.

    Partial-line comments are ignored, and are not retained when a
    line is modified.
    '''

    def copy(self):
        new = CM.copy(self)
        new.filedata = list(self.filedata)
        return new

    def reset(self):
        CM.reset(self)
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
                raise CMError('%s: Invalid statement "%s"'
                              %(CMLocation(index, self.context), line))

            if verb == VersionOperation.key:
                nouns = nouns.split('#')[0].strip()
                self.setVersion(VersionOperation(text=nouns,
                    modified=False, index=index, context=self.context))

            elif verb == SearchOperation.key:
                # Handle it if quoted, but it doesn't need to be
                try:
                    nouns = ' '.join(shlex.split(nouns, comments=True))
                except ValueError, e:
                    raise CMError('%s: %s' %(
                        CMLocation(index, self.context), str(e)))
                try:
                    searchOp = SearchLabel(text=nouns,
                       modified=False, index=index, context=self.context)
                except errors.ParseError:
                    try:
                        searchOp = SearchTrove(text=nouns,
                           modified=False, index=index, context=self.context)
                    except cmdline.TroveSpecError, e:
                        raise CMError('%s: %s' %(
                            CMLocation(index, self.context), str(e)))
                self.appendOp(searchOp)

            elif verb == IncludeOperation.key:
                # Handle it if quoted, but it doesn't need to be
                try:
                    nouns = ' '.join(shlex.split(nouns, comments=True))
                except ValueError, e:
                    raise CMError('%s: %s' %(
                        CMLocation(index, self.context), str(e)))
                try:
                    includeOp = IncludeOperation(text=nouns,
                       modified=False, index=index, context=self.context)
                except cmdline.TroveSpecError, e:
                    raise CMError('%s: %s' %(
                        CMLocation(index, self.context), str(e)))
                self.appendOp(includeOp)

            elif verb in opMap:
                try:
                    self.appendOpByName(verb,
                        text=shlex.split(nouns, comments=True),
                        modified=False, index=index, context=self.context)
                except ValueError, e:
                    raise CMError('%s: %s' %(
                        CMLocation(index, self.context), str(e)))

            else:
                raise CMError(
                    '%s: Unrecognized command "%s"' %(
                    CMLocation(index, self.context), verb))

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
