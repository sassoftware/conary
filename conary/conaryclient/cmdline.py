#
# Copyright (c) 2004-2008 rPath, Inc.
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
#
import os

from conary import conarycfg
from conary import errors
from conary import state
from conary.deps import deps
from conary.lib import log, cfgtypes
from conary.repository import changeset
from conary.repository.filecontainer import BadContainer

def parseTroveSpec(specStr, allowEmptyName = True):
    """
    Parse a TroveSpec string

    @param specStr: the input string
    @type specStr: string

    @param allowEmptyName: if set, will accept an empty string and some other 
    variations.
    @type allowEmptyName: bool 

    @rtype: list
    @return: (name, version, flavor)

    @raise TroveSpecError: Raised if the input string is not a valid TroveSpec
    """
    origSpecStr = specStr
    if specStr.find('[') > 0 and specStr[-1] == ']':
        specStr = specStr[:-1]
        l = specStr.split('[')
        if len(l) != 2:
            raise TroveSpecError(origSpecStr, "bad flavor spec")
        specStr, flavorSpec = l
        flavor = deps.parseFlavor(flavorSpec)
        if flavor is None:
            raise TroveSpecError(origSpecStr, "bad flavor spec")
    else:
        flavor = None

    if specStr.find("=") >= 0:
        l = specStr.split("=")
        if len(l) != 2:
            raise TroveSpecError(origSpecStr, "Too many ='s")
        name, versionSpec = l
    else:
        name = specStr
        versionSpec = None
    if not name and not allowEmptyName:
        raise TroveSpecError(origSpecStr, 'Trove name is required')

    return (name, versionSpec, flavor)

def _getChangeSet(path):
        try:
            cs = changeset.ChangeSetFromFile(path)
        except BadContainer, msg:
            # ensure that it is obvious that a file is being referenced
            if path[0] not in './':
                path = './' + path
            log.error("'%s' is not a valid conary changeset: %s" % 
                      (path, msg))
            # XXX sys.exit is gross
            import sys
            sys.exit(1)
        log.debug("found changeset file %s" % path)
        return cs

def parseUpdateList(updateList, keepExisting, updateByDefault=True):
    # If keepExisting is true, we want our specifications to be relative
    # to nothing. If it's false, they should be absolute as updateChangeSet
    # interperts absolute jobs as ones which should be rooted (if there is
    # anything available to root them to).

    areAbsolute = not keepExisting

    applyList = []

    if type(updateList) is str:
        updateList = ( updateList, )

    for updateStr in updateList:
        if os.path.exists(updateStr) and os.path.isfile(updateStr):
            applyList.append(_getChangeSet(updateStr))
            continue
        else:
            troveSpec = parseTroveSpec(updateStr)
            if troveSpec[0][0] == '-':
                applyList.append((troveSpec[0], troveSpec[1:],
                                  (None, None), False))
            elif troveSpec[0][0] == '+':
                applyList.append((troveSpec[0], (None, None), 
                                  troveSpec[1:], areAbsolute))
            elif updateByDefault:
                applyList.append((troveSpec[0], (None, None), 
                                  troveSpec[1:], areAbsolute))
            else:
                applyList.append((troveSpec[0], troveSpec[1:],
                                  (None, None), False))
            log.debug("will look for %s", applyList[-1])

    # dedup
    return set(applyList)


def parseChangeList(changeSpecList, keepExisting=False, updateByDefault=True,
                    allowChangeSets=True):
    """
    Parse a change specification list, as presented on the command line.

    Takes input specifying changeSpecs, such as C{foo=1.1--1.2},
    and turns it into C{(name, (oldVersionSpec, oldFlavorSpec),
    (newVersionSpec, newFlavorSpec), isAbsolute)} tuples.

    @note:
        If a filename is passed as a changeSpec, and the file does not contain
    a valid conary changeset, a sys.exit() will be called.
    
    @param changeSpecList: a changeSpec, such as C{foo=1.1--1.2}
    @type changeSpecList: string

    @param keepExisting: specifies whether an installed trove should be
    kept in addition to an updated version.
    @type keepExisting: bool
    
    @param updateByDefault:
    @type updateByDefault: bool
   
    @param allowChangeSets: specifies whether file-based changesets are
    allowed.
    @type allowChangeSets: bool

    @raise TroveSpecError: Raised if an invalid TroveSpec is passed within the
    ChangeSpec list.

    @rtype: list
    @return: a list of changes to apply, of the form
    (name, (oldVersion, oldFlavor), (newVersion, newFlavor), replaceExisting)
    where either the old or new version/flavor (but not both) may be 
    (None, None)
    """
    applyList = []

    if isinstance(changeSpecList, str):
        changeSpecList = (changeSpecList,)


    for changeSpec in changeSpecList:

        isAbsolute = False

        if (allowChangeSets and os.path.exists(changeSpec) 
            and os.path.isfile(changeSpec)):
            applyList.append(_getChangeSet(changeSpec))
            continue

        l = changeSpec.split("--")

        if len(l) == 1:
            (troveName, versionStr, flavor) = parseTroveSpec(l[0], False)

            if troveName[0] == '-':
                applyList.append((troveName, (versionStr, flavor), 
                                  (None, None), False))
            elif troveName[0] == '+' or updateByDefault:
                applyList.append((troveName, (None, None), 
                                  (versionStr, flavor), not keepExisting))
            else:
                applyList.append((troveName, (versionStr, flavor), 
                                  (None, None), False))
        elif len(l) != 2:
            log.error("one -- expected in change spec '%s'", 
                      changeSpec)
            return
        else:
            oldSpec, newSpec = l
            (troveName, oldVersion, oldFlavor) = parseTroveSpec(oldSpec)

            if newSpec:
                newSpec = troveName + "=" + newSpec
                (troveName, newVersion, newFlavor) = parseTroveSpec(newSpec)
            else:
                newVersion, newFlavor = None, None

            if (newVersion or (newFlavor is not None)) and \
                   not (oldVersion or (oldFlavor is not None)):
                # foo=--1.2
                oldVersion, oldFlavor = None, None

            applyList.append((troveName, (oldVersion, oldFlavor), 
                              (newVersion, newFlavor), False))

    # dedup, but keep ordering - this is a little slower but can be handy 
    # for lining up input -> output (if input is deduped)
    finalList = []
    for item in applyList:
        if item not in finalList:
            finalList.append(item)

    return finalList

def toTroveSpec(name, versionStr, flavor):
    """
    Construct a TroveSpec string from name + version + flavor

    @param name: trove name
    @type name: string

    @param versionStr: trove version string
    @type versionStr: string

    @param flavor: trove flavor
    @type flavor: L{deps.deps.Flavor}

    @rtype: string
    @return: a TroveSpec of the form name=version[flavor]
    """

    disp = [name]
    if versionStr:
        disp.extend(('=', versionStr))
    if flavor is not None and not flavor.isEmpty():
        disp.extend(('[', deps.formatFlavor(flavor), ']'))
    return ''.join(disp)

def askYn(prompt, default=None):
    while True:
        try:
            resp = raw_input(prompt + ' ')
        except EOFError:
            return False

        resp = resp.lower()
        if resp in ('y', 'yes'):
            return True
        elif resp in ('n', 'no'):
            return False
        elif not resp:
            return default
        else:
            print "Unknown response '%s'." % resp

class TroveSpecError(errors.ParseError):
    def __init__(self, spec, error):
        self.spec = spec
        errors.ParseError.__init__(self, 'Error with spec "%s": %s' % (spec, error))

def setContext(cfg, context=None, environ=None, searchCurrentDir=False):
    if environ is None:
        environ = os.environ
    if context is not None:
        where = 'given manually'
    else:
        context = cfg.context
        where = 'specified as the default context in the conary configuration'
        if searchCurrentDir and os.access('CONARY', os.R_OK):
            conaryState = state.ConaryStateFromFile('CONARY', parseSource=False)
            if conaryState.hasContext():
                context = conaryState.getContext()
                where = 'specified in the CONARY state file'

        if 'CONARY_CONTEXT' in environ:
            context = environ['CONARY_CONTEXT']
            where = 'specified in the CONARY_CONTEXT environment variable'
    if context:
        if not cfg.getContext(context):
            raise cfgtypes.CfgError('context "%s" (%s) does not exist' % (context, where))
        cfg.setContext(context)
    return cfg


