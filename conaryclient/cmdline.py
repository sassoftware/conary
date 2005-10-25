#
# Copyright (c) 2004 rPath, Inc.
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
#
import os

from deps import deps
from lib import log

from repository import changeset
from repository.filecontainer import BadContainer

def parseTroveSpec(specStr):
    if specStr.find('[') > 0 and specStr[-1] == ']':
        specStr = specStr[:-1]
        l = specStr.split('[')
        if len(l) != 2:
            raise TroveSpecError, "bad trove spec %s]" % specStr
        specStr, flavorSpec = l
        flavor = deps.parseFlavor(flavorSpec)
        if flavor is None:
            raise TroveSpecError, "bad flavor [%s]" % flavorSpec
    else:
        flavor = None

    if specStr.find("=") >= 0:
        l = specStr.split("=")
        if len(l) != 2:
            raise TroveSpecError, "too many ='s in %s" %specStr
        name, versionSpec = l
    else:
        name = specStr
        versionSpec = ''

    return (name, versionSpec, flavor)

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
            try:
                cs = changeset.ChangeSetFromFile(updateStr)
            except BadContainer, msg:
                # ensure that it is obvious that a file is being referenced
                if updateStr[0] not in './':
                    updateStr = './' + updateStr
                log.error("'%s' is not a valid conary changeset: %s" % 
                          (updateStr, msg))
                sys.exit(1)
            applyList.append(cs)
            log.debug("found changeset file %s" % updateStr)
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


def parseChangeList(troveList):
    """ Takes input specifying changesets, such as foo=1.1--1.2,
        and turns it into (name, (oldVersionSpec, oldFlavorSpec),
                                 (newVersionSpec, newFlavorSpec), isAbsolute)
        tuples.
    """
    if isinstance(troveList, str):
        troveList = (troveList,)
    applyList = []
    for item in troveList:
        l = item.split("--")
        isAbsolute = False
        add = True

        if len(l) == 1:
            (troveName, versionStr, flavor) = parseTroveSpec(l[0])

            if troveName[0] == '-':
                troveName = troveName[1:]
                oldVersion, oldFlavor = versionStr, flavor
                newVersion, newFlavor = None, None
            else:
                isAbsolute = True
                if troveName[0] == '+':
                    troveName = troveName[1:]
                oldVersion, oldFlavor = None, None
                newVersion, newFlavor = versionStr, flavor
        elif len(l) != 2:
            log.error("one = expected in '%s' argument to changeset", item)
            return
        else:
            (troveName, oldVersion, oldFlavor) = parseTroveSpec(l[0])

            if l[1]:
                if l[0]:
                    l[1] = troveName + "=" + l[1]
                (troveName, newVersion, newFlavor) = parseTroveSpec(l[1])
            else:
                newVersion, newFlavor = None, None

            if (newVersion or newFlavor) and not (oldVersion or oldFlavor):
                # foo=--1.2
                oldVersion, oldFlavor = None, None
        applyList.append((troveName, (oldVersion, oldFlavor), 
                                     (newVersion, newFlavor), isAbsolute))

    return applyList

def toTroveSpec(name, versionStr, flavor):
    disp = [name]
    if versionStr:
        disp.extend(('=', versionStr))
    if flavor:
        disp.extend(('[', deps.formatFlavor(flavor), ']'))
    return ''.join(disp)

class TroveSpecError(Exception):

    pass

