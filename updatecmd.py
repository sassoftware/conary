#
# Copyright (c) 2004-2005 Specifix, Inc.
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
from deps import deps
from lib import log
from lib import util
from local import database
from repository import changeset
from repository import repository
from repository.filecontainer import BadContainer
import conaryclient
import os
import sys

# FIXME client should instantiated once per execution of the command line 
# conary client

def doUpdate(cfg, pkgList, replaceFiles = False, tagScript = None, 
                                  keepExisting = False, depCheck = True,
                                  recurse = True, test = False,
                                  justDatabase = False):
    client = conaryclient.ConaryClient(cfg)

    applyList = []

    if type(pkgList) is str:
        pkgList = ( pkgList, )
    for pkgStr in pkgList:
        if os.path.exists(pkgStr) and os.path.isfile(pkgStr):
            try:
                cs = changeset.ChangeSetFromFile(pkgStr)
            except BadContainer, msg:
                log.error("'%s' is not a valid conary changset: %s" % 
                          (pkgStr, msg))
                sys.exit(1)
            applyList.append(cs)
        else:
            applyList.append(parseTroveSpec(pkgStr, cfg.flavor))

    # dedup
    applyList = {}.fromkeys(applyList).keys()

    try:
        (cs, depFailures, suggMap, brokenByErase) = \
            client.updateChangeSet(applyList, recurse = recurse,
                                   resolveDeps = depCheck,
                                   keepExisting = keepExisting,
                                   test = test)

        if brokenByErase:
            print "Troves being removed create unresolved dependencies:"
            for (troveName, depSet) in brokenByErase:
                print "    %s:\n\t%s" %  \
                        (troveName, "\n\t".join(str(depSet).split("\n")))
            return

        if depFailures:
            print "The following dependencies could not be resolved:"
            for (troveName, depSet) in depFailures:
                print "    %s:\n\t%s" %  \
                        (troveName, "\n\t".join(str(depSet).split("\n")))
            return
        elif (not cfg.autoResolve or brokenByErase) and suggMap:
            print "Additional troves are needed:"
            for (req, suggList) in suggMap.iteritems():
                print "    %s -> %s" % \
                  (req, " ".join(["%s(%s)" % 
                  (x[0], x[1].trailingRevision().asString()) for x in suggList]))
            return
        elif suggMap:
            print "Including extra troves to resolve dependencies:"
            print "   ",
            items = {}
            for suggList in suggMap.itervalues():
                # remove duplicates
                items.update(dict.fromkeys([(x[0], x[1]) for x in suggList]))

            items = items.keys()
            items.sort()
            print "%s" % (" ".join(["%s(%s)" % 
                           (x[0], x[1].trailingRevision().asString())
                           for x in items]))

        client.applyUpdate(cs, replaceFiles, tagScript, keepExisting,
                           test = test, justDatabase = justDatabase)
    except conaryclient.UpdateError, e:
        log.error(e)
    except repository.CommitError, e:
        log.error(e)

def doErase(cfg, itemList, tagScript = None, depCheck = True, test = False,
            justDatabase = False):
    client = conaryclient.ConaryClient(cfg=cfg)

    troveList = [ parseTroveSpec(item, cfg.flavor) for item in itemList ]
    # dedup
    troveList = {}.fromkeys(troveList).keys()

    brokenByErase = []
    try:
        brokenByErase = client.eraseTrove(troveList, tagScript = tagScript, 
                                          depCheck = depCheck, test = test,
                                          justDatabase = justDatabase)
    except repository.TroveNotFound, e:
        log.error(str(e))

    if brokenByErase:
        print "Troves being removed create unresolved dependencies:"
        for (troveName, depSet) in brokenByErase:
            print "    %s:\n\t%s" %  \
                    (troveName, "\n\t".join(str(depSet).split("\n")))
        return 1

def parseTroveSpec(specStr, defaultFlavor):
    if specStr.find('[') > 0 and specStr[-1] == ']':
        specStr = specStr[:-1]
        l = specStr.split('[')
        if len(l) != 2:
            raise TroveSpecError, "bad trove spec %s]" % specStr
        specStr, flavorSpec = l
        flavor = deps.parseFlavor(flavorSpec, mergeBase = defaultFlavor)
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
        versionSpec = None

    return (name, versionSpec, flavor)

class TroveSpecError(Exception):

    pass
