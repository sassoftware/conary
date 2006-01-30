# Copyright (c) 2005 rPath, Inc.
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

from conary import trove, versions
from conary.lib import util
from conary.repository import errors
from conary.build import errors as builderrors
from conary.build import macros
from conary.build import use
from conary.build.recipe import Recipe, RECIPE_TYPE_REDIRECT

import itertools

class RedirectRecipe(Recipe):
    Flags = use.LocalFlags
    _recipeType = RECIPE_TYPE_REDIRECT
    ignore = 1

    def addRedirect(self, name, branchStr = None, sourceFlavor = None,
                    targetFlavor = None, fromTrove = None):
        if (sourceFlavor and not targetFlavor) or \
           (targetFlavor and not sourceFlavor):
            raise builderrors.RecipeFileError, \
                "sourceFlavor and targetFlavor must be specified jointly"

        if sourceFlavor is not None:
            f = deps.parseFlavor(sourceFlavor)
            if f is None:
                raise ValueError, 'invalid flavor %s' % sourceFlavor
            sourceFlavor = f

        if targetFlavor is not None:
            f = deps.parseFlavor(targetFlavor)
            if f is None:
                raise ValueError, 'invalid flavor %s' % targetFlavor
            targetFlavor = f

        if fromTrove is None:
            fromTrove = self.name
        elif fromTrove.find(":") != -1:
            raise ValueError, 'components cannot be individually redirected'
        elif fromTrove.startswith("group-"):
            # how sad
            raise ValueError, "groups cannot be redirected"

        self.addTroveList.append((name, branchStr, sourceFlavor, 
                                  targetFlavor, fromTrove))

    def findTroves(self):
        self.size = 0

        validSize = True
        troveList = []

        packageSet = {}

        sourceSearch = {}
        fromRule = {}
        for (name, branchStr, sourceFlavor, targetFlavor, fromTrove) in \
                                self.addTroveList:
            l = fromRule.setdefault(fromTrove, list())
            # the catch-all (with no sourceFlavor) has to be at the end
            if not sourceFlavor:
                l.append((name, branchStr, sourceFlavor, targetFlavor))
            else:
                l.insert(0, (name, branchStr, sourceFlavor, targetFlavor))

            d = sourceSearch.setdefault(fromTrove, { self.branch : None })

        sourceTroveMatches = self.repos.getTroveLeavesByBranch(sourceSearch)

        if len(sourceTroveMatches) != len(sourceSearch):
            missing = set(sourceSearch) - set(sourceTroveMatches)
            raise builderrors.RecipeFileError, \
                    "No troves found with name(s) %s" % " ".join(missing)

        l = []
        for name, d in sourceTroveMatches.iteritems():
            for version, flavorList in d.iteritems():
                l += [ (name, (None, None), (version, x), True) 
                                for x in flavorList ]

        trvCsDict = {}
        # We don't need to recurse here since we only support package
        # redirects
        cs = self.repos.createChangeSet(l, recurse = True, withFiles = False)
        for trvCs in cs.iterNewTroveList():
            info = (trvCs.getName(), trvCs.getNewVersion(),
                    trvCs.getNewFlavor())
            trvCsDict[info] = trvCs

        redirMap = {}

        names = sourceTroveMatches.keys()
        additionalNameQueue = util.IterableQueue()
        for name in itertools.chain(names, additionalNameQueue):
            versionDict = sourceTroveMatches.pop(name)

            destSet = fromRule.get(name, None)
            if destSet is None and ':' in name:
                # package redirections imply component redirections
                pkgName, compName = name.split(':')
                destSet = fromRule.get(pkgName, None)
                if destSet is not None:
                    destSet = set(
                        [ (x[0] + ':' + compName,) + x[1:] for x in destSet ])

            if destSet is None:
                raise builderrors.RecipeFileError, \
                    "Cannot find redirection for trove %s" % name

            # XXX the repository operations should be pulled out of all of
            # these loops
            additionalNames = set()
            for (destName, branchStr, sourceFlavorRestriction, 
                                      targetFlavorRestriction) in destSet:
                if branchStr[0] == '/':
                    branch = versions.VersionFromString(branchStr)
                    if not isinstance(branch, versions.Branch):
                        raise builderrors.RecipeFileError, \
                            "Redirects must specify branches or labels, " \
                            "not versions"

                    matches = self.repos.getTroveLeavesByBranch(
                                    { destName : { branch : None } })
                else:
                    label = versions.Label(branchStr)
                    matches = self.repos.getTroveLeavesByLabel(
                                    { destName : { label : None } })
                    # if there are multiple versions returned, the label
                    # specified multiple branches
                    if matches and len(matches[destName]) > 1:
                        raise builderrors.RecipeFileError, \
                            "Label %s matched multiple branches." % str(label)

                # use an empty list to indicate notexistance
                if destName not in matches:
                    # We're redirecting to something which doesn't
                    # exist. This is an error if it's the top of a
                    # redirect (a package), but generates an erase
                    # redirect if it's for a component.
                    if name in names:
                        raise builderrors.RecipeFileError, \
                            "Trove %s does not exist" % (destName, targetFlavor)

                    redirMap[(name, sourceFlavor)] = (None, None, None, [])
                    continue

                # Get the flavors and branch available on the target
                targetFlavors = set()
                for version, flavorList in matches[destName].iteritems():
                    targetFlavors.update((version, x) for x in flavorList)
                del matches

                for version, flavorList in versionDict.items():
                    for sourceFlavor in flavorList:
                        if sourceFlavorRestriction is not None and \
                           sourceFlavor != sourceFlavorRestriction: continue

                        match = None
                        for targetVersion, targetFlavor in targetFlavors:
                            if targetFlavorRestriction is not None and \
                               targetFlavor != targetFlavorRestriction:
                                continue

                            if sourceFlavor.score(targetFlavor) is not None:
                                match = targetVersion
                                break

                        if match is not None:
                            # we can redirect this trove. go ahead and
                            # setup the redirect for it, and add any troves
                            # it references to the todo list
                            trvCs = trvCsDict[(name, version, sourceFlavor)]
                            trv = trove.Trove(trvCs)

                            for info in trv.iterTroveList(strongRefs = True):
                                assert(info[1] == version)
                                assert(info[2] == sourceFlavor)
                                additionalNames.add(info[0])
                                d = sourceTroveMatches.setdefault(info[0], {})
                                flavorList = d.setdefault(info[1], [])
                                flavorList.append(info[2])

                            assert((name, sourceFlavor) not in redirMap)
                            redirMap[(name, sourceFlavor)] = \
                                (destName, match.branch(),
                                 targetFlavorRestriction,
                                 [ x[0] for x in 
                                    trv.iterTroveList(strongRefs = True) ] )
                        elif targetFlavorRestriction is not None:
                            raise builderrors.RecipeFileError, \
                                "Trove %s does not exist for flavor %s" \
                                % (name, targetFlavor)

            for name in additionalNames:
                additionalNameQueue.add(name)

        self.redirections = redirMap

    def getRedirections(self):
        return self.redirections

    def __init__(self, repos, cfg, branch, flavor, extraMacros={}):
        self.repos = repos
        self.cfg = cfg
        self.redirections = {}
        self.branch = branch
        self.flavor = flavor
        self.addTroveList = []
        self.macros = macros.Macros()
        self.macros.update(extraMacros)


