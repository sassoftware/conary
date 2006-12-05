# Copyright (c) 2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary import trove, versions
from conary.deps import deps
from conary.lib import util
from conary.build import errors as builderrors
from conary.build import macros
from conary.build import use
from conary.build.recipe import Recipe, RECIPE_TYPE_REDIRECT

import itertools

class RedirectRecipe(Recipe):
    Flags = use.LocalFlags
    _recipeType = RECIPE_TYPE_REDIRECT
    internalAbstractBaseClass = 1

    def addRedirect(self, name, branchStr = None, sourceFlavor = None,
                    targetFlavor = None, fromTrove = None, 
                    skipTargetMatching = False):
        if ((sourceFlavor is not None) and (targetFlavor is None)) or \
           ((targetFlavor is not None) and (sourceFlavor is None)):
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

        if fromTrove.startswith("group-"):
            # how sad
            raise ValueError, "groups cannot be redirected"

        self.addTroveList.append((name, branchStr, sourceFlavor, 
                                  targetFlavor, fromTrove, skipTargetMatching))

    def addRemoveRedirect(self, fromTrove = None):
        if fromTrove is None:
            fromTrove = self.name
        elif fromTrove.find(":") != -1:
            raise ValueError, 'components cannot be individually redirected'

        # the None for branchStr (the second item in this tuple) indicates
        # this is a redirect to nothing
        self.addTroveList.append((self.name, None, None, None, fromTrove, None))

    def findTroves(self):
        self.size = 0

        validSize = True
        troveList = []

        packageSet = {}

        sourceSearch = {}
        fromRule = {}
        for (name, branchStr, sourceFlavor, targetFlavor,
             fromTrove, skipTargetMatching) in self.addTroveList:
            l = fromRule.setdefault(fromTrove, list())
            # the catch-all (with no sourceFlavor) has to be at the end
            if sourceFlavor is None:
                l.append((name, branchStr, sourceFlavor, targetFlavor,
                          skipTargetMatching))
            else:
                l.insert(0, (name, branchStr, sourceFlavor, targetFlavor,
                             skipTargetMatching))

            sourceSearch.setdefault(fromTrove, { self.branch : None })

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
        cs = self.repos.createChangeSet(l, recurse = False, withFiles = False)
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
                targetFlavorRestriction, skipTargetMatching) in destSet:

                if branchStr is None:
                    # redirect to nothing
                    matches = None
                elif branchStr[0] == '/':
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
                    # may have matched multiple branches
                    if matches:
                        branches = set(x.branch() for x in matches[destName])
                        if len(branches) > 1:
                            raise builderrors.RecipeFileError, \
                                "Label %s matched multiple branches." % str(label)
                targetFlavors = set()
                if matches is None:
                    # Intentional redirect to nothing
                    pass
                elif destName not in matches:
                    # We're redirecting to something which doesn't
                    # exist. This is an error if it's the top of a
                    # redirect (a package), but generates an erase
                    # redirect if it's for a component.
                    if name in names:
                        raise builderrors.RecipeFileError, \
                            "Trove %s does not exist" % (destName)
                else:
                    # Get the flavors and branch available on the target
                    for version, flavorList in matches[destName].iteritems():
                        targetFlavors.update((version, x) for x in flavorList)
                del matches

                foundMatch = False
                for version, flavorList in versionDict.items():
                    for sourceFlavor in flavorList:
                        if sourceFlavorRestriction is not None and \
                           sourceFlavor != sourceFlavorRestriction: continue

                        match = None
                        for targetVersion, targetFlavor in targetFlavors:
                            if (not skipTargetMatching and 
                                targetFlavorRestriction is not None and
                                targetFlavor != targetFlavorRestriction):
                                continue

                            if ((sourceFlavorRestriction is not None)
                                or skipTargetMatching
                                or sourceFlavor.score(targetFlavor) is not False):
                                match = targetVersion
                                break

                        if match is not None:
                            if (name, sourceFlavor) in redirMap:
                                raise builderrors.RecipeFileError, \
                                    "Multiple redirect targets specified " \
                                    "from trove %s[%s]" % (name, sourceFlavor)

                            redirInfo = (destName, match.branch(),
                                         targetFlavorRestriction)
                        elif not targetFlavors:
                            # redirect to nothing
                            redirInfo = (None, None, None)
                        elif targetFlavorRestriction is not None:
                            raise builderrors.RecipeFileError, \
                                "Trove %s does not exist for flavor %s" \
                                % (name, targetFlavor)
                        else:
                            continue

                        # we created a redirect!
                        foundMatch = True

                        redirMap[(name, sourceFlavor)] = redirInfo + ([], )

                        if not trove.troveIsCollection(name):
                            continue

                        # add any troves the redirected trove referenced
                        # to the todo list
                        trvCs = trvCsDict[(name, version, sourceFlavor)]

                        # we can't integrity check here because we got
                        # the trove w/o files
                        trv = trove.Trove(trvCs, skipIntegrityChecks = True)

                        subNames = set()
                        for info in trv.iterTroveList(strongRefs = True):
                            assert(info[1] == version)
                            assert(info[2] == sourceFlavor)
                            subNames.add(info[0])
                            d = sourceTroveMatches.setdefault(info[0], {})
                            d.setdefault(info[1], []).append(info[2])

                        allOldVersions = self.repos.getTroveVersionsByBranch(
                            { trv.getName() :
                                { trv.getVersion().branch() : None } } )
                        l = []
                        for subVersion, subFlavorList in \
                                allOldVersions[trv.getName()].iteritems():
                            l += [ ( trv.getName(), subVersion, flavor)
                                     for flavor in subFlavorList ]

                        allTroves = self.repos.getTroves(l, withFiles = False)
                        neededNames = set()
                        for otherTrv in allTroves:
                            neededNames.update(
                               [ x[0] for x in
                                 otherTrv.iterTroveList(strongRefs = True) ] )

                        # subNames is all of the included troves we've built
                        # redirects for, and neededNames is everything we
                        # should have built them for; the difference is troves
                        # that disappeared
                        for subName in neededNames - subNames:
                            d = sourceTroveMatches.setdefault(subName, {})
                            d.setdefault(None, []).append(sourceFlavor)

                        redirMap[(name, sourceFlavor)][-1].extend(neededNames)

                        additionalNames.update(neededNames)

                if not foundMatch:
                    raise builderrors.CookError(
                    "Could not find target with satisfying flavor"
                    " for redirect %s - either create a redirect"
                    " with targetFlavor and sourceFlavor set, or"
                    " create a redirect with skipTargetMatching = True" % name)

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


