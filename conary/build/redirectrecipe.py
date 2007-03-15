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

    def addRedirect(self, toTrove, branchStr = None, sourceFlavor = None,
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

        l = self.rules.setdefault(fromTrove, list())
        if sourceFlavor is None:
            l.append((toTrove, branchStr, sourceFlavor, targetFlavor,
                      skipTargetMatching))
        else:
            # the default (with no sourceFlavor) has to be at the end to
            # make sure it matches last
            l.insert(0, (toTrove, branchStr, sourceFlavor, targetFlavor,
                         skipTargetMatching))

    def addRemoveRedirect(self, fromTrove = None):
        # We don't allow flavor-specificty for remove rules. You could write
        # redirect rules for everything which ought to be redirected and have
        # a catch-all remove redirect for everything else.
        if fromTrove is None:
            fromTrove = self.name
        elif fromTrove.find(":") != -1:
            raise ValueError, 'components cannot be individually redirected'

        # the None for branchStr (the second item in this tuple) indicates
        # this is a redirect to nothing
        l = self.rules.setdefault(self.name, list())
        l.insert(0, (self.name, None, None, None, None))

    def _findSourceTroves(self):
        sourceSearch = {}
        for fromTrove in self.rules.iterkeys():
            sourceSearch.setdefault(fromTrove, { self.branch : None })

        # this treats previously-built redirects as flavors we need to
        # redirect from, which seems a bit weird
        sourceTroveMatches = self.repos.getTroveLeavesByBranch(sourceSearch)

        if len(sourceTroveMatches) != len(sourceSearch):
            missing = set(sourceSearch) - set(sourceTroveMatches)
            raise builderrors.RecipeFileError, \
                    "No troves found with name(s) %s" % " ".join(missing)

        return sourceTroveMatches

    def _getSourceTroves(self, searchResult):
        l = []
        for name, d in searchResult.iteritems():
            for version, flavorList in d.iteritems():
                l += [ (name, (None, None), (version, x), True) 
                                for x in flavorList ]

        trvCsDict = {}
        # We don't need to recurse here since we only support package
        # redirects
        cs = self.repos.createChangeSet(l, recurse = False,
                                        withFiles = False)
        for trvCs in cs.iterNewTroveList():
            info = (trvCs.getName(), trvCs.getNewVersion(),
                    trvCs.getNewFlavor())
            trvCsDict[info] = trvCs

        return trvCsDict

    @staticmethod
    def _getTargetRules(rules, name):
        # return the rules for troves with this name; if it's a component of
        # a package we alrady built reuse the rule which we used for that 
        # package
        targetRules = rules.get(name, None)
        if targetRules is None and ':' in name:
            pkgName, compName = name.split(':')
            targetRules = rules.get(pkgName, None)
            if targetRules is not None:
                targetRules = \
                    [ (x[0] + ':' + compName,) + x[1:] for x in targetRules ]

        if targetRules is None:
            raise builderrors.RecipeFileError, \
                "Cannot find redirection for trove %s" % name

        return targetRules

    def _findAvailableTargetFlavors(self, destName, branchStr):
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
            # check for label multiplicity
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
            if destName in self.rules:
                raise builderrors.RecipeFileError, \
                    "Trove %s does not exist" % (destName)
        else:
            # Get the flavors and branch available on the target
            for version, flavorList in matches[destName].iteritems():
                targetFlavors.update((version, x) for x in flavorList)
        del matches

        return targetFlavors

    def findTroves(self):
        sourceTroveMatches = self._findSourceTroves()
        trvCsDict = self._getSourceTroves(sourceTroveMatches)

        redirMap = {}
        redirRuleMap = {}

        names = sourceTroveMatches.keys()
        additionalNameQueue = util.IterableQueue()
        # iterate over the troves which need redirects built by name; the
        # additionalNameQueue is used to build redirects for components of
        # packages
        for name in itertools.chain(names, additionalNameQueue):
            # all of the versions/flavors which currently exist for this
            # trove
            sourceTroveVersions = sourceTroveMatches.pop(name)

            # set of rules for where this trove should redirect to
            targetRules = self._getTargetRules(self.rules, name)

            # XXX the repository operations should be pulled out of all of
            # these loops
            additionalNames = set()
            for rule in targetRules:
                (destName, branchStr, sourceFlavorRestriction,
                         targetFlavorRestriction, skipTargetMatching) = rule
                # get all of the flavors this rule specifies redirecting to
                targetFlavors = \
                    self._findAvailableTargetFlavors(destName, branchStr)

                # This lets us catch where we haven't found any matches for
                # a particular trove name. If we have found any matches, no
                # error results, even if some of the troves on that label
                # cannot be redirected due to flavor conflicts
                foundMatch = False
                for version, flavorList in sourceTroveVersions.items():
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
                                # a default-flavor rule doesn't cause a
                                # conflict with a flavor-specifying rule
                                # because the later is more specific (and
                                # we know we've already processed the
                                # flavor-specifying rule because self.rules
                                # is sorted with flavor-specifying rules
                                # at the front)
                                previousRule = redirRuleMap[(name,
                                                             sourceFlavor)]
                                if (previousRule[2] is not None and
                                            sourceFlavorRestriction is None):
                                    # the default rule should be skipped
                                    # rather than causing a conflict
                                    continue

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
                        redirRuleMap[(name, sourceFlavor)] = rule

                        # Groups don't include any additional redirections, and
                        # neither do items which aren't collections
                        if (name.startswith('group-') or 
                            not trove.troveIsCollection(name)):
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

                        # the package redirect includes references to the
                        # component redirects to let the update code know
                        # how to redirect the components; this tracks the
                        # components of this redirect
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
        self.macros = macros.Macros()
        self.macros.update(extraMacros)
        self.rules = {}


