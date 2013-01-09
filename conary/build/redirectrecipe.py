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


from conary import trove, versions
from conary.deps import deps
from conary.build import errors as builderrors
from conary.build import defaultrecipes
from conary.build import macros
from conary.build import use
from conary.build.recipe import Recipe, RECIPE_TYPE_REDIRECT
from conary.build.packagerecipe import BaseRequiresRecipe
from conary.lib import log

class _Redirect(object):

    __slots__ = [ 'components', 'isRemove' ]

    def __init__(self):
        self.components = []

    def addComponents(self, nameList):
        self.components += nameList

class _RemoveRedirect(_Redirect):

    isRemove = True

class _RedirectInfo(_Redirect):

    __slots__ = [ 'targetName', 'targetBranch', 'targetFlavor', 'components' ]
    isRemove = False

    def __init__(self, targetName, targetBranch, targetFlavor):
        assert(targetName is not None)
        _Redirect.__init__(self)
        self.targetName = targetName
        self.targetBranch = targetBranch
        self.targetFlavor = targetFlavor

class _Redirections(dict):

    def add(self, sourceName, sourceFlavor, redir):
        l = self.setdefault((sourceName, sourceFlavor), [])
        l.append(redir)

class _RedirectRule(object):
    __slots__ = [ 'destName', 'branchStr', 'sourceFlavor', 'targetFlavor',
                  'skipTargetMatching', 'sourceName', 'allowMultipleTargets' ]

    def findAvailableTargetFlavors(self, repos):
        if self.branchStr is None:
            # redirect to nothing
            return set()

        if self.branchStr[0] == '/':
            branch = versions.VersionFromString(self.branchStr)
            if not isinstance(branch, versions.Branch):
                raise builderrors.RecipeFileError, \
                    "Redirects must specify branches or labels, " \
                    "not versions"

            log.info('redirecting to branches is deprecated; redirects must '
                     'be to labels')

            matches = repos.getTroveLeavesByBranch(
                            { self.destName : { branch : None } })
        else:
            label = versions.Label(self.branchStr)
            matches = repos.getTroveLatestByLabel(
                            { self.destName : { label : None } })

        targetFlavors = set()
        # Get the flavors and branch available on the target
        for version, flavorList in matches.get(self.destName, {}).iteritems():
            targetFlavors.update((version, x) for x in flavorList)

        return targetFlavors

    def __str__(self):
        return "%s[%s] -> %s=%s[%s]" % (self.sourceName, self.sourceFlavor,
                self.destName, self.branchStr, self.targetFlavor)

    def __init__(self, sourceName = None, destName = None, branchStr = None,
                 sourceFlavor = None, targetFlavor = None,
                 skipTargetMatching = None, allowMultipleTargets = False):
        self.sourceName = sourceName
        self.destName = destName
        self.branchStr = branchStr
        self.sourceFlavor = sourceFlavor
        self.targetFlavor = targetFlavor
        self.skipTargetMatching = skipTargetMatching
        self.allowMultipleTargets = allowMultipleTargets

class _RedirectRecipe(Recipe):
    Flags = use.LocalFlags
    _recipeType = RECIPE_TYPE_REDIRECT
    internalAbstractBaseClass = 1

    def _addRule(self, rule):
        l = self.rules.setdefault(rule.sourceName, list())
        if rule.sourceFlavor is None:
            l.append(rule)
        else:
            # the default (with no sourceFlavor) has to be at the end to
            # make sure it matches last
            l.insert(0, rule)

    def addRedirect(self, toTrove, branchStr = None, sourceFlavor = None,
                    targetFlavor = None, fromTrove = None,
                    skipTargetMatching = False, allowMultipleTargets = False):
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

        rule = _RedirectRule(sourceName = fromTrove, destName = toTrove,
                             branchStr = branchStr, sourceFlavor = sourceFlavor,
                             targetFlavor = targetFlavor,
                             skipTargetMatching = skipTargetMatching,
                             allowMultipleTargets = allowMultipleTargets)
        self._addRule(rule)

    def addRemoveRedirect(self, fromTrove = None):
        # We don't allow flavor-specificty for remove rules. You could write
        # redirect rules for everything which ought to be redirected and have
        # a catch-all remove redirect for everything else.
        if fromTrove is None:
            fromTrove = self.name
        elif fromTrove.find(":") != -1:
            raise ValueError, 'components cannot be individually redirected'

        rule = _RedirectRule(sourceName = fromTrove)
        self._addRule(rule)

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
        if targetRules is None:
            raise builderrors.RecipeFileError, \
                "Cannot find redirection for trove %s" % name

        return targetRules

    def _buildRedirect(self, trvCsDict, sourceFlavor,
                       sourceVersion, rule, target):
        if target[0] is not None:
            redirInfo = _RedirectInfo(target[0], target[1].branch(), rule.targetFlavor)
        else:
            redirInfo = _RemoveRedirect()

        self.redirections.add(rule.sourceName, sourceFlavor, redirInfo)

        # Groups don't include any additional redirections, and
        # neither do items which aren't collections
        if (trove.troveIsGroup(rule.sourceName) or
            not trove.troveIsCollection(rule.sourceName)):
            return

        if target[0] is not None:
            targetTrove = self.repos.getTrove(withFiles = False, *target)
            targetComponents = set([ x[0].split(':')[1]
                for x in
                targetTrove.iterTroveList(strongRefs = True) ])
        else:
            targetComponents = set()

        # we can't integrity check here because we got
        # the trove w/o files
        trvCs = trvCsDict[(rule.sourceName, sourceVersion, sourceFlavor)]
        trv = trove.Trove(trvCs)

        # assemble a set of all of the components included
        # in this trove
        currentComponents = set([ x[0].split(':')[1] for x in
                        trv.iterTroveList(strongRefs = True) ])

        # components shared between the current trove and
        # the target should be redirected to the target
        # components
        for compName in currentComponents & targetComponents:
            sourceCompName = rule.sourceName + ':' + compName
            targetCompName = redirInfo.targetName + ':' + compName
            self.redirections.add(sourceCompName, sourceFlavor,
                    _RedirectInfo(targetCompName, redirInfo.targetBranch,
                                  redirInfo.targetFlavor))

        # now get all of the components which have been
        # included in this trove anywhere on the branch; those
        # components need to generate erase redirects
        allVersions = self.repos.getTroveVersionsByBranch(
            { trv.getName() :
                { trv.getVersion().branch() : None } } )
        l = []
        for subVersion, subFlavorList in \
                allVersions[trv.getName()].iteritems():
            l += [ ( trv.getName(), subVersion, flavor)
                     for flavor in subFlavorList ]

        allTroves = self.repos.getTroves(l, withFiles = False)
        allComponents = set()
        for otherTrv in allTroves:
            allComponents.update(
               [ x[0].split(':')[1] for x in
                 otherTrv.iterTroveList(strongRefs = True) ] )

        # components which existed at any point for this
        # trove but don't have a component in the redirect
        # target need to be erased
        for subName in allComponents - targetComponents:
            newName = rule.sourceName + ':' + subName
            self.redirections.add(newName, sourceFlavor, _RemoveRedirect())

        # the package redirect includes references to the
        # component redirects to let the update code know
        # how to redirect the components; this tracks the
        # components of this redirect
        redirInfo.addComponents(
            [ rule.sourceName + ':' + x for x in allComponents ])

    def findTroves(self):
        sourceTroveMatches = self._findSourceTroves()
        trvCsDict = self._getSourceTroves(sourceTroveMatches)

        redirRuleMap = {}

        # sourceTroveVersions is all of the versions/flavors which
        # currently exist for this trove
        for sourceName, sourceTroveVersions in sourceTroveMatches.iteritems():
            # set of rules for where this trove should redirect to
            targetRules = self._getTargetRules(self.rules, sourceName)

            # XXX the repository operations should be pulled out of all of
            # these loops
            additionalNames = set()
            for rule in targetRules:
                # get all of the flavors this rule specifies redirecting to
                targetFlavors = rule.findAvailableTargetFlavors(self.repos)

                if rule.branchStr and not targetFlavors:
                    # We're redirecting to something which doesn't
                    # exist.
                    raise builderrors.RecipeFileError, \
                        "Trove %s does not exist" % (rule.destName)

                # This lets us catch where we haven't found any matches for
                # this rule. If we have found any matches for this rule, no
                # error results, even if some of the troves on that label
                # cannot be redirected due to flavor conflicts
                foundMatch = False

                # Try to create redirects for each version/flavor combination
                for sourceVersion, flavorList in sourceTroveVersions.items():
                    for sourceFlavor in flavorList:
                        if rule.sourceFlavor is not None and \
                           sourceFlavor != rule.sourceFlavor:
                            continue

                        match = None
                        for targetVersion, targetFlavor in targetFlavors:
                            if (not rule.skipTargetMatching and
                                rule.targetFlavor is not None and
                                targetFlavor != rule.targetFlavor):
                                continue

                            if ((rule.sourceFlavor is not None)
                                or rule.skipTargetMatching
                                or sourceFlavor.score(targetFlavor) is not False):
                                match = (targetVersion, targetFlavor)
                                break

                        if match is not None:
                            # found a compatible trove to redirect to
                            if (sourceName, sourceFlavor) in self.redirections:
                                # a default-flavor rule doesn't cause a
                                # conflict with a flavor-specifying rule
                                # because the later is more specific (and
                                # we know we've already processed the
                                # flavor-specifying rule because self.rules
                                # is sorted with flavor-specifying rules
                                # at the front)
                                previousRule = redirRuleMap[(sourceName,
                                                             sourceFlavor)]
                                if (previousRule.sourceFlavor
                                                    is not None and
                                    rule.sourceFlavor is None):
                                    # the default rule should be skipped
                                    # rather than causing a conflict
                                    continue

                                if not rule.allowMultipleTargets:
                                    raise builderrors.RecipeFileError, \
                                        "Multiple redirect targets specified " \
                                        "from trove %s[%s]" \
                                        % (sourceName, sourceFlavor)

                            targetTroveInfo = (rule.destName, match[0],
                                               match[1])
                        elif not targetFlavors:
                            # redirect to nothing
                            targetTroveInfo = (None, None, None)
                        elif rule.targetFlavor is not None:
                            raise builderrors.RecipeFileError, \
                                "Trove %s does not exist for flavor [%s]" \
                                % (sourceName, targetFlavor)
                        else:
                            continue

                        # we created a redirect!
                        foundMatch = True
                        redirRuleMap[(sourceName, sourceFlavor)] = rule

                        self._buildRedirect(trvCsDict, sourceFlavor,
                                            sourceVersion, rule,
                                            targetTroveInfo)


                if not foundMatch:
                    raise builderrors.CookError(
                    "Could not find target with satisfying flavor"
                    " for redirect %s - either create a redirect"
                    " with targetFlavor and sourceFlavor set, or"
                    " create a redirect with skipTargetMatching = True" % sourceName)

    def getRedirections(self):
        return self.redirections

    def __init__(self, repos, cfg, branch, flavor, extraMacros={}):
        Recipe.__init__(self, cfg)
        self.repos = repos
        self.redirections = _Redirections()
        self.flavor = flavor
        if branch is not None:
            self.macros.binarybranch = str(branch)
            self.macros.update(extraMacros)
            # this allows override of binarybranch externally (used by rMake)
            self.branch = versions.VersionFromString(self.macros.binarybranch)
        else:
            self.branch = None
        self.rules = {}

exec defaultrecipes.RedirectRecipe
