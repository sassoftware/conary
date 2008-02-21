#
# Copyright (c) 2008 rPath, Inc.
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
Module used after group has been finalized to create the
initial packaging.  Also contains error reporting.
"""

from conary.build import policy
from conary.build import packagepolicy

_DatabaseDepCache = packagepolicy._DatabaseDepCache
class reportErrors(packagepolicy.reportErrors):
    groupError = True

class VersionConflicts(policy.ImageGroupEnforcementPolicy):
    """
    NAME
    ====

    B{C{r.VersionConflicts()}} - Prevents multiple versions of a trove from
    the same branch from being cooked into a group.

    SYNOPSIS
    ========

    C{r.VersionConflicts([I{filterexp}] || [I{exceptions=filterexp}])}

    DESCRIPTION
    ===========

    The C{r.VersionConflicts} policy enforces that two troves with different
    versions cannot come from the same branch. This situation commonly occurs
    when the current group consumes an upstream group, and they both refer to
    a trove without being explicit about the version to use.

    If an inclusion of exception is passed as a string, it will be converted
    to a trove filter object, treating the string as the name regexp. Trove
    filters can be combined with boolean algebra or bitwise logical operators.

    This policy is an image group policy. Please see GroupRecipe for more
    information.

    EXAMPLES
    ==========

    r.VersionConflicts(exceptions = 'foo')

    Any trove named exactly foo will be ignored for this policy.

    r.VersionConflicts(exceptions = 'group-compat32')

    Any trove inside of group-compat32 will be ignored for this policy.

    fooFilter = r.troveFilter('foo.*', version = 'foo.rpath.org@rpl:1')
    groupFilter = r.troveFilter('group-core')
    r.VersionConflicts(exceptions = fooFilter & groupFilter)

    Any trove that starts with foo on the foo.rpath.org@rpl:1 label in
    group-core will be ignored.

    fooFilter = r.troveFilter('foo.*', flavor = 'is: x86')
    groupFilter = r.troveFilter('group-core')
    r.VersionConflicts(exceptions = fooFilter & -groupFilter)

    Any trove in group-core that does not match fooFilter will be ignored for
    this policy. Effectively this means that foo[is: x86] will be considered,
    but no other trove in group-core.
    """
    invariantexceptions = ['kernel.*', '.*:lib', '.*:devellib', '.*:debuginfo']
    def __init__(self, *args, **kwargs):
        self.conflicts = {}
        policy.ImageGroupEnforcementPolicy.__init__(self, *args, **kwargs)

    def doTroveSet(self, troveSet):
        seen = {}
        for trovePath, byDefault, isStrong in troveSet:
            nvf = trovePath[-1]
            if ":" not in nvf[0]:
                # we have to skip packages because they're always present if a
                # component is. if we don't we'll flag excluded components.
                continue
            pkgName = nvf[0].split(':')[0]
            pkgPath = trovePath[:-1]
            id = (pkgName, nvf[1].trailingLabel())
            if id in seen:
                otherPaths = seen[id]
                for otherPath in otherPaths:
                    otherNvf = otherPath[-1]
                    if otherNvf[1] != nvf[1]:
                        existingConflicts = self.conflicts.setdefault(id, [])
                        if otherPath not in existingConflicts:
                            existingConflicts.append(otherPath)
                        if pkgPath not in existingConflicts:
                            existingConflicts.append(trovePath)
            else:
                seen[id] = []
            seen[id].append(trovePath)

    def postProcess(self):
        if self.conflicts:
            allTroves = set()
            for id, paths in self.conflicts.iteritems():
                errorMessage = \
                        "Multiple versions of %s from %s were found:\n\n" % id
                for path in paths:
                    errorMessage += self.formatTrovePath(path) + '\n'
                    allTroves.add(path[-1][0])
                self.recipe.reportErrors(errorMessage[:-1])
            errorMessage = "Multiple versions of these troves were found:"
            errorMessage += '\n' + '\n'.join(sorted(allTroves))
            self.recipe.reportErrors(errorMessage)
