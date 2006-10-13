#
# Copyright (c) 2005-2006 rPath, Inc.
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
from conary.errors import CvcError

class RecipeFileError(CvcError):
    pass

class CookError(CvcError):
    pass

class LoadRecipeError(RecipeFileError):
    pass

class RecipeDependencyError(RecipeFileError):
    pass

class BadRecipeNameError(RecipeFileError):
    pass

class GroupPathConflicts(CookError):
    def __init__(self, conflicts, groupDict):
        self.conflicts = conflicts
        self.groupDict = groupDict
        errStrings = []
        for groupName, conflictSets in conflicts.iteritems():
            group = groupDict[groupName]
            errStrings.append('%s:' % groupName)
            for conflictSet, paths in conflictSets:
                errStrings.append('  The following %s troves share %s conflicting paths:' % (len(conflictSet), len(paths)))
                errStrings.append('\n    Troves:')
                for (n,v,f) in conflictSet:
                    incReason = group.getReasonString(n,v,f)
                    errStrings.append('     %s=%s[%s]\n       (%s)' % (n,v,f,incReason))
                errStrings.append('\n    Conflicting Files:')
                errStrings.extend('      %s' % x for x in sorted(paths)[0:11])
                if len(paths) > 10:
                    errStrings.append('      ... (%s more)' % (len(paths) - 10))
                errStrings.append('')
            
        self.args = """
The following troves in the following groups have conflicts:

%s""" % ('\n'.join(errStrings))

class GroupDependencyFailure(CookError):
    def __init__(self, groupName, failedDeps):
        lns = ["Dependency failure\n"]
        lns.append("Group %s has unresolved dependencies:" % groupName)
        for (name, depSet) in failedDeps:
            lns.append("\n" + name[0])
            lns.append('\n\t')
            lns.append("\n\t".join(str(depSet).split("\n")))
        self.args = (''.join(lns),)


class GroupCyclesError(CookError):
    def __init__(self, cycles):
        lns = ['cycle in groups:']
        lns.extend(str(sorted(x)) for x in cycles)
        self.args = ('\n  '.join(lns),)

class GroupAddAllError(CookError):
    def __init__(self, parentGroup, troveTup, groupTups ):
        groupNames = [ x[0] for x in groupTups ]
        repeatedGroups = sorted(set(x for x in groupNames \
                                                if groupNames.count(x) > 1))

        repeatedGroups = "'" + "', '".join(repeatedGroups) + "'"

        lns = ['Cannot recursively addAll from group "%s":' % troveTup[0]]
        lns.append('Multiple groups with the same name(s) %s' % repeatedGroups)
        lns.append('are included.')
            
        self.args = ('\n  '.join(lns),)

class GroupImplicitReplaceError(CookError):
    def __init__(self, parentGroup, troveTups):
        lns = ['Cannot replace the following troves in %s:\n\n' % parentGroup.name]
        for troveTup in troveTups:
            lns.append('   %s=%s[%s]\n' % troveTup)
            lns.append('   (%s)\n' % parentGroup.getReasonString(*troveTup))
        lns.append('\nYou are not building the containing group, so conary does not know where to add the replacement.\n')
        lns.append('To resolve this problem, use r.addCopy for the containing group instead of r.add.\n')
        self.args = (''.join(lns),)

class _UnmatchedSpecs(CookError):
    def __init__(self, msg, troveSpecs):
        lns = [msg]
        for troveSpec in troveSpecs:
            ver = flavor = ''
            if troveSpec[1]:
                ver = '=%s' % troveSpec[1]
            if troveSpec[2] is not None:
                flavor = '[%s]' % troveSpec[2]
            lns.append('    %s%s%s\n' % (troveSpec[0], ver, flavor))
        self.args = (''.join(lns),)

class GroupUnmatchedRemoves(_UnmatchedSpecs):
    def __init__(self, troveSpecs, group):
        msg = 'Could not find troves to remove in %s:\n' % group.name
        _UnmatchedSpecs.__init__(self, msg, troveSpecs)

class GroupUnmatchedReplaces(_UnmatchedSpecs):
    def __init__(self, troveSpecs, group):
        msg = 'Could not find troves to replace in %s:\n' % group.name
        _UnmatchedSpecs.__init__(self, msg, troveSpecs)

class GroupUnmatchedGlobalReplaces(_UnmatchedSpecs):
    def __init__(self, troveSpecs):
        msg = 'Could not find troves to replace in any group:\n'
        _UnmatchedSpecs.__init__(self, msg, troveSpecs)

class MacroKeyError(KeyError):
    def __str__(self):
        return 'Unknown macro "%s" - check for spelling mistakes' % self.args[0]

class MirrorError(CvcError):
    pass

