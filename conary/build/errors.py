#
# Copyright (c) 2005-2006 rPath, Inc.
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
from conary.errors import CvcError

class RecipeFileError(CvcError):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
    
class BuildError(RecipeFileError):
    pass

class LoadRecipeError(RecipeFileError):
    pass

class RecipeDependencyError(RecipeFileError):
    pass

class BadRecipeNameError(RecipeFileError):
    pass

class GroupBuildError(RecipeFileError):
    pass

class GroupPathConflicts(GroupBuildError):
    def __init__(self, conflicts):
        self.conflicts = conflicts
        errStrings = []
        for groupName, conflictSets in conflicts.iteritems():
            errStrings.append('%s:' % groupName)
            for conflictSet in conflictSets:
                errStrings.append('The following %s troves have conflicting paths:' % (len(conflictSet),))
                errStrings.extend('    %s=%s[%s]' % x for x in conflictSet)
            
        self.msg = """
The following troves in the following groups have conflicts:

%s""" % ('\n'.join(errStrings))

class GroupDependencyFailure(GroupBuildError):
    def __init__(self, groupName, failedDeps):
        lns = ["Dependency failure\n"]
        lns.append("Group %s has unresolved dependencies:" % groupName)
        for (name, depSet) in failedDeps:
            lns.append("\n" + name[0])
            lns.append('\n\t')
            lns.append("\n\t".join(str(depSet).split("\n")))
        self.msg = ''.join(lns)


class GroupCyclesError(GroupBuildError):
    def __init__(self, cycles):
        lns = ['cycle in groups:']
        lns.extend(str(sorted(x)) for x in cycles)
        self.msg = '\n  '.join(lns)

class GroupAddAllError(GroupBuildError):
    def __init__(self, parentGroup, troveTup, groupTups ):
        groupNames = [ x[0] for x in groupTups ]
        repeatedGroups = sorted(set(x for x in groupNames \
                                                if groupNames.count(x) > 1))

        repeatedGroups = "'" + "', '".join(repeatedGroups) + "'"

        lns = ['Cannot recursively addAll from group "%s":' % troveTup[0]]
        lns.append('Multiple groups with the same name(s) %s' % repeatedGroups)
        lns.append('are included.')
            
        self.msg = '\n  '.join(lns)
