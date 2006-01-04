#
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

class BuildError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
    
class RecipeFileError(BuildError):
    pass


class RecipeDependencyError(RecipeFileError):
    pass

class BadRecipeNameError(RecipeFileError):
    pass

class GroupPathConflicts(BuildError):
    def __init__(self, conflicts):
        self.conflicts = conflicts
        errStrings = []
        for groupName, conflictSets in conflicts.iteritems():
            errStrings.append(groupName)
            for conflictSet in conflictSets:
                errStrings.append('The following %s troves have conflicting paths:' % (groupName, len(conflictSet)))
                errStrings.extend('    %s=%s[%s]' % x for x in conflictSet)
            
        self.msg = """
The following troves in the following groups have conflicts:

%s""" % ('\n'.join(errStrings))
