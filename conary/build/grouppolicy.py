#
# Copyright (c) 2008-2010 rPath, Inc.
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

import re

from conary.build import packagepolicy, policy, trovefilter
from conary.build.errors import GroupPathConflicts

class PathConflicts(policy.GroupEnforcementPolicy):
    '''
    NAME
    ====

    B{C{r.PathConflicts()}} - Raises errors when paths conflict

    SYNOPSIS
    ========

    C{r.PathConflicts(exceptions='/path/regexp/.*')}
    C{r.PathConflicts(exceptions=['/regexp/.*', '/other/foo.*'])}
    C{r.PathConflicts(exceptions=r.troveFilter('foo:runtime'))}
    C{foo = r.troveFilter('foo:runtime')}
    C{bar = r.troveFilter('bar:runtime')}
    C{r.PathConflicts(exceptions=[foo, bar])}

    DESCRIPTION
    ===========

    Raises a GroupPathConflict error describing path conflicts,
    except for path conflicts specifically provided as exceptions.

    Exceptions may be provided either by regular expressions matched
    against paths, or as C{troveFilter}s matched against the troves
    containing the paths.

    Paths removed from conflict handling are not reported in any
    errors.

    If two troves conflict, and only one of the troves has been
    listed as an exception, no path conflict error is raised.  A
    path conflict error is raised only if there is more than one
    trove that is B{not} listed as an exception to this C{PathConflicts}
    policy.
    '''
    def __init__(self, *args, **kwargs):
        policy.GroupEnforcementPolicy.__init__(self, *args, **kwargs)
        self.pathExceptions = set()
        self.troveExceptions = set()

    def updateArgs(self, *args, **kwargs):
        if '_groupsWithConflicts' in kwargs:
            self.groupsWithConflicts = kwargs.pop('_groupsWithConflicts')
        exceptions = kwargs.pop('exceptions', [])
        if isinstance(exceptions, (str, trovefilter.TroveFilter)):
            exceptions = [ exceptions ]
        for exception in exceptions:
            if isinstance(exception, str):
                self.pathExceptions.add(re.compile(exception))
            else:
                self.troveExceptions.add(exception)

    def do(self):
        conflicts = {}
        for group in self.groupsWithConflicts.keys():
            for trvs, paths in self.groupsWithConflicts[group]:
                isConflict = True
                for exception in self.pathExceptions:
                    failPaths = [p for p in paths if not exception.match(p)]
                    if len(failPaths) == 0:
                        # all these paths are excepted
                        isConflict = False
                        continue
                    if len(failPaths) != len(paths):
                        # don't print out paths that have been ignored
                        paths = failPaths
                if isConflict:
                    for exception in self.troveExceptions:
                        failTrvs = [t for t in trvs if not exception.match([t])]
                        if len(failTrvs) <= 1:
                            # there is still a conflict after ignoring troves
                            # only if there is more than one trove matched
                            isConflict = False
                            continue
                        if len(failTrvs) != len(trvs):
                            # don't print out troves that have been ignored
                            trvs = failTrvs
                if isConflict:
                    l = conflicts.setdefault(group, [])
                    l.append((trvs, paths))
            
        if conflicts:
            raise GroupPathConflicts(conflicts,
                self.recipe.getGroupDict())

_DatabaseDepCache = packagepolicy._DatabaseDepCache
class reportErrors(packagepolicy.reportErrors):
    groupError = True
