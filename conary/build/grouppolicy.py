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
        for grpTrv in self.recipe.troveMap.values():
            groupName = grpTrv.getNameVersionFlavor()[0]
            if groupName not in self.groupsWithConflicts.keys():
                continue

            for trvs, paths in self.groupsWithConflicts[groupName]:
                isConflict = True
                for exception in self.pathExceptions:
                    failPaths = [p for p in paths if not exception.match(p)]
                    if len(failPaths) == 0:
                        # all these paths are excepted
                        for path in paths:
                            grpTrv.troveInfo.pathConflicts.append(path)
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
                    l = conflicts.setdefault(groupName, [])
                    l.append((trvs, paths))
            
        if conflicts:
            raise GroupPathConflicts(conflicts,
                self.recipe.getGroupDict())

_DatabaseDepCache = packagepolicy._DatabaseDepCache
class reportErrors(packagepolicy.reportErrors):
    groupError = True
