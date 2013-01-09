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


import os

class DirectorySet:

    """
    Tracks a set of directories by the shortest parent which has been included.
    If /a/c and /a/b are in the set, once /a is added both disappear as they
    are under /a.
    """

    def _split(self, dirName):
        i = dirName.find('/', 1)
        if i == -1:
            topDir = dirName
            rest = ''
        else:
            topDir = dirName[:i]
            rest = dirName[i:]

        return topDir, rest

    def add(self, dirName):
        topDir, rest = self._split(dirName)

        if rest:
            next = self.dirs.get(topDir, None)
            if next is True:
                # we already have the parent
                pass
            elif next is None:
                next = DirectorySet()
                self.dirs[topDir] = next
                next.add(rest)
            else:
                next.add(rest)
        else:
            next = self.dirs.get(topDir, None)
            if next is not True:
                self.dirs[topDir] = True

    def __iter__(self):
        for dirName, val in sorted(self.dirs.iteritems()):
            if val is True:
                yield dirName
            else:
                for s in val:
                    yield dirName + s

    def __contains__(self, dirName):
        topDir, rest = self._split(dirName)
        val = self.dirs.get(topDir, None)
        if val is True:
            return True
        elif val:
            return rest in val
        else:
            return False

    def __init__(self, members = []):
        self.dirs = {}
        for x in members:
            self.add(x)

class DirectoryDict(dict):

    def itertops(self):
        s = DirectorySet(self.keys())
        for x in s:
            yield x

    def _find(self, item):
        # returns (False, None) is the item was not found, (True, data) if
        # it is found
        dirName = item
        while dirName:
            if dict.__contains__(self, dirName):
                return True, dict.__getitem__(self, dirName)

            if dirName == '/':
                dirName = ''
            else:
                dirName = os.path.dirname(dirName)

        return False, None

    def __contains__(self, item):
        exists, val = self._find(item)
        return exists

    def __getitem__(self, item):
        exists, val = self._find(item)
        if not exists:
            raise KeyError

        return val

    def get(self, item, default):
        exists, val = self._find(item)
        if not exists:
            return default

        return val
