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

import fnmatch
from conary.lib.cfgtypes import CfgType, ParseError, Path


class GlobListType(list):

    def __delitem__(self, key):
        raise NotImplementedError

    def __init__(self, *args):
        list.__init__(self, *args)
        self.matchCache = set()

    def __getstate__(self):
        return list(self)

    def __setstate__(self, state):
        self += state

    def __contains__(self, item):
        if item in self.matchCache:
            return True

        for glob in self:
            if fnmatch.fnmatch(item, glob):
                self.matchCache.add(item)
                return True

        return False


class CfgContentStore(CfgType):
    FLAT = 'flat'
    SHALLOW = 'shallow'
    LEGACY = 'legacy'
    STORE_TYPES = [
            FLAT,
            SHALLOW,
            LEGACY,
            ]

    def parseString(self, val):
        paths = val.split()
        if '/' not in paths[0]:
            storeType = paths.pop(0)
        else:
            storeType = self.LEGACY
        if storeType not in self.STORE_TYPES:
            raise ParseError("Invalid content store type %r. Valid "
                    "types are: %s" % (storeType, ' '.join(self.STORE_TYPES)))
        return (storeType, [Path(x) for x in paths])

    def format(self, val, displayOptions=None):
        storeType, paths = val
        return ' '.join(str(x) for x in [storeType] + list(paths))

