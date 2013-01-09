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


import itertools

from conary.conaryclient import cml, modelupdate
from conary.lib import log
from conary.repository import trovecache

class ModelData(object):

    def search(self, item):
        import re
        l = []
        for troveTup in self.troveTups:
            if re.match(item, troveTup[0]):
                l.append(troveTup)

        return sorted(l)

    def __init__(self, client):
        log.debug("loading system model cache");

        troveCache = trovecache.TroveCache(None)
        troveCache.load(client.cfg.dbPath + '/modelcache')

        model = cml.CML(client.cfg)
        troveSet = client.cmlGraph(model)
        troveSet.g.realize(modelupdate.CMLActionData(troveCache,
                                              client.cfg.flavor[0],
                                              client.getRepos(), client.cfg))

        self.troveTups = set()
        for withFiles, trv in troveCache.cache.values():
            for nvf in trv.iterTroveList(strongRefs = True, weakRefs = True):
                self.troveTups.add(nvf)


def globMap(s):
    r = ""
    for ch in s:
        if ch == '*':
            r += '[^:]*'
        elif ch in '[]^+':
            r += '\\' + ch
        else:
            r += ch
    return '^' + r + '$'

def search(client, searchArgs):
    data = ModelData(client)
    db = client.getDatabase()
    for arg in searchArgs:
        matches = data.search(globMap(arg))
        for trvTuple, present in itertools.izip(matches, db.hasTroves(matches)):
            installed = (present and "installed") or "available"
            print "%s=%s[%s] (%s)" % (trvTuple + (installed,))
