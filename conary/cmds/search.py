#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
