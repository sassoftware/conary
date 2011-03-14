# -*- mode: python -*-
#
# Copyright (c) 2004-2011 rPath, Inc.
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
