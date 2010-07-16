#
# Copyright (c) 2010 rPath, Inc.
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
from conary.lib import graph

class TroveSet(object):

    def __init__(self, graph = None):
        self.realized = False
        self.g = graph

    def __str__(self):
        return self.__class__.__name__

    def _action(self, *args, **kwargs):
        ActionClass = kwargs.pop('ActionClass')
        action = ActionClass(self, *args, **kwargs)
        troveSet = action.getResultTupleSet(graph = self.g)

        self.g.addNode(troveSet)
        self.g.addEdge(self, troveSet, value = None)

        for arg in itertools.chain(args, kwargs.itervalues()):
            if isinstance(arg, TroveSet):
                self.g.addEdge(arg, troveSet, value = None)

        return troveSet

class TroveTupleSet(TroveSet):

    def addTuples(self, l):
        self.l.extend(l)

    def __init__(self, *args, **kwargs):
        TroveSet.__init__(self, *args, **kwargs)
        self.l = []

class DelayedTupleSet(TroveTupleSet):

    def __init__(self, graph = None, action = None):
        assert(graph)
        assert(action)
        TroveTupleSet.__init__(self, graph = graph)
        self.action = action

    def __str__(self):
        return self.action.__class__.__name__[:-6]

    def beenRealized(self):
        self.realized = True

    def realize(self):
        self.action()
        self.beenRealized()

class SearchSourceTroveSet(TroveSet):

    def _find(self, troveTuple):
        return self.searchSource.findTrove(troveTuple, requireLatest = True)

    def __init__(self, searchSource, graph = graph):
        TroveSet.__init__(self, graph = graph)
        self.realized = True
        self.searchSource = searchSource

class Action(object):

    pass

class DelayedTupleSetAction(Action):

    resultClass = DelayedTupleSet

    def __init__(self, primaryTroveSet):
        self.primaryTroveSet = primaryTroveSet

    def getResultTupleSet(self, graph = None):
        self.outSet = self.resultClass(action = self, graph = graph)
        return self.outSet

class FindAction(DelayedTupleSetAction):

    def __init__(self, primaryTroveSet, troveSpec):
        DelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.troveSpec = troveSpec

    def __call__(self):
        from conary.conaryclient.cmdline import parseTroveSpec
        self.outSet.addTuples(
            self.primaryTroveSet._find(parseTroveSpec(self.troveSpec)))

class UnionAction(DelayedTupleSetAction):

    def __init__(self, primaryTroveSet, *args):
        DelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.troveSets = [ primaryTroveSet ] + list(args)

    def __call__(self):
        for troveSet in self.troveSets:
            self.outSet.addTuples(troveSet.l)

class OperationGraph(graph.DirectedGraph):

    def realize(self):
        ordering = self.getTotalOrdering()
        for node in ordering:
            if not node.realized:
                node.realize()

