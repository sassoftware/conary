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
        return str(self.action)

    def beenRealized(self):
        self.realized = True

    def realize(self):
        self.action()
        self.beenRealized()

class SearchSourceTroveSet(TroveSet):

    def _findTroves(self, troveTuple):
        return self.searchSource.findTroves(troveTuple, requireLatest = True)

    def __init__(self, searchSource, graph = graph):
        TroveSet.__init__(self, graph = graph)
        self.realized = True
        self.searchSource = searchSource

class Action(object):

    def __str__(self):
        return self.__class__.__name__[:-6]

class DelayedTupleSetAction(Action):

    resultClass = DelayedTupleSet

    def __init__(self, primaryTroveSet):
        self.primaryTroveSet = primaryTroveSet

    def getResultTupleSet(self, graph = None):
        self.outSet = self.resultClass(action = self, graph = graph)
        return self.outSet

class ParallelAction(DelayedTupleSetAction):

    pass

class FindAction(ParallelAction):

    def __init__(self, primaryTroveSet, *troveSpecs):
        ParallelAction.__init__(self, primaryTroveSet)
        self.troveSpecs = troveSpecs

    def __call__(self, actionList):
        troveSpecsByInSet = {}
        for action in actionList:
            l = troveSpecsByInSet.setdefault(action.primaryTroveSet, [])
            from conary.conaryclient.cmdline import parseTroveSpec
            l.extend([ (action.outSet, parseTroveSpec(troveSpec))
                            for troveSpec in action.troveSpecs ] )

        for inSet, searchList in troveSpecsByInSet.iteritems():
            d = inSet._findTroves([ x[1] for x in searchList ])
            for outSet, troveSpec in searchList:
                outSet.addTuples(d[troveSpec])

    def __str__(self):
        n1 = self.troveSpecs[0].split('=')[0]
        n2 = self.troveSpecs[-1].split('=')[0]

        if len(self.troveSpecs) == 1:
            s =  n1
        elif len(self.troveSpecs) == 2:
            s =  n1 + r' ,\n' + n2
        else:
            s =  n1 + r' ...\n' + n2

        return r'Find\n' + s

class UnionAction(DelayedTupleSetAction):

    def __init__(self, primaryTroveSet, *args):
        DelayedTupleSetAction.__init__(self, primaryTroveSet)
        self.troveSets = [ primaryTroveSet ] + list(args)

    def __call__(self):
        for troveSet in self.troveSets:
            self.outSet.addTuples(troveSet.l)

class OperationGraph(graph.DirectedGraph):

    def realize(self):
        transpose = self.transpose()
        ordering = self.getTotalOrdering()

        while True:
            # grab as many bits as we can whose parents have been realized
            layer = []
            needWork = False
            for node in ordering:
                if node.realized: continue

                needWork = True
                parents = transpose.getChildren(node)
                if len([ x for x in parents if x.realized ]) == len(parents):
                    layer.append(node)

            if not needWork:
                assert(not layer)
                break

            assert(layer)
            byAction = {}

            for node in layer:
                if not node.realized:
                    if isinstance(node, DelayedTupleSet):
                        byAction.setdefault(
                            node.action.__class__, []).append(node)
                    else:
                        node.realize(self.getParents(node), node)

            for action, nodeList in byAction.iteritems():
                if issubclass(action, ParallelAction):
                    nodeList[0].action([ node.action for node in nodeList ])
                    for node in nodeList:
                        node.beenRealized()
                else:
                    for node in nodeList:
                        node.realize()

