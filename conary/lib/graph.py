#
# Copyright (c) 2006 rPath, Inc.
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
"""General graph algorithms"""
import copy
import itertools

class NodeData(object):
    """Stores data associated with nodes.  Subclasses can determine
       faster ways to retrieve the index from a data object
    """

    __slots__ = ['data', 'index']

    def __init__(self):
        self.index = 0
        self.data = []

    def copy(self):
        new = self.__class__()
        new.data = copy.copy(self.data)
        new.index = self.index
        return new

    def get(self, index):
        return self.data[index]

    def sort(self, sortAlg=None):
        return sorted(enumerate(self.data), sortAlg)

    def sortSubset(self, indexes, sortAlg=None, reverse=False):
        data = self.data
        return sorted(((x, data[x]) for x in indexes), sortAlg,
                      reverse=reverse)
 
    def getItemsByIndex(self, indexes):
        return [self.data[x] for x in indexes]

    def getIndex(self, item):
        try:
            return self.data.index(item)
        except ValueError:
            self.data.append(item)
            idx = self.index
            self.index += 1
            return idx


class NodeDataByHash(NodeData):
    """ Stores node data indexed by hash for faster retrieval"""
    __slots__ = ['hashedData']

    def __init__(self):
        NodeData.__init__(self)
        self.hashedData = {}
        self.data = []

    def sort(self, sortAlg=None):
        return sorted(((x[1], x[0]) for x in self.hashedData.iteritems()), 
                      sortAlg)

    def copy(self):
        new = self.__class__()
        new.data = list(self.data)
        new.hashedData = self.hashedData.copy()
        return new

    def getIndex(self, item):
        idx = self.hashedData.setdefault(item, self.index)
        if idx == self.index:
            self.data.append(item)
            self.index += 1
        return idx

    def isEmpty(self):
        return not self.hashedData

    def delete(self, item):
        idx = self.hashedData.pop(item)
        # we can't delete from self.data, since that array position is how
        # things are indexed.
        self.data[idx] = None

class DirectedGraph:
    def __init__(self, dataSearchMethod=NodeDataByHash):
        self.data = dataSearchMethod()
        self.edges = {}

    def addNode(self, item):
        nodeId = self.data.getIndex(item)
        self.edges.setdefault(nodeId, {})
        return nodeId

    def isEmpty(self):
        return self.data.isEmpty()

    def get(self, idx):
        return self.data.get(idx)

    def addEdge(self, fromItem, toItem, value=1):
        fromIdx, toIdx = (self.data.getIndex(fromItem), 
                          self.data.getIndex(toItem))
        self.edges.setdefault(fromIdx, {})[toIdx] = value
        self.edges.setdefault(toIdx, {})

    def getEdge(self, fromItem, toItem):
        return self.edges[fromIdx, toIdx]

    def delete(self, item):
        idx = self.data.getIndex(item)
        self.data.delete(item)
        del self.edges[idx]
        [ x.pop(idx, None) for x in self.edges.itervalues() ]

    def deleteEdges(self, item):
        self.edges[self.data.getIndex(item)] = {}

    def getChildren(self, item, withEdges=False):
        idx = self.data.getIndex(item)
        children = self.data.getItemsByIndex(self.edges[idx])
        if withEdges:
            return itertools.izip(children, self.edges[idx].itervalues())
        else:
            return children

    def getReversedEdges(self):
        newEdges = {}
        for fromId, toIdList in self.edges.iteritems():
            newEdges.setdefault(fromId, [])
            for toId, value in toIdList.iteritems():
                newEdges.setdefault(toId, []).append((fromId, value))
        return dict((x[0], dict(x[1])) for x in newEdges.iteritems())

    def iterChildren(self, node):
        return (self.data.get(idx) 
                    for idx in self.edges[self.data.getIndex(node)])

    def getParents(self, node, withEdges=False):
        idx = self.data.getIndex(node)
        if withEdges:
            return [ (self.data.get(x[0]), x[1][idx])
                        for x in self.edges.iteritems() if idx in x[1] ]
        else:
            return [self.data.get(x) for x in self.edges if idx in self.edges[x]]

    def getLeaves(self):
        return [ self.data.get(x[0])
                    for x in self.edges.iteritems() if not x[1] ]


    def transpose(self):
        g = DirectedGraph()
        g.data = self.data.copy()
        g.edges = self.getReversedEdges()
        return g

    def doDFS(self, start=None, nodeSort=None):
        nodeData = self.data

        nodeIds = [ x[0] for x in nodeData.sort(nodeSort) ]

        trees = {}
        starts = {}
        finishes = {}
        timeCount = 0
        parent = None
        nodeStack = []

        if start is not None:
            startId = nodeData.getIndex(start)
            nodeIds.remove(startId)
            nodeIds.insert(0, startId)

        while nodeIds:
            if not nodeStack:
                nodeId = nodeIds.pop(0)
                if nodeId in starts:
                    continue
                nodeStack = [(nodeId, False)]
                parent = nodeId
                trees[nodeId] = []

            while nodeStack:
                nodeId, finish = nodeStack.pop()
                if finish:
                    finishes[nodeId] = timeCount
                    timeCount += 1
                    continue
                elif nodeId in starts:
                    continue

                starts[nodeId] = timeCount
                timeCount += 1

                trees[parent].append(nodeId)

                nodeStack.append((nodeId, True))
                childNodes = [x[0] for x in nodeData.sortSubset(
                                            self.edges[nodeId], nodeSort,
                                            reverse=True)]
                for childNodeId in childNodes:
                    if childNodeId not in starts:
                        nodeStack.append((childNodeId, False))

        return starts, finishes, trees

    def getTotalOrdering(self, nodeSort=None):
        # to sort correctly, we need the nodes the user wants first to 
        # be picked _last_ by the selection algorithm.  That way they'll
        # have the latest possible finish times, and score better in the
        # nodeSelect below.
        if nodeSort:
            reversedSort = lambda a,b: -nodeSort(a,b)
        else:
            reversedSort = None

        starts, finishes, trees = self.doDFS(nodeSort=reversedSort)

        def nodeSelect(a, b):
            return cmp(finishes[b[0]], finishes[a[0]])

        return [ x[1] for x in self.data.sort(nodeSelect)]

    def getStronglyConnectedComponents(self):
        if self.isEmpty():
            return []

        starts, finishes, trees = self.doDFS()
        t = self.transpose()

        def nodeSelect(a, b):
            return cmp(finishes[b[0]], finishes[a[0]])

        finishesByTime = sorted(finishes.iteritems(), key=lambda x: x[1])
        starts, finished, trees = t.doDFS(
                                        start=self.get(finishesByTime[-1][0]), 
                                        nodeSort=nodeSelect)
        treeKeys = [ x[0] for x in self.data.sortSubset(trees.iterkeys(), 
                                                        nodeSelect) ]
        return [ set(self.get(y) for y in trees[x]) for x in treeKeys ]

    def getStronglyConnectedGraph(self):
        compSets = self.getStronglyConnectedComponents()

        sccGraph = self.__class__()

        setsByNode = {}

        for compSet in compSets:
            for node in compSet:
                setsByNode[node] = frozenset(compSet)

        for compSet in compSets:
            compSet = frozenset(compSet)
            sccGraph.addNode(compSet)
            for node in compSet:
                for childNode in self.iterChildren(node):
                    childComp = setsByNode[childNode]
                    if childComp != compSet:
                        sccGraph.addEdge(compSet, setsByNode[childNode])
        return sccGraph

    def flatten(self):
        start, finished, trees = self.doDFS()
        for node in self.edges.keys():
            seen = set()
            children = set(self.edges.get(node, set()))
            while children:
                child = children.pop()
                if child in seen:
                    continue
                children.update(self.edges.get(child, []))
                self.edges[node].update(self.edges.get(child, []))
                seen.add(child)
