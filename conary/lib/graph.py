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
        new.data = list(self.data)
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

    def copy(self):
        new = NodeData.copy(self)
        new.hashedData = self.hashedData.copy()
        return new

    def getIndex(self, item):
        idx = self.hashedData.setdefault(item, self.index)
        if idx == self.index:
            self.index += 1
            self.data.append(item)
        return idx
       
            
class DirectedGraph:
    def __init__(self, dataSearchMethod=NodeDataByHash):
        self.data = dataSearchMethod()
        self.edges = {}

    def addNode(self, item):
        nodeId = self.data.getIndex(item)
        self.edges[nodeId] = set()
        return nodeId

    def get(self, idx):
        return self.data.get(idx)

    def addEdge(self, fromItem, toItem):
        fromIdx, toIdx = (self.data.getIndex(fromItem), 
                          self.data.getIndex(toItem))
        self.edges[fromIdx].add(toIdx)

    def getReversedEdges(self):
        newEdges = {}
        for fromId, toIdList in self.edges.iteritems():
            newEdges.setdefault(fromId, [])
            for toId in toIdList:
                newEdges.setdefault(toId, []).append(fromId)
        return dict((x[0], set(x[1])) for x in newEdges.iteritems())

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

        if start is not None:
            startId = nodeData.getIndex(start)
            nodeIds.remove(startId)

            nodeStack = [(startId,False)]
            nodeId = startId
            parent = nodeId
            trees[nodeId] = []
        else:
            nodeStack = []

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
        starts, finishes, trees = self.doDFS(nodeSort=nodeSort)

        def nodeSelect(a, b):
            return cmp(finishes[b[0]], finishes[a[0]])

        return [ x[1] for x in self.data.sort(nodeSelect)]

    def getStronglyConnected(self):
        starts, finishes, trees = self.doDFS()
        t = self.transpose()

        def nodeSelect(a, b):
            return cmp(finishes[b[0]], finishes[b[0]])

        finishesByTime = sorted(finishes.iteritems(), key=lambda x: x[1])
        starts, finished, trees = t.doDFS(
                                        start=self.get(finishesByTime[-1][0]), 
                                        nodeSort=nodeSelect)
        treeKeys = [ x[0] for x in self.data.sortSubset(trees.iterkeys(), 
                                                        nodeSelect) ]
        return [ set(trees[x]) for x in treeKeys ]
