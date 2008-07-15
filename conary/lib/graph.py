#
# Copyright (c) 2006-2008 rPath, Inc.
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
"""General graph algorithms"""
import copy
import itertools
from collections import deque

class BackEdgeError(Exception):
    def __init__(self, src, dst, *args, **kwargs):
        self.src = src
        self.dst = dst
        Exception.__init__(self, *args, **kwargs)

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

    def __contains__(self, item):
        return item in self.data

    def get(self, index):
        return self.data[index]

    def iterNodes(self):
        return iter(self.data)

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

    def iterNodes(self):
        return iter(self.hashedData)

    def sort(self, sortAlg=None):
        return sorted(((x[1], x[0]) for x in self.hashedData.iteritems()), 
                      sortAlg)

    def copy(self):
        new = self.__class__()
        new.data = list(self.data)
        new.hashedData = self.hashedData.copy()
        return new

    def __contains__(self, node):
        return node in self.hashedData

    def getIndex(self, item):
        if item not in self.hashedData:
            idx = self.index
            self.hashedData[item] = idx
            self.data.append(item)
            self.index += 1
        else:
            idx = self.hashedData[item]
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
        if nodeId not in self.edges:
            self.edges[nodeId] = {}
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

    def addEdges(self, edgeList):
        getIndex = self.data.getIndex
        edges = self.edges

        for fromItem, toItem, value in edgeList:
            fromIdx = getIndex(fromItem)
            toIdx = getIndex(toItem)
            if fromIdx not in edges:
                edges[fromIdx] = {toIdx:value}
            else:
                edges[fromIdx][toIdx] = value
            if toIdx not in edges:
                edges[toIdx] = {}

    def delete(self, item):
        idx = self.data.getIndex(item)
        self.data.delete(item)
        self.edges.pop(idx, 0)
        [ x.pop(idx, 0) for x in self.edges.itervalues() ]

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
            if fromId not in newEdges:
                newEdges[fromId] = []
            for toId, value in toIdList.iteritems():
                if toId not in newEdges:
                    newEdges[toId] = [(fromId, value)]
                else:
                    newEdges[toId].append((fromId, value))
        return dict((x[0], dict(x[1])) for x in newEdges.iteritems())

    def iterChildren(self, node, withEdges=False):
        if withEdges:
            return ((self.data.get(x[0]), x[1])
                    for x in self.edges[self.data.getIndex(node)].iteritems())
        return (self.data.get(idx)
                    for idx in self.edges[self.data.getIndex(node)])

    def __contains__(self, node):
        return node in self.data

    def getIndex(self, node):
        return self.data.getIndex(node)

    def iterNodes(self):
        return self.data.iterNodes()

    def iterEdges(self):
        get = self.data.get
        for fromId, toIdList in self.edges.iteritems():
            fromNode = get(fromId)
            for toId in toIdList:
                yield fromNode, get(toId)

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

    def getDisconnected(self):
        # gets nodes with neither edges pointing in or out
        disconnected = set(x[0] for x in self.edges.iteritems() if not x[1])
        for edges in self.edges.itervalues():
            disconnected.difference_update(edges)
            if not disconnected:
                break
        return self.data.getItemsByIndex(disconnected)

    def transpose(self):
        g = DirectedGraph()
        g.data = self.data.copy()
        g.edges = self.getReversedEdges()
        return g

    def doDFS(self, start=None, nodeSort=None, finishCallback=None,
              depthLimit = None):
        # DFS doesn't need the predecessor data structure (although the tree
        # data structure could be derived from it)
        starts, finishes, trees, pred, depth = self._walk(start=start,
                nodeSort=nodeSort, finishCallback=finishCallback,
                depthLimit = depthLimit, dfs = True)
        return starts, finishes, trees

    def doBFS(self, start=None, nodeSort=None, finishCallback=None,
              depthLimit = None, getChildrenCallback = None):
        return self._walk(start=start, nodeSort=nodeSort,
                finishCallback=finishCallback,
                getChildrenCallback = getChildrenCallback,
                depthLimit = depthLimit, dfs = False)

    def _walk(self, start=None, nodeSort=None, finishCallback=None,
                    getChildrenCallback = None, depthLimit = None,
                    dfs=True):
        nodeData = self.data

        nodeIds = [ x[0] for x in nodeData.sort(nodeSort) ]

 
        trees = {}
        starts = {}
        finishes = {}
        pred = {}
        depth = {}
        timeCount = 0
        parent = None
        nodeStruct = deque()
        if dfs:
            popFn = nodeStruct.pop
        else:
            popFn = nodeStruct.popleft
        pushFn = nodeStruct.append

        if start is not None:
            if not isinstance(start, list):
                start = [start]
            if dfs:
                start.reverse()
            for s in start:
                startId = nodeData.getIndex(s)
                nodeIds.remove(startId)
                nodeIds.insert(0, startId)

        if getChildrenCallback is None:
            getChildrenCallback = self.edges.__getitem__

        # Only have to reverse the order of the nodes for DFS
        reverse = bool(dfs)

        while nodeIds:
            if not nodeStruct:
                # with BFS we really don't want to explore the whole graph,
                # expecially if we do depth limiting searches
                if not dfs and depthLimit is not None and trees:
                    break
                nodeId = nodeIds.pop(0)
                if nodeId in starts:
                    continue
                nodeStruct.clear()
                pushFn((nodeId, None, False))
                parent = nodeId
                trees[nodeId] = []
                depth[nodeId] = 0

            while nodeStruct:
                nodeId, predNode, finish = popFn()
                if finish:
                    finishes[nodeId] = timeCount
                    if finishCallback:
                        finishCallback(nodeId, starts, finishes)
                    timeCount += 1
                    continue
                elif nodeId in starts:
                    continue

                if predNode is not None:
                    predDepth = depth[predNode]
                    if depthLimit is not None and predDepth == depthLimit:
                        # This child node is beyond the depth limit
                        continue

                    pred[nodeId] = predNode
                    depth[nodeId] = depth[predNode] + 1
                starts[nodeId] = timeCount
                timeCount += 1

                trees[parent].append(nodeId)

                # Both in DFS and in BFS, the parent node has to be finalized
                # after all its children. Because in DFS we put the items in a
                # queue, we have to do it after we add the children.
                if dfs:
                    pushFn((nodeId, None, True))

                childNodes = [x[0] for x in nodeData.sortSubset(
                                            getChildrenCallback(nodeId),
                                            nodeSort, reverse=reverse)]
                for childNodeId in childNodes:
                    if childNodeId not in starts:
                        pushFn((childNodeId, nodeId, False))

                if not dfs:
                    pushFn((nodeId, None, True))

        return starts, finishes, trees, pred, depth

    def getTotalOrdering(self, nodeSort=None):
        """
            Note: children are ordered after their parents. 
        """
        # to sort correctly, we need the nodes the user wants first to 
        # be picked _last_ by the selection algorithm.  That way they'll
        # have the latest possible finish times, and score better in the
        # nodeSelect below.
        if nodeSort:
            reversedSort = lambda a,b: -nodeSort(a,b)
        else:
            reversedSort = None

        # Accumulate elements in finishList as they are finished.
        finishList = []
        def finishCallback(nodeId, starts, finishes):
            finishList.append(nodeId)

        starts, finishes, trees = self.doDFS(nodeSort=reversedSort,
                                             finishCallback=finishCallback)

        # finishList is sorted by finish times, for a total ordering we need
        # it reversed
        finishList.reverse()

        # Find back edges (two adjacent nodes u, v have a back edge iff
        # starts[u] > starts[v] and finishes[u] < finishes[v]
        for fromIdx, toIdxList in self.edges.iteritems():
            for toIdx in toIdxList:
                if starts[fromIdx] > starts[toIdx] and \
                   finishes[fromIdx] < finishes[toIdx]:
                   # Back edge
                   src, dst = self.data.getItemsByIndex([fromIdx, toIdx])
                   raise BackEdgeError(src, dst)

        return self.data.getItemsByIndex(finishList)

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
        edges = []
        addEdge = edges.append

        sccGraph = self.__class__()

        setsByNode = {}
        for compSet in compSets:
            setsByNode.update(dict.fromkeys(compSet, compSet))

        for compSet in compSets:
            sccGraph.addNode(compSet)
            for node in compSet:
                for childNode in self.iterChildren(node):
                    childComp = setsByNode[childNode]
                    if childComp is not compSet:
                        addEdge((compSet, setsByNode[childNode], 1))
        sccGraph.addEdges(edges)
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

    def generateDotFile(self, out, labelFormatFn=str, edgeFormatFn=None,
                        filterFn=None):
        """
            Generates a dot file based on the contents of the graph.
            @param out: file-like object we write to
            @param labelFormatFn: function that takes a node as a parameter
              and returns the output string
            @param edgeFormatFn: function that takes fromNode, toNode, value as 
                           parameters and returns a string for the edge.
            @param filterFn: if given, is a function that returns true if a node
            should be included in the graph.
        """
        if isinstance(out, str):
            out = open(out, 'w')
        out.write('digraph graphName {\n')
        nodes = {}
        for node in self.iterNodes():
            if not filterFn or filterFn(node):
                idx = self.data.getIndex(node)
                nodes[idx] = node
                out.write('   n%s [label="%s"]\n' % (idx, labelFormatFn(node)))
        for fromIdx, toIdxDict in self.edges.iteritems():
            if fromIdx not in nodes:
                continue
            fromNode = nodes[fromIdx]
            for toIdx, value in toIdxDict.iteritems():
                if toIdx not in nodes:
                    continue
                out.write('   n%s -> n%s' % (fromIdx, toIdx))
                if edgeFormatFn:
                    labelStr = edgeFormatFn(fromNode,
                                         nodes[toIdx],
                                         value)
                    out.write(' [label="%s"]' % (labelStr,))
                out.write('\n')
        out.write('}\n')
