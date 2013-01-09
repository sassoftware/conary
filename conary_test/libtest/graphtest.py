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


from testrunner import testhelp

from StringIO import StringIO

#conary
from conary.lib import graph

#test

class GraphTest(testhelp.TestCase):

    def testDFS(self):
        g = graph.DirectedGraph()
        a = g.addNode('a')
        b = g.addNode('b')
        c = g.addNode('c')
        d = g.addNode('d')
        g.addEdge('a','b')
        g.addEdge('b','c')
        g.addEdge('c','b')
        g.addEdge('c','d')
        starts, finishes, trees = g.doDFS(start='a')
        assert(max(finishes.values()) == finishes[a])
        assert(min(finishes.values()) == finishes[d])
        assert(min(starts.values()) == starts[a])
        assert(max(starts.values()) == starts[d])
        assert(len(trees) == 1)

        starts, finishes, trees = g.doDFS(start='b')
        assert(max(finishes.values()) == finishes[a])
        assert(min(finishes.values()) == finishes[d])

        assert(min(starts.values()) == starts[b])
        assert(max(starts.values()) == starts[a])
        assert(len(trees) == 2)
        assert(len(trees[a]) == 1)
        assert(len(trees[b]) == 3)

    def testBFS(self):
        g = graph.DirectedGraph()
        a = g.addNode('a')
        b = g.addNode('b')
        c = g.addNode('c')
        d = g.addNode('d')
        g.addEdge('a','b')
        g.addEdge('b','c')
        g.addEdge('c','b')
        g.addEdge('c','d')
        starts, finishes, trees, pred, depth = g.doBFS(start='a')
        self.assertEqual([ starts[x] for x in [ a, b, c, d ] ],
            [0, 1, 3, 5])
        self.assertEqual([ finishes[x] for x in [ a, b, c, d ] ],
            [2, 4, 6, 7])
        assert(len(trees) == 1)
        self.assertEqual(depth[a], 0)
        self.assertEqual(depth[b], 1)
        self.assertEqual(depth[c], 2)
        self.assertEqual(depth[d], 3)

        starts, finishes, trees, pred, depth = g.doBFS(start='b')
        self.assertEqual([ starts[x] for x in [ a, b, c, d ] ],
            [6, 0, 1, 3])
        self.assertEqual([ finishes[x] for x in [ a, b, c, d ] ],
            [7, 2, 4, 5])

        assert(len(trees) == 2)
        assert(len(trees[a]) == 1)
        assert(len(trees[b]) == 3)
        self.assertEqual(depth[a], 0)
        self.assertEqual(depth[b], 0)
        self.assertEqual(depth[c], 1)
        self.assertEqual(depth[d], 2)

    def testDynamicBFS(self):
        # Dynamic graphs (the graph structure is not known in advance)
        g = graph.DirectedGraph()
        a = g.addNode('a')

        initialized = {}
        def getChildrenCallback(nodeIdx):
            node = g.get(nodeIdx)
            if nodeIdx not in initialized:
                if node == 'a':
                    for toIdx in ['b', 'c', 'd']:
                        g.addEdge(node, toIdx)
                elif node == 'b':
                    for toIdx in ['d', 'e', 'f']:
                        g.addEdge(node, toIdx)
                elif node in [ 'c', 'd' ]:
                    for toIdx in ['g', 'h']:
                        g.addEdge(node, toIdx)
                elif node == 'e':
                    for toIdx in ['i']:
                        g.addEdge(node, toIdx)
                elif node == 'i':
                    for toIdx in ['j']:
                        g.addEdge(node, toIdx)
                elif node == 'j':
                    for toIdx in ['k']:
                        g.addEdge(node, toIdx)
                initialized[nodeIdx] = True
            return g.edges[nodeIdx]

        starts, finishes, trees, pred, depth = g.doBFS(start='a',
            getChildrenCallback = getChildrenCallback)

        self.assertTrue(len([ x for x in g.iterNodes()]), 13)
        self.assertFalse(g.getIndex('a') in pred)
        self.assertEqual(pred[g.getIndex('b')], g.getIndex('a'))
        self.assertEqual(pred[g.getIndex('c')], g.getIndex('a'))
        self.assertEqual(pred[g.getIndex('d')], g.getIndex('a'))

        self.assertEqual(pred[g.getIndex('e')], g.getIndex('b'))
        self.assertEqual(pred[g.getIndex('f')], g.getIndex('b'))

        self.assertEqual(pred[g.getIndex('g')], g.getIndex('c'))
        self.assertEqual(pred[g.getIndex('h')], g.getIndex('c'))

        self.assertEqual(pred[g.getIndex('i')], g.getIndex('e'))
        self.assertEqual(pred[g.getIndex('j')], g.getIndex('i'))
        self.assertEqual(pred[g.getIndex('k')], g.getIndex('j'))

        self.assertEqual(depth[g.getIndex('a')], 0)
        self.assertEqual(depth[g.getIndex('b')], 1)
        self.assertEqual(depth[g.getIndex('c')], 1)
        for i in ['e', 'f', 'g', 'h']:
            self.assertEqual(depth[g.getIndex(i)], 2)
        self.assertEqual(depth[g.getIndex('i')], 3)
        self.assertEqual(depth[g.getIndex('j')], 4)
        self.assertEqual(depth[g.getIndex('k')], 5)

        # Same thing, but limit the depth
        initialized.clear()
        starts, finishes, trees, pred, depth = g.doBFS(start='a',
            getChildrenCallback = getChildrenCallback, depthLimit = 3)
        self.assertEqual(len(trees), 1)

        self.assertEqual(depth[g.getIndex('a')], 0)
        self.assertEqual(depth[g.getIndex('b')], 1)
        self.assertEqual(depth[g.getIndex('c')], 1)
        for i in ['e', 'f', 'g', 'h']:
            self.assertEqual(depth[g.getIndex(i)], 2)
        self.assertEqual(depth[g.getIndex('i')], 3)
        self.assertFalse(g.getIndex('j') in pred)
        self.assertFalse(g.getIndex('k') in pred)

    def testSCC(self):
        g = graph.DirectedGraph()
        a = g.addNode('a')
        b = g.addNode('b')
        c = g.addNode('c')
        d = g.addNode('d')

        g.addEdge('a', 'b')
        g.addEdge('b', 'c')
        g.addEdge('c', 'b')
        g.addEdge('c', 'd')
        components = g.getStronglyConnectedComponents()
        assert(components == [set(['a']), set(['b', 'c']), set(['d'])])

        g.addEdge('d', 'a')

        components = g.getStronglyConnectedComponents()
        assert(components == [set(['a', 'b', 'c', 'd'])])

    def testTotalOrdering(self):
        g = graph.DirectedGraph()
        a = g.addNode('a')
        b = g.addNode('b')
        c = g.addNode('c')
        d = g.addNode('d')
        d = g.addNode('e')

        g.addEdge('a', 'b')
        g.addEdge('a', 'c')
        g.addEdge('a', 'd')
        g.addEdge('a', 'e')
        g.addEdge('b', 'e')
        g.addEdge('c', 'e')
        g.addEdge('d', 'e')

        def nodeSort(a, b):
            return cmp(ord(a[1]), ord(b[1]))

        assert(g.getTotalOrdering(nodeSort) == ['a', 'b', 'c', 'd', 'e'])

        # add back edge
        g.addNode('f')
        g.addEdge('e', 'f')
        g.addEdge('f', 'a')
        self.assertRaises(graph.BackEdgeError, g.getTotalOrdering, nodeSort)

        g.delete('f')

        g.delete('d')
        assert(g.getTotalOrdering(nodeSort) == ['a', 'b', 'c', 'e'])
        g.delete('a')
        assert(g.getTotalOrdering(nodeSort) == ['b', 'c', 'e'])
        g.delete('c')
        assert(g.getTotalOrdering(nodeSort) == ['b', 'e'])
        g.delete('e')
        assert(g.getTotalOrdering(nodeSort) == ['b'])
        assert(not g.isEmpty())
        g.delete('b')
        assert(g.getTotalOrdering(nodeSort) == [])
        assert(g.isEmpty())


    def testFlatten(self):
        g = graph.DirectedGraph()
        a = g.addNode('a')
        b = g.addNode('b')
        c = g.addNode('c')
        d = g.addNode('d')
        d = g.addNode('e')

        g.addEdge('a', 'b')
        g.addEdge('a', 'c')
        g.addEdge('a', 'd')
        g.addEdge('a', 'e')
        g.addEdge('b', 'e')
        g.addEdge('c', 'e')
        g.addEdge('d', 'e')

        g.flatten()

        assert(sorted(g.iterChildren('a')) == ['b', 'c', 'd', 'e'])
        assert(sorted(g.iterChildren('b')) == ['e'])
        assert(sorted(g.iterChildren('c')) == ['e'])
        assert(sorted(g.iterChildren('d')) == ['e'])
        assert(sorted(g.iterChildren('e')) == [])

    def testGetDisconnected(self):
        g = graph.DirectedGraph()
        g.addNode('a')
        assert(sorted(g.getDisconnected()) == ['a'])
        g.addNode('b')
        assert(sorted(g.getDisconnected()) == ['a', 'b'])
        g.addEdge('a', 'b')
        assert(sorted(g.getDisconnected()) == [])

        g.addNode('c')
        g.addNode('d')
        assert(sorted(g.getDisconnected()) == ['c', 'd'])
        g.addEdge('a', 'c')
        assert(sorted(g.getDisconnected()) == ['d'])

    def testCreateDotFile(self):
        g = graph.DirectedGraph()
        s = StringIO()

        g.addNode('a')
        g.addNode('b')
        g.addEdge('a', 'b')
        g.generateDotFile(s)
        s.seek(0)
        self.assertEquals(s.read(), """\
digraph graphName {
   n0 [label="a"]
   n1 [label="b"]
   n0 -> n1
}
""")
        s = StringIO()
        g.generateDotFile(s, lambda x: 'Node %s' % x, 
                            lambda fromNode, toNode, value: '%s -> %s: %s' % (fromNode, toNode, value))

        s.seek(0)
        self.assertEquals(s.read(), """\
digraph graphName {
   n0 [label="Node a"]
   n1 [label="Node b"]
   n0 -> n1 [label="a -> b: 1"]
}
""")
