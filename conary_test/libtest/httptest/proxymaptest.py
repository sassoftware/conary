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


import copy
import pickle
from testrunner import testhelp
from conary.lib.networking import Hostname, HostGlob, HostPort
from conary.lib.http.proxy_map import DirectConnection, FilterSpec, ProxyMap
from conary.lib.http.request import URL


class ProxyMapTest(testhelp.TestCase):

    def testDeepCopy(self):
        m = ProxyMap()
        m.addStrategy('*', ['DIRECT'])
        url = 'http://example.url/'
        manglers = [
                copy.deepcopy,
                lambda x: pickle.loads(pickle.dumps(x)),
                lambda x: pickle.loads(pickle.dumps(x, 2)),
                ]
        for mangle in manglers:
            m2 = mangle(m)
            targets = list(m2.getProxyIter(url))
            self.assertEqual(targets, [DirectConnection])
            assert targets[0] is DirectConnection

    def testAddStrategy(self):
        m = ProxyMap()
        m.addStrategy('example.foo', ['http://proxy1', 'https://proxy2'])
        m.addStrategy('example.bar', ['conary://proxy3/conary/'])
        m.addStrategy('http:*', ['http://user:pass@proxy4'],
                replaceScheme='conary')
        m.addStrategy('https:*', ['https://user:pass@proxy4'],
                replaceScheme='conary')
        m.addStrategy('*', ['https://proxy5'])
        self.assertEqual(m.items(), [
            (FilterSpec(None, Hostname('example.foo')), [
                URL('http', (None, None), HostPort('proxy1', 80), ''),
                URL('https', (None, None), HostPort('proxy2', 443), ''),
                ]),
            (FilterSpec(None, Hostname('example.bar')), [
                URL('conary', (None, None), HostPort('proxy3', 80),
                    '/conary/')]),
            (FilterSpec('http', HostGlob('*')), [
                URL('conary', ('user', 'pass'), HostPort('proxy4:80'), '')]),
            (FilterSpec('https', HostGlob('*')), [
                URL('conarys', ('user', 'pass'), HostPort('proxy4:443'), '')]),
            (FilterSpec(None, HostGlob('*')), [
                URL('https', (None, None), HostPort('proxy5:443'), '')]),
            ])

    def testFilterSpec(self):
        foo = FilterSpec('*.foo')
        self.assertEqual(foo, FilterSpec(foo))
        self.assertEqual(foo, FilterSpec(str(foo)))
        bar = FilterSpec('https:*.bar')
        self.assertEqual(bar, FilterSpec(bar))
        self.assertEqual(bar, FilterSpec(str(bar)))
        assert foo.match(URL('https://example.foo/blargh'))
        assert not bar.match(URL('https://example.foo/blargh'))
        assert bar.match(URL('https://example.bar/blargh'))
        assert not bar.match(URL('conarys://example.bar/blargh'))

    def testProxyIter(self):
        m = ProxyMap()
        m.addStrategy('example.foo', ['http://proxy1', 'https://proxy2'])
        m.addStrategy('http:*', ['http://user:pass@proxy4'],
                replaceScheme='conary')
        m.addStrategy('https:*', ['https://user:pass@proxy4'],
                replaceScheme='conary')
        m.addStrategy('http:*', ['https://proxy5'])

        i = m.getProxyIter(URL('http://unrelated.foo'))
        self.assertEqual(i.next(), URL('https://proxy5'))
        self.assertRaises(StopIteration, i.next)

        m.blacklistUrl(URL('https://proxy5'))
        i = m.getProxyIter(URL('http://unrelated.foo'))
        self.assertRaises(StopIteration, i.next)

        i = m.getProxyIter(URL('https://unrelated.foo'))
        self.assertEqual(i.next(), DirectConnection)
        self.assertRaises(StopIteration, i.next)

        i = m.getProxyIter(URL('http://example.foo/bar'))
        expected = set([URL('http://proxy1'), URL('https://proxy2')])
        while expected:
            got = i.next()
            assert got in expected
            expected.remove(got)
        self.assertRaises(StopIteration, i.next)

        i = m.getProxyIter(URL('https://example.foo/bar'),
                protocolFilter=('http', 'https', 'conary', 'conarys'))
        expected = set([URL('http://proxy1'), URL('https://proxy2')])
        while expected:
            got = i.next()
            assert got in expected
            expected.remove(got)
        self.assertEqual(i.next(), URL('conarys://user:pass@proxy4'))
        self.assertRaises(StopIteration, i.next)
