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


from conary import errors as baseerrors
from conary.deps.deps import parseFlavor
from conary_test import rephelp
from conary.repository import errors
from conary.repository import searchsource
from conary import versions
from conary.versions import Label
from conary import trove

class SearchSourceTest(rephelp.RepositoryHelper):

    def testNetworkSearchSource(self):
        repos = self.openRepository()
        trv1 = self.addComponent('foo:runtime', '1', 'ssl')
        trv2 = self.addComponent('foo:runtime', '2', '!ssl')

        s = searchsource.NetworkSearchSource(repos, self.cfg.installLabelPath,
                                             parseFlavor('ssl'))
        tup = s.findTrove(('foo:runtime', None, None))[0]
        assert(tup == trv1.getNameVersionFlavor())
        tup = s.findTrove(('foo:runtime', None, parseFlavor('!ssl')))[0]
        assert(tup == trv2.getNameVersionFlavor())
        trv = s.getTrove(*tup)
        assert(trv == trv2)
        cs = s.createChangeSet([(tup[0], (None, None), (tup[1], tup[2]), True)])
        trvCs = cs.getNewTroveVersion(*trv.getNameVersionFlavor())
        trv = trove.Trove(trvCs)
        assert(trv == trv2)
        assert(list(s.iterFilesInTrove(*trv.getNameVersionFlavor()))
               == list(repos.iterFilesInTrove(*trv.getNameVersionFlavor())))

        # test dep resolution - the resolve source for this should check
        # the right branch.
        self.cfg.installLabelPath = [versions.Label('localhost@rpl:branch')]
        self.addComponent('bar:runtime', ':branch/1', filePrimer=1, 
                          requires='trove:foo:runtime')
        self.checkUpdate('bar:runtime', ['bar:runtime', 'foo:runtime=1'],
                          resolveSource=s.getResolveMethod(), resolve=True)

    def testTroveSearchSource(self):
        repos = self.openRepository()
        trv1 = self.addComponent('foo:runtime', ':branch/1', '!ssl')
        trv2 = self.addComponent('bar:runtime', ':branch/1', 'ssl',
                                 filePrimer=1)
        s = searchsource.TroveSearchSource(repos, [trv1, trv2],
                                           parseFlavor('!ssl'))
        tup = s.findTrove(('foo:runtime', None, None))[0]
        assert(tup == trv1.getNameVersionFlavor())
        self.assertRaises(errors.TroveNotFound,
                          s.findTrove, ('bar:runtime', None, None))
        tup = s.findTrove(('bar:runtime', None, parseFlavor('ssl')))[0]
        assert(tup == trv2.getNameVersionFlavor())

        self.addComponent('bam:runtime', 1, filePrimer=2, 
                          requires='trove:foo:runtime')
        # make sure we resolve in foo:runtime even though it 
        # a) isn't on the label and b) has an incompatible flavor with the
        # update flavor.
        self.checkUpdate('bam:runtime', ['bam:runtime', 'foo:runtime'],
                          resolveSource=s.getResolveMethod(), resolve=True)

    def testSearchSourceStack(self):
        foo1 = self.addComponent('foo:runtime', ':branch/1')
        foo2 = self.addComponent('foo:runtime', ':branch2/1', filePrimer=1)
        bar1 = self.addComponent('bar:runtime', ':branch2/1', filePrimer=2)
        heh = self.addComponent('heh:runtime', ':3/1', filePrimer=3)
        gd = self.addCollection('group-dist', ':branch2/1',
                                ['foo:runtime', 'bar:runtime'])

        ss = self.getSearchSource()
        ss = searchsource.createSearchSourceStackFromStrings(ss,
                                ['localhost@rpl:branch', 'group-dist=:branch2',
                                 'localhost@rpl:3'],
                                self.cfg.flavor)
        tup = ss.findTrove(('foo:runtime', None, None))
        assert(tup == [foo1.getNameVersionFlavor()])
        tup = ss.findTrove(('bar:runtime', None, None))
        assert(tup == [bar1.getNameVersionFlavor()])
        tup = ss.findTrove(('group-dist', None, None))
        assert(tup == [gd.getNameVersionFlavor()])
        tup = ss.findTrove(('heh:runtime', None, None))
        assert(tup == [heh.getNameVersionFlavor()])

        self.addComponent('bam:runtime', '1', filePrimer=3, 
                          requires='trove:foo:runtime trove:bar:runtime')
        self.checkUpdate('bam:runtime', ['bam:runtime', 'foo:runtime=:branch',
                                         'bar:runtime=:branch2'],
                          resolveSource=ss.getResolveMethod(), resolve=True)


    def testCreateSearchSourceStack(self):
        ss = self.getSearchSource()
        path = searchsource.createSearchPathFromStrings(['foo', 'conary.rpath.com@rpl:1', 'foo=:1', 'foo=@rpl:1', 'foo=conary.rpath.com@rpl:1'])
        assert(path == ((('foo', None, None),),
                        (Label('conary.rpath.com@rpl:1'),),
                        (('foo', ':1', None),),
                        (('foo', '@rpl:1', None),),
                        (('foo', 'conary.rpath.com@rpl:1', None),)))
        self.assertRaises(baseerrors.ParseError,
                        searchsource.createSearchPathFromStrings, ['/f!oo@'])
        self.assertRaises(baseerrors.ParseError,
                        searchsource.createSearchPathFromStrings, ['foo=='])
        trv = self.addComponent('foo:run')
        stack = searchsource.createSearchSourceStackFromStrings(ss, ['foo:run'],
                                                                self.cfg.flavor,
                                                                fallBackToRepos=False)
        assert(len(stack.sources) == 1)
        assert(isinstance(stack.sources[0], searchsource.TroveSearchSource))

        stack = searchsource.createSearchSourceStack(ss, [trv], self.cfg.flavor,
                                                     fallBackToRepos=False)
        assert(len(stack.sources) == 1)
        assert(isinstance(stack.sources[0], searchsource.TroveSearchSource))

        stack = searchsource.createSearchSourceStackFromStrings(ss,
                    ['localhost@rpl:1', 'localhost@rpl:1', 'localhost@rpl:3'],
                    self.cfg.flavor, fallBackToRepos=False)
        assert(len(stack.sources) == 1)
        assert(isinstance(stack.sources[0], searchsource.NetworkSearchSource))
        assert(len(stack.sources[0].installLabelPath) == 3)

        stack = searchsource.createSearchSourceStackFromStrings(ss,
                                                    [self.cfg.installLabelPath],
                                                    self.cfg.flavor,
                                                    fallBackToRepos=False)
        assert(stack.sources[0].installLabelPath == tuple(self.cfg.installLabelPath))
        stack = searchsource.createSearchSourceStackFromStrings(ss,
                                                self.cfg.installLabelPath[0],
                                                self.cfg.flavor,
                                                fallBackToRepos=False)
        assert(stack.sources[0].installLabelPath == tuple(self.cfg.installLabelPath))

        self.assertRaises(baseerrors.ParseError,
                        searchsource.createSearchPathFromStrings, [None])
        self.assertRaises(baseerrors.ParseError,
                          searchsource.createSearchSourceStack, ss, [None],
                                                               self.cfg.flavor)

    def testSearchSourceStackSearchesOtherLabelsFirst(self):
        ss = self.getSearchSource()
        self.addComponent('foo:runtime=:branch/1-1-1')
        self.addComponent('foo:runtime=:branch/1-1-2')
        self.addComponent('foo:runtime=:branch/2-1-1')
        self.addComponent('foo:runtime=:linux/3-1-1')
        self.addComponent('foo:runtime=:linux/3-1-2')
        self.addComponent('foo:runtime=:linux/4-1-1')
        stack = searchsource.createSearchSourceStackFromStrings(ss,
                    ['localhost@rpl:1', 'foo:runtime=:branch/1-1-1'],
                    self.cfg.flavor)
        def _find(verStr):
            troveSpec = ('foo:runtime', verStr, None)
            results = stack.findTroves([troveSpec])
            assert(len(results[troveSpec]) == 1)
            return str(results[troveSpec][0][1].trailingRevision())

        assert(_find('localhost@rpl:branch') == '1-1-1')
        assert(_find('localhost@rpl:branch/1') == '1-1-1')
        assert(_find('localhost@rpl:branch/2') == '2-1-1')
        assert(_find('localhost@rpl:branch/1-1-2') == '1-1-2')
        assert(_find('@rpl:branch') == '1-1-1')
        assert(_find('@rpl:branch/1') == '1-1-1')
        assert(_find('@rpl:branch/1-1-1') == '1-1-1')
        assert(_find('@rpl:branch/1-1-2') == '1-1-2')
        assert(_find('@rpl:branch/2') == '2-1-1')
        assert(_find(':branch') == '1-1-1')
        assert(_find(':branch/1') == '1-1-1')
        assert(_find(':branch/2') == '2-1-1')
        assert(_find(':branch/1-1-1') == '1-1-1')
        assert(_find(':branch/1-1-2') == '1-1-2')
        assert(_find('localhost@') == '1-1-1')
        assert(_find('/localhost@rpl:branch') == '1-1-1')
        assert(_find('/localhost@rpl:branch/1-1-1') == '1-1-1')
        assert(_find('/localhost@rpl:branch/1-1-2') == '1-1-2')
        assert(_find('/localhost@rpl:branch/2-1-1') == '2-1-1')
        assert(_find(versions.VersionFromString('/localhost@rpl:branch')) == '1-1-1')
        assert(_find(versions.VersionFromString('/localhost@rpl:branch/1-1-1')) == '1-1-1')
        assert(_find(versions.VersionFromString('/localhost@rpl:linux')) == '4-1-1')
        assert(_find(versions.VersionFromString('/localhost@rpl:linux/4-1-1')) == '4-1-1')
        assert(_find('localhost@rpl:linux') == '4-1-1')

        try:
            _find('/localhost@rpl:branch//')
            assert(0)
        except errors.TroveNotFound, msg:
            assert(str(msg) == 'Error parsing version "/localhost@rpl:branch//": branch tag may not be empty')

        # test dep resolution.  Should pull in foo:runtime from the trove stack
        # not from the repository.
        self.addComponent('bam:runtime=1', requires='trove: foo:runtime')
        self.checkUpdate('bam:runtime', ['bam:runtime', 'foo:runtime=:branch/1'],
                          resolveSource=stack.getResolveMethod(), resolve=True)

        self.addComponent('foo:runtime=:1/5-1-1')
        self.addComponent('foo:runtime=:1/5-1-2')
        self.addComponent('foo:runtime=:1/6-1-1')
        assert(_find('localhost@rpl:1') == '6-1-1')
        assert(_find('localhost@rpl:1/5') == '5-1-2')
        assert(_find('@rpl:1') == '6-1-1')
        assert(_find('@rpl:1/5') == '5-1-2')
        assert(_find('@rpl:1/5-1-1') == '5-1-1')
        assert(_find(':1') == '6-1-1')
        assert(_find(':1/5') == '5-1-2')
        assert(_find('localhost@') == '6-1-1')
        assert(_find('localhost@rpl:1') == '6-1-1')
        assert(_find('localhost@rpl:1/5') == '5-1-2')
        assert(_find('localhost@rpl:1/5-1-1') == '5-1-1')
        assert(_find('/localhost@rpl:1') == '6-1-1')
        assert(_find('/localhost@rpl:1/5-1-1') == '5-1-1')
        assert(_find('/localhost@rpl:1/6-1-1') == '6-1-1')

        self.addComponent('foo:runtime=localhost@rpl:foo/1-1-1')
        self.addComponent('foo:runtime=localhost@bam:foo/2-1-1')
        # NOTE: stack is redefined here - this changes the behavior of _find.
        stack = searchsource.createSearchSourceStackFromStrings(ss,
                                [ 'localhost@bam:bar', 'localhost@rpl:foo'],
                                self.cfg.flavor)
        # localhost@rpl:foo is actually on the ILP, it should be found
        # before localhost@bam:foo which isn't on the ILP.
        assert(_find(':foo') == '1-1-1')
        # but we still fall back to the repository
        assert(_find(':foo/2') == '2-1-1')

    def testSearchSourceStackHarder(self):
        ss = self.getSearchSource()
        self.addComponent('foo:runtime=:branch/1-1-1', 
                           provides='trove:foo:runtime(bar)')
        self.addComponent('foo:runtime=:branch/1-1-2')
        self.addComponent('bar:runtime=:branch/1-1-1', 
                           provides='trove:bar:runtime(bar)')
        self.addComponent('bar:runtime=:branch/1-1-2')

        self.addComponent('foo:runtime=:linux/1-1-1', 
                           provides='trove:foo:runtime(bar)')
        self.addComponent('bam:runtime=:linux',
                requires='trove: foo:runtime(bar)  trove:bar:runtime(bar)')
        stack = searchsource.createSearchSourceStackFromStrings(ss,
                    ['localhost@rpl:branch', 'foo:runtime=:linux'],
                    self.cfg.flavor)
        self.checkUpdate('bam:runtime', ['bam:runtime',
                                         'foo:runtime=:linux',
                                         'bar:runtime=:branch/1-1-1'],
                          resolveSource=stack.getResolveMethod(), resolve=True)
