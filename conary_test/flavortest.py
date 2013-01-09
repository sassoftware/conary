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


import os

from conary_test import rephelp
from conary_test import recipes

from conary import versions
from conary.repository.netrepos import versionops
from conary.deps import deps
from conary.local import database


class FlavorTest(rephelp.RepositoryHelper):

    def cmpQueryResult(self, d, targ):
        def _flavorCmp(a, b):
            a = str(a)
            b = str(b)
            if a < b:
                return -1
            elif a == b:
                return 0
            else:
                return 1

        for versionDict in d.itervalues():
            for flavorList in versionDict.itervalues():
                flavorList.sort(_flavorCmp)

        for versionDict in targ.itervalues():
            for flavorList in versionDict.itervalues():
                flavorList.sort(_flavorCmp)

        assert(d == targ)

    def testFlavorQueries(self):
        def _build(v, f):
            self.addComponent('manyflavors:runtime', str(v), flavor = f)
            self.addCollection('manyflavors', str(v), [ ':runtime' ],
                               defaultFlavor = f)

        v1 = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        v2 = versions.VersionFromString('/localhost@rpl:linux/1.0-1-2')
        defFlavor = deps.parseFlavor('readline,ssl')
        noReadLine = deps.parseFlavor('~!readline,ssl')

        for version, flavor in [ (v1, defFlavor), (v1, noReadLine) ]:
            _build(version, flavor)

        # put it back
        self.overrideBuildFlavor('~readline')

        buildBranch = versions.VersionFromString("/%s" %
                            self.cfg.buildLabel.asString())

        repos = self.openRepository()
        d = repos.getTroveVersionList('localhost', { None : None })
        full = { 'manyflavors': { v1 : [ defFlavor, noReadLine ] },
                 'manyflavors:runtime': { v1 : [ defFlavor, noReadLine ] } }
        self.cmpQueryResult(d, full)

        d = repos.getTroveVersionList('localhost', { None : [ defFlavor ]})
        full = { 'manyflavors': { v1 : [ defFlavor ] },
                 'manyflavors:runtime': { v1 : [ defFlavor ] } }
        self.cmpQueryResult(d, full)

        # both flavors are compatible here, so both are returned
        d = repos.getTroveVersionList('localhost', { None : [ noReadLine ] })
        full = { 'manyflavors': { v1 : [ defFlavor, noReadLine ] },
                 'manyflavors:runtime': { v1 : [ defFlavor, noReadLine ] } }
        self.cmpQueryResult(d, full)

    # this should return all of the flavors
        d = repos.getTroveLeavesByBranch(
                    { 'manyflavors' : { buildBranch : None } },
                    bestFlavor = False)
        self.cmpQueryResult(d, { 'manyflavors': 
                                    { v1 : [ defFlavor, noReadLine ] } })

    # this chooses the best flavor
        d = repos.getTroveLeavesByLabel(
                            { None : { self.cfg.buildLabel : [ defFlavor ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v1 : [ defFlavor ] },
                 'manyflavors:runtime': { v1 : [ defFlavor ] } }
        self.cmpQueryResult(d, full)

        d = repos.getTroveLatestByLabel(
                            { None : { self.cfg.buildLabel : [ defFlavor ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v1 : [ defFlavor ] },
                 'manyflavors:runtime': { v1 : [ defFlavor ] } }
        self.cmpQueryResult(d, full)

        d = repos.getTroveLeavesByLabel(
                            { None : { self.cfg.buildLabel : [ noReadLine ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v1 : [ noReadLine ] },
                 'manyflavors:runtime': { v1 : [ noReadLine ] } }
        self.cmpQueryResult(d, full)

        d = repos.getTroveLatestByLabel(
                            { None : { self.cfg.buildLabel : [ noReadLine ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v1 : [ noReadLine ] },
                 'manyflavors:runtime': { v1 : [ noReadLine ] } }
        self.cmpQueryResult(d, full)

    # now make the two branches of different lengths
        _build(v2, defFlavor)

        d = repos.getTroveLeavesByLabel(
                            { None : { self.cfg.buildLabel : [ defFlavor ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v2 : [ defFlavor ] },
                 'manyflavors:runtime': { v2 : [ defFlavor ] } }
        self.cmpQueryResult(d, full)

        # v2 is still the best match since it's not actually incompatible
        # with the noReadLine flavor
        d = repos.getTroveLeavesByLabel(
                            { None : { self.cfg.buildLabel : [ noReadLine ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v2 : [ defFlavor ] },
                 'manyflavors:runtime': { v2 : [ defFlavor ] } }
        self.cmpQueryResult(d, full)

        d = repos.getTroveLatestByLabel(
                            { None : { self.cfg.buildLabel : [ noReadLine ] } },
                            bestFlavor = True)
        full = { 'manyflavors': { v2 : [ defFlavor ] },
                 'manyflavors:runtime': { v2 : [ defFlavor ] } }
        self.cmpQueryResult(d, full)

        disallowReadLine = deps.parseFlavor(
            deps.formatFlavor(noReadLine).replace('~!readline','!readline'))

        # now that we explicitly disallow troves which use readline, we
        # should get the old version
        d = repos.getTroveLeavesByLabel(
                    { None : { self.cfg.buildLabel : [ disallowReadLine ] } },
                    bestFlavor = True)
        self.cmpQueryResult(d, 
                    { 'manyflavors': { v1 : [ noReadLine ] },
                      'manyflavors:runtime': { v1 : [ noReadLine ] } })

        d = repos.getTroveLatestByLabel(
                    { None : { self.cfg.buildLabel : [ disallowReadLine ] } },
                    bestFlavor = True)
        self.cmpQueryResult(d, 
                    { 'manyflavors': { v1 : [ noReadLine ] },
                      'manyflavors:runtime': { v1 : [ noReadLine ] } })

    def _updateFlavor(self, flavorString):
        flavor = deps.parseFlavor(flavorString)
        self.cfg.flavor[0].union(flavor, 
                                 mergeType = deps.DEP_MERGE_TYPE_OVERRIDE)

    def _checkVersion(self, vers, flavorCheck = None):
        db = database.Database(self.cfg.root, self.cfg.dbPath)
        l = db.getTroveVersionList('manyflavors')
        assert(len(l) == 1)
        assert(l[0] == vers)
        if flavorCheck is not None:
            flavors = db.getAllTroveFlavors({ 'manyflavors' : l })
            flavor = deps.parseFlavor(flavorCheck)
            assert(flavors['manyflavors'][vers] == [ flavor ])

    def testSimpleUpdates(self):
        self.resetRepository()
        self.resetRoot()
        trove = self.build(recipes.manyFlavors, "ManyFlavors")
        v1 = trove.getVersion()
        defFlavor = trove.getFlavor()
        assert(str(defFlavor) == 'readline,ssl')

        self.overrideBuildFlavor('~!readline')
        trove = self.build(recipes.manyFlavors, "ManyFlavors")
        v = trove.getVersion()
        noReadLine = trove.getFlavor()
        assert(str(noReadLine) == '~!readline,ssl')
        assert(v1 == v)

        self.overrideBuildFlavor('~readline')

    # basic test w/ readline in the flavor
        self.cfg.flavor = [ deps.parseFlavor('use: ~readline,ssl') ]
        self.updatePkg(self.rootDir, 'manyflavors')
        assert(os.path.exists(self.rootDir + '/etc/readline'))

    # now test w/ preferring not readline
        self.resetRoot()
        self._updateFlavor('use: ~!readline')
        self.updatePkg(self.rootDir, 'manyflavors')
        assert(not os.path.exists(self.rootDir + '/etc/readline'))

    # now test w/ preferring not readline, but an override
        self.resetRoot()
        self._updateFlavor('use: ~!readline')
        self.updatePkg(self.rootDir, 'manyflavors', flavor = 'use:readline,ssl')
        assert(os.path.exists(self.rootDir + '/etc/readline'))

    # now test w/ preferring not allowing readline
        self.resetRoot()
        self._updateFlavor('use: !readline')
        self.updatePkg(self.rootDir, 'manyflavors')
        assert(not os.path.exists(self.rootDir + '/etc/readline'))

        self._updateFlavor('use: readline')

        trove = self.build(recipes.manyFlavors, "ManyFlavors")
        v2 = trove.getVersion()
        assert(defFlavor == trove.getFlavor())

    # basic test w/ readline in the flavor
        self.resetRoot()
        self.updatePkg(self.rootDir, 'manyflavors', version = v1)
        assert(os.path.exists(self.rootDir + '/etc/readline'))
        self.updatePkg(self.rootDir, 'manyflavors')
        assert(os.path.exists(self.rootDir + '/etc/readline'))
        self._checkVersion(v2)

    # with ~!readline, install the old version, and then update to whatever
    # is availble (which should give us the new version, even though it
    # does use readline)
        self.resetRoot()
        self._updateFlavor('use: ~!readline')
        self.updatePkg(self.rootDir, 'manyflavors', version = v1)
        assert(not os.path.exists(self.rootDir + '/etc/readline'))
        self.updatePkg(self.rootDir, 'manyflavors')
        assert(os.path.exists(self.rootDir + '/etc/readline'))
        self._checkVersion(v2)

    # now set the system flavor to !readline. we should get v1, but trying
    # to switch to v2 will cause a "no new versions" message
        self.resetRoot()
        self._updateFlavor('use: !readline')
        self.updatePkg(self.rootDir, 'manyflavors', version = v1)
        assert(not os.path.exists(self.rootDir + '/etc/readline'))
        rc = self.logCheck(self.updatePkg, (self.rootDir, 'manyflavors'),
                           'error: no new troves were found')
        self._checkVersion(v1)
        assert(not os.path.exists(self.rootDir + '/etc/readline'))

    # if we don't specify ssl in our system flags, we should not get any
    # match at all (since everything built has ssl flavor)
        self.resetRoot()
        self.cfg.flavor = [ deps.parseFlavor('use: ~readline')]
        rc = self.logCheck(self.updatePkg, (self.rootDir, 'manyflavors'),
                   'error: manyflavors was not found on path localhost@rpl:linux (Closest alternate flavors found: [~ssl])')

    def testMixedUpdates(self):
        def _oneSet(versionStr):
            self.resetRoot()
            self.cfg.flavor = [ deps.parseFlavor('use: ~readline,~ssl') ]
            self.updatePkg(self.rootDir, 'manyflavors', version = '1.0-1-1')
            self.updatePkg(self.rootDir, 'manyflavors', version = versionStr)
            self._checkVersion(v2, 'use: readline,~!ssl')

            self.resetRoot()
            self.cfg.flavor = [ deps.parseFlavor('use: ~readline,~ssl') ]
            self.updatePkg(self.rootDir, 'manyflavors', version = '1.0-1-1')
            self.updatePkg(self.rootDir, 'manyflavors', version = versionStr)
            self._checkVersion(v2, 'use: readline,~!ssl')

            self.resetRoot()
            self.cfg.flavor = [ deps.parseFlavor('use: ~readline') ]
            self.updatePkg(self.rootDir, 'manyflavors', version = '1.0-1-1')
            self.updatePkg(self.rootDir, 'manyflavors', version = versionStr)
            self._checkVersion(v2, 'use: readline,~!ssl')

        self.overrideBuildFlavor('~!readline,~!ssl')
        trove = self.build(recipes.manyFlavors, "ManyFlavors")
        assert(str(trove.getFlavor()) == '~!readline,~!ssl')

        self.overrideBuildFlavor('~readline')
        trove = self.build(recipes.manyFlavors2, "ManyFlavors")
        v2 = trove.getVersion()
        assert(str(trove.getFlavor()) == 'readline,~!ssl')

        self.overrideBuildFlavor('~ssl')
        trove = self.build(recipes.manyFlavors2, "ManyFlavors")
        assert(trove.getVersion() == v2)
        assert(str(trove.getFlavor()) == 'readline,ssl')

        # test w/ no version specifier
        _oneSet(None)
        _oneSet('2.0-1-1')
        _oneSet(v2.asString())
        _oneSet('localhost@rpl:linux')
        _oneSet('@rpl:linux')

    def testSimultaneousInstalls(self):
        # various tests with two (nonoverlapping) flavors installed at the
        # same time
        def build():
            self.overrideBuildFlavor('~readline,~!ssl')
            trove = self.build(recipes.manyFlavors, "ManyFlavors")
            v = trove.getVersion()
            assert(str(trove.getFlavor()) == 'readline,~!ssl')

            self.overrideBuildFlavor('~!readline,~ssl')
            trove = self.build(recipes.manyFlavors, "ManyFlavors")
            assert(trove.getVersion() == v)
            assert(str(trove.getFlavor()) == '~!readline,ssl')
            return v

        v1 = build()
        v2 = build()
        
        # update two packages at the same time w/ a single specification
        self.updatePkg(self.rootDir, 'manyflavors', version = v1,
                       flavor = 'use: ssl,~!readline')
        # need to specify ~!ssl because by default the install flavor is 
        # requires ssl, which will eliminate other ssl choices
        self.updatePkg(self.rootDir, 'manyflavors', version = v1,
                       flavor = 'use: readline,~!ssl', keepExisting = True)
        assert(os.path.exists(self.rootDir + "/etc/ssl"))
        assert(os.path.exists(self.rootDir + "/etc/readline"))
        self.updatePkg(self.rootDir, 'manyflavors', checkPathConflicts = False)
        assert(os.path.exists(self.rootDir + "/etc/ssl"))
        assert(os.path.exists(self.rootDir + "/etc/readline"))
        self._checkVersion(v2)

        # and check erase can remove just one
        self.erasePkg(self.rootDir, 'manyflavors', flavor = 'use: readline is:')
        assert(os.path.exists(self.rootDir + "/etc/ssl"))
        assert(not os.path.exists(self.rootDir + "/etc/readline"))

    # update just one and make sure the other stays around, and that a
    # subsequant update gets both
        self.resetRoot()
        self.updatePkg(self.rootDir, 'manyflavors', version = v1,
                       flavor = 'use: ssl')
        self.updatePkg(self.rootDir, 'manyflavors', version = v1,
                       flavor = 'use: readline,~!ssl', keepExisting = True)
        self.updatePkg(self.rootDir, 'manyflavors', version = v2,
                       flavor = 'use: ssl')
        assert(os.path.exists(self.rootDir + "/etc/ssl"))
        assert(os.path.exists(self.rootDir + "/etc/readline"))
        self.updatePkg(self.rootDir, 'manyflavors')
        assert(os.path.exists(self.rootDir + "/etc/ssl"))
        assert(os.path.exists(self.rootDir + "/etc/readline"))
        self._checkVersion(v2)

    def testDependencies(self):
        def _checkFlavor(targ):
            db = database.Database(self.rootDir, self.cfg.dbPath)
            l = db.getTroveVersionList('bash:runtime', withFlavors = True)
            assert(str(l[0][1]) == targ)

        self.overrideBuildFlavor('ssl')
        trove = self.build(recipes.bashMissingRecipe, "Bash")
        v = trove.getVersion()
        assert(str(trove.getFlavor()) == 'ssl')
         
        self.overrideBuildFlavor('!ssl')
        trove = self.build(recipes.bashMissingRecipe, "Bash")
        v = trove.getVersion()
        assert(str(trove.getFlavor()) == '~!ssl')
         
        self.overrideBuildFlavor('ssl')
        trove = self.build(recipes.bashRecipe, "Bash")
        v = trove.getVersion()
        assert(str(trove.getFlavor()) == 'ssl')
         
        self.overrideBuildFlavor('!ssl')
        trove = self.build(recipes.bashRecipe, "Bash")
        assert(trove.getVersion() == v)

        self.build(recipes.bashUserRecipe, 'BashUser')

        self.cfg.flavor = [ deps.parseFlavor('ssl') ]
        self.captureOutput(self.updatePkg, self.rootDir, 'bashuser', 
                           resolve = True)
        _checkFlavor('ssl')

        self.resetRoot()
        self.cfg.flavor = [ deps.parseFlavor('!ssl') ]
        self.captureOutput(self.updatePkg, self.rootDir, 'bashuser', 
                           resolve = True)
        _checkFlavor('~!ssl')

        self.resetRoot()
        self.cfg.flavor = [ deps.parseFlavor('ssl') ]
        self.updatePkg(self.rootDir, 'bash:runtime', '1-1-1')
        self.captureOutput(self.updatePkg, self.rootDir, 'bashuser', 
                           resolve = True)
        _checkFlavor('ssl')

        self.resetRoot()
        self.cfg.flavor = [ deps.parseFlavor('~!ssl') ]
        self.updatePkg(self.rootDir, 'bash:runtime', '1-1-1')
        self.captureOutput(self.updatePkg, self.rootDir, 'bashuser', 
                           resolve = True)
        _checkFlavor('~!ssl')

        self.resetRoot()
        self.cfg.flavor = [ deps.parseFlavor('ssl') ]
        self.updatePkg(self.rootDir, 'bash:runtime', '0-1-1')
        self.cfg.flavor = [ deps.parseFlavor('~!ssl') ]
        self.captureOutput(self.updatePkg, self.rootDir, 'bashuser', 
                           resolve = True)
        _checkFlavor('ssl')

        self.resetRoot()
        self.cfg.flavor = [ deps.parseFlavor('~!ssl') ]
        self.updatePkg(self.rootDir, 'bash:runtime', '0-1-1')
        self.cfg.flavor = [ deps.parseFlavor('ssl') ]
        self.captureOutput(self.updatePkg, self.rootDir, 'bashuser', 
                           resolve = True)
        _checkFlavor('~!ssl')

    def testLeavesFlavor(self):
        repos = self.openRepository()
        # this is a nasty, nasty, nasty test. msw suggested it
        server = self.servers.getServer(0)

        # add permutations of these objects
        tlabel = 'localhost@rpl:linux'
        # the least desirable that would also be the "latest"
        tlast = ('/%s/9.%%d-1-1' % (tlabel,), 'use:flag1,flag2,flag3')
        tlist = [
            ('/%s/1.%%d-1-1' % (tlabel,), 'use:flag1'),
            ('/%s/2.%%d-1-1' % (tlabel,), 'use:flag1,flag2'),
            #('/%s/3.%%d-1-1' % (tlabel,), 'use:flag1,flag2,flag3'),
            tlast,
            ]
        # neither use flags makes us happy
        self.cfg.flavor = [ deps.parseFlavor('~!flag1,~!flag2,~!flag3') ]

        db = server.reposDB.connect()

        def print_db(db, tname = None):
            cu = db.cursor()                  
            args = []
            query = """
            select Items.item, Versions.version,
            Nodes.timestamps as ts, Nodes.finalTimestamp as fts
            from Nodes join Items using (itemid)
            join Versions on Nodes.versionId = Versions.versionId
            """
            if tname:
                query = query + " where Items.item = ? "
                args.append(tname)
            cu.execute(query, args)
            ret = cu.fetchall()
            print "Dataset is: (trove, version, TS, finalTS)"
            for x in ret:
                print x

        # update a node's timestamps
        def set_ts(db, tname, tversion, ts):
            cu = db.cursor()
            updq = """
            update Nodes
                set timestamps = '%s',
                finalTimeStamp = %s
            where
                Nodes.itemId in (
                    select itemId from Items where Items.item = '%s' )
                and Nodes.versionId in (
                    select versionId from Versions where Versions.version = '%s' )
            """ % (str(ts), ts, tname, tversion)
            cu.execute(updq, [] )
            db.commit()
            
        # since we're manually messing with timestamps, we need to update the Latest
        # table as well
        def fix_latest(db):
            cu = db.cursor()
            cu.execute('DELETE FROM LatestCache')
            latest = versionops.LatestTable(db)
            latest.rebuild()
            db.commit()

        # exhaustive search - generate permutations of the above
        def xcombinations(items, n):
            if n==0: yield []
            else:
                for i in xrange(len(items)):
                    for cc in xcombinations(items[:i]+items[i+1:],n-1):
                        yield [items[i]]+cc

        cnt = 0
        req = {}
        for xlist in xcombinations(range(len(tlist)), len(tlist)):            
            cnt = cnt + 1
            # each permutation gets its own trove name
            tname = "test%d:test" % (cnt,)
            for t in xlist:
                # we build increasingly larger sets of versions for the
                # chosen trove name
                for iter in range(1, cnt+1):
                    l = (tname, tlist[t][0] % (iter,), tlist[t][1])
                    self.addQuickTestComponent(*l)
                    set_ts(db, tname, l[1], t*100+iter+1)
            # make sure the "latest" has the highest timestamp
            # XXX: watch out for max number of permutations that could top
            #      the highest chosen timestamp
            set_ts(db, tname, tlast[0] % (cnt,), 200*len(tlist)+1)
            # fix the latest table manualy (we emulate 'triggers')
            fix_latest(db)
            # now see what the repo "thinks" about it
            d = repos.getTroveLeavesByLabel(
                { tname : { versions.Label(tlabel): self.cfg.flavor } },
                bestFlavor=True)
            # we only ask for one element
            assert(len(d) == 1)
            assert(d.has_key(tname))
            ret = d[tname]
            need = {versions.VersionFromString(tlast[0] % (cnt,)): [deps.parseFlavor(tlast[1])]}
            if  ret != need:
                print
                print "NEED:", need
                print "GOT :", ret
                print_db(db, tname)
            assert(ret == need)

    def testLatestByLabel(self):
        def _build(*specList):
            for (v, f) in specList:
                self.addComponent('t:rt', v, flavor = f)

        v1 = versions.ThawVersion('/localhost@rpl:foo//1/1:1.0-1-1')
        v2 = versions.ThawVersion('/localhost@rpl:bar//1/2:1.0-1-2')
        l = v1.trailingLabel()
        defFlavor = deps.parseFlavor('readline,ssl')
        noReadLine = deps.parseFlavor('~!readline,ssl')
        reqNoReadLine = deps.parseFlavor('!readline,ssl')

        _build( (v1, defFlavor), (v2, defFlavor) )

        repos = self.openRepository()

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : None } } )
        assert(d['t:rt'].keys() == [ v2 ])

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ defFlavor ] } } )
        assert(d['t:rt'].keys() == [ v2 ])

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ defFlavor ] } },
                                        bestFlavor = True)
        assert(d['t:rt'].keys() == [ v2 ])

        _build( (v1, noReadLine) )

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : None } } )
        assert(d['t:rt']) == { v1 : [ noReadLine ],
                               v2 : [ defFlavor ] }

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ noReadLine ] } },
                                         bestFlavor = True)
        assert(d['t:rt']) == { v2 : [ defFlavor ] }

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ reqNoReadLine ] } },
                                         bestFlavor = True)
        assert(d['t:rt']) == { v1 : [ noReadLine ] }

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ noReadLine ] } } )
        assert(d['t:rt']) == { v2 : [ defFlavor ], v1 : [noReadLine] }

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ defFlavor,
                                                            noReadLine ] } } )
        assert(d['t:rt']) == { v2 : [ defFlavor ], v1 : [noReadLine] }

        d = repos.getTroveLatestByLabel( { 't:rt' : { l : [ defFlavor,
                                                            reqNoReadLine ] } },
                                        bestFlavor = True)
        assert(d['t:rt']) == { v2 : [ defFlavor ], v1: [noReadLine]}
