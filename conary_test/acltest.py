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
import cgi
import itertools
import os
from SimpleHTTPServer import SimpleHTTPRequestHandler

from testrunner import testhelp
from testutils import sock_utils
from conary_test import recipes
from conary_test.auth_helper import AuthHelper

from conary import conarycfg, versions, trove
from conary.build import use
from conary.deps import deps
from conary.lib import httputils
from conary.repository import errors, netclient
from conary.server.server import HTTPServer


class AclTest(AuthHelper):
    def testAddAcls(self):
        # test calling addAcls in various forms
        label = versions.Label("localhost@rpl:linux")
        self.openRepository()
        repos = self.getRepositoryClient()

        # add simple users
        self.addUserAndRole(repos, label, "user1", "pw1")
        repos.addAcl(label, "user1", "ALL", label)
        repos.setRoleCanMirror(label, "user1", True)
        repos.setRoleCanMirror(label, "user1", False)

        self.addUserAndRole(repos, label, "user2", "pw2")
        repos.addAcl(label, "user2", "ALL", label, write=True)
        repos.setRoleCanMirror(label, "user2", True)
        repos.setRoleCanMirror(label, "user2", False)

        self.addUserAndRole(repos, label, "user3", "pw3")
        repos.addAcl(label, "user3", "ALL", label, write=True)
        repos.addAcl(label, "user3", ".*:source", label, write=True,
                     remove=True)
        repos.setRoleIsAdmin(label, 'user2', True)

    @testhelp.context('entitlements')
    def testBasicAcls(self):
        # limitedRepos can only see .*:runtime on localhost@rpl:linux
        # branchRepos can only see localhost@rpl:branch
        # runtimeRepos can only see .*:runtime
        # repeatRepos has multiple acl's which grant permission to
        #    double:.* and .*:runtime
        rootLabel = versions.Label("localhost@rpl:linux")
        branchLabel = versions.Label("localhost@rpl:branch")

        rootBranch = versions.VersionFromString('/localhost@rpl:linux')

        self.makeSourceTrove('double', recipes.doubleRecipe1)
        p = self.build(recipes.doubleRecipe1, "Double")

        repos = self.getRepositoryClient()

        limitedRepos = self.setupUser(repos, rootLabel, 'limited', 'bar',
                                      '.*:runtime', branchLabel)

        # branch is done as an entitlement instead of as a user/password
        # remove the anonymous user, require entitlement
        repos.deleteUserByName(rootLabel, 'anonymous')
        branchRepos = self.setupEntitlement(repos, "entGroup", "12345", 
                                            rootLabel, None, branchLabel,
                                            withClass = True)[0]
        runtimeRepos = self.setupUser(repos, rootLabel, 'runtime', 'bar',
                                      '.*:runtime', None)
        repeatRepos = self.setupUser(repos, rootLabel, 'repeat', 'bar',
                                      '.*:runtime', None)
        repos.addAcl(rootLabel, 'repeat', 'double:runtime', None, False, False)

        # add a user/role for troveAccess *only* permissions
        self.addUserAndRole(repos, rootLabel, 'ta', 'ta')
        taRepos = self.getRepositoryClient(user='ta', password='ta')
        repos.addTroveAccess('ta', [ p.getNameVersionFlavor() ])

        both = [ 'double', 'double:runtime', 'double:source' ]
        runtime = [ 'double:runtime' ]

    # troveNames
        # the sets here make it order independent
        assert(set(repos.troveNames(rootLabel)) ==
               set(both))
        assert(limitedRepos.troveNames(rootLabel) == [])
        assert(branchRepos.troveNames(rootLabel) == [])
        assert(runtimeRepos.troveNames(rootLabel) == runtime)
        assert(repeatRepos.troveNames(rootLabel)==  runtime)
        assert(taRepos.troveNames(rootLabel) == runtime)

        self.mkbranch(self.cfg.buildLabel, branchLabel, 'double:source')
        branchVersion = versions.VersionFromString(
                                '/localhost@rpl:linux/1.0-1-0/branch')

        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = branchLabel
        #self.build(recipes.doubleRecipe1, "Double")
        self.updateSourceTrove('double', recipes.doubleRecipe1_1)
        double1_1 = self.build(recipes.doubleRecipe1_1, "Double")
        self.cfg.buildLabel = oldLabel

        repos.addTroveAccess('ta', [ double1_1.getNameVersionFlavor() ])

        assert({}.fromkeys(repos.troveNames(branchLabel)) == 
               {}.fromkeys(both))
        assert(limitedRepos.troveNames(branchLabel) == runtime)
        assert({}.fromkeys(branchRepos.troveNames(branchLabel)) == 
               {}.fromkeys(both))
        assert(runtimeRepos.troveNames(branchLabel) == runtime)
        assert(taRepos.troveNames(rootLabel) == runtime)

    # getTroveVersionList
        full = { 'double':         
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                              '/localhost@rpl:linux/1.0-1-1',
                            ], 
                 'double:runtime': 
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                              '/localhost@rpl:linux/1.0-1-1',
                            ],
                 'double:source': 
                            [ 
                              '/localhost@rpl:linux/1.0-1',
                              '/localhost@rpl:linux/1.0-1/branch/1',
                              '/localhost@rpl:linux/1.0-1/branch/1.1-1',
                            ],

            }
        d = repos.getTroveVersionList('localhost', 
                        { 'double' : None, 
                          'double:source' : None,
                          'double:runtime' : None } )
        self.cmpTroveVersionList(d, full)
        d = repos.getTroveVersionList('localhost', { None : None })
        self.cmpTroveVersionList(d, full)

        d = limitedRepos.getTroveVersionList('localhost', 
                        { 'double' : None, 
                          'double:runtime' : None } )
        self.cmpTroveVersionList(d, { 'double:runtime' : 
                                            full['double:runtime'][0:1] } )

        d = branchRepos.getTroveVersionList('localhost', { None : None })
        self.cmpTroveVersionList(d, { 'double' : 
                                            full['double'][0:1],
                                      'double:runtime' : 
                                            full['double:runtime'][0:1],
                                      'double:source' : 
                                            full['double:source'][1:3] } )

        d = runtimeRepos.getTroveVersionList('localhost', { None : None })
        self.cmpTroveVersionList(d, { 'double:runtime' : 
                                            full['double:runtime'] } )

        d = repeatRepos.getTroveVersionList('localhost', { None : None })
        self.cmpTroveVersionList(d, { 'double:runtime' : 
                                            full['double:runtime'] } )

        d = taRepos.getTroveVersionList('localhost', { None : None })
        self.cmpTroveVersionList(d, { 'double:runtime' : 
                                            full['double:runtime'] } )

    # getTroveVersionsByLabel
        full = { 'double':         [ '/localhost@rpl:linux/1.0-1-1' ],
                 'double:runtime': [ '/localhost@rpl:linux/1.0-1-1' ],
                 'double:source': [ '/localhost@rpl:linux/1.0-1' ]
               }

        q = { None : { self.cfg.buildLabel : None } }
        d = repos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveVersionsByLabel(q)
        assert(d == {})
        d = branchRepos.getTroveVersionsByLabel(q)
        assert(d == {})
        d = runtimeRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })

        full = { 'double':         
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1'], 
                 'double:runtime': 
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1'],
                'double:source': 
                            ['/localhost@rpl:linux/1.0-1/branch/1', 
                             '/localhost@rpl:linux/1.0-1/branch/1.1-1']
                            
            }

        q = { None : { branchLabel : None } }
        d = repos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = branchRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, full)
        d = runtimeRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveVersionsByLabel(q)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })

    # getAllTroveLeaves
        full = { 'double':         
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                              '/localhost@rpl:linux/1.0-1-1'], 
                 'double:runtime': 
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                              '/localhost@rpl:linux/1.0-1-1'],
                 'double:source': 
                            [ '/localhost@rpl:linux/1.0-1',
                              '/localhost@rpl:linux/1.0-1/branch/1.1-1'],
            }
        d = repos.getAllTroveLeaves('localhost', { None : None })
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getAllTroveLeaves('localhost', { None : None })
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'][0:1] })
        d = branchRepos.getAllTroveLeaves('localhost', { None : None })
        self.cmpTroveVersionList(d, 
                            { 'double'         : full['double'][0:1],
                              'double:runtime' : full['double:runtime'][0:1],
                              'double:source'  : full['double:source'][1:2],
                              })
        d = runtimeRepos.getAllTroveLeaves('localhost', { None : None })
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getAllTroveLeaves('localhost', { None : None })
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })

    # getTroveLeavesByLabel
        full = { 'double':         [ '/localhost@rpl:linux/1.0-1-1' ],
                 'double:runtime': [ '/localhost@rpl:linux/1.0-1-1' ],
                 'double:source': [ '/localhost@rpl:linux/1.0-1' ],
            }

        qd = { None : { self.cfg.buildLabel : None } }
        d = repos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveLeavesByLabel(qd)
        assert(d == {})
        d = branchRepos.getTroveLeavesByLabel(qd)
        assert(d == {})
        d = runtimeRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })

        full = { 'double':         
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1'], 
                 'double:runtime': 
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1'],
                 'double:source': 
                            [ '/localhost@rpl:linux/1.0-1/branch/1.1-1'],
            }
        qd = { None : { branchLabel : None } }
        d = repos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = branchRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, full)
        d = runtimeRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            { 'double:runtime' : full['double:runtime'] })

    # getTroveLeavesByBranch
        q = { 'double' : { branchVersion : None }, 
                 'double:runtime' : { rootBranch : None } 
            }

        full = { 'double' : [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1' ],
                 'double:runtime' :  [ '/localhost@rpl:linux/1.0-1-1' ] 
            }

        d = repos.getTroveLeavesByBranch(q)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveLeavesByBranch(q)
        assert(d == {})
        d = branchRepos.getTroveLeavesByBranch(q)
        self.cmpTroveVersionList(d, { 'double' : full['double'] })
        d = runtimeRepos.getTroveLeavesByBranch(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveLeavesByBranch(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveLeavesByBranch(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })

    # getTroveVersionsByBranch
        q = { 'double' : { branchVersion : None }, 
                 'double:runtime' : { rootBranch : None } 
            }

        full = { 'double':         
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1'], 
                 'double:runtime': 
                            [ '/localhost@rpl:linux/1.0-1-1' ],
            }

        d = repos.getTroveVersionsByBranch(q)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveVersionsByBranch(q)
        assert(d == {})
        d = branchRepos.getTroveVersionsByBranch(q)
        self.cmpTroveVersionList(d, { 'double' : full['double'] })
        d = runtimeRepos.getTroveVersionsByBranch(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveVersionsByBranch(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveVersionsByBranch(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })

    # getTroveVersionFlavors
        versionList = ['/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                       '/localhost@rpl:linux/1.0-1-1', ] 
        versionDict = {}.fromkeys([ versions.VersionFromString(x) for x in
                                        versionList ], [ None ])

        q = { 'double' : versionDict,
              'double:runtime' : versionDict
            }

        full = { 'double' : versionList,
                 'double:runtime' :  versionList,
            }

        d = repos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, full)
        d = limitedRepos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, { 'double:runtime' : versionList[0:1] })
        d = branchRepos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, { 'double' : versionList[0:1],
                                      'double:runtime': versionList[0:1] } )
        d = runtimeRepos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })
        d = repeatRepos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })
        d = taRepos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, 
                                 { 'double:runtime' : full['double:runtime'] })

        flavor = deps.Flavor()
        if use.Arch.x86:
            flavor.addDep(deps.InstructionSetDependency, 
                          deps.Dependency('x86',
                                          [('mmx', deps.FLAG_SENSE_REQUIRED)]))
        elif use.Arch.x86_64:
            pass
        else:
            raise NotImplementedError, 'edit test for this arch'
        versionDict = {}.fromkeys([ versions.VersionFromString(x) for x in
                                        versionList ], [ flavor ])
        q = { 'double' : versionDict,
              'double:runtime' : versionDict
            }
        d = repos.getTroveVersionFlavors(q)
        self.cmpTroveVersionList(d, full)

    # hasTroves, getTrove & getNewTroveList
    # this test assumes getTroveVersionList works properly
        all = repos.getTroveVersionList('localhost', { None : None })
        all = list(self.asSet(all))

        troves = dict(itertools.izip(all, repos.getTroves(all)))

        for testRepos in (limitedRepos, branchRepos, repeatRepos):
            canSee = testRepos.getTroveVersionList('localhost', { None : None })
            canSee = self.asSet(canSee)
            isPresent = testRepos.hasTroves(all)
            for trvInfo in all:
                # We use double:runtime to check file permission acls becaues
                # it's the only trove which changes between the main label
                # and the branch
                if trvInfo == 'double:runtime':
                    # (pathId, fileId, version)
                    files = [ (x[0], x[2], x[3]) for x in
                                            troves[trvInfo].iterFileList() ]
                else:
                    files = ''

                if trvInfo in canSee:
                    assert(isPresent[trvInfo])
                    testRepos.getTrove(*trvInfo)
                    if files:
                        testRepos.getFileVersions(files)
                        testRepos.getFileContents([ x[1:3] for x in files ])
                else:
                    assert(not isPresent[trvInfo])
                    self.assertRaises(errors.InsufficientPermission,
                                      testRepos.getTrove, *trvInfo)
                    if files:
                        self.assertRaises(errors.FileStreamMissing,
                                          testRepos.getFileVersions, files)
                        self.assertRaises(errors.FileStreamNotFound,
                                          testRepos.getFileContents,
                                          [ x[1:3] for x in files ])

            new = testRepos.getNewTroveList('localhost', 0)
            assert(canSee == set([ x[1] for x in new]))

    # getTroveInfo
    # this test assumes getTroveVersionList works properly
        all = repos.getTroveVersionList('localhost', { None : None })
        del all["double:source"]
        all = list(self.asSet(all))

        troves = dict(itertools.izip(all, repos.getTroves(all)))
        infos = dict(itertools.izip(all, repos.getTroveInfo(
            trove._TROVEINFO_TAG_SOURCENAME, all)))
        for trv in all:
            assert(troves[trv].troveInfo.sourceName == infos[trv])
        # test sigs as well
        infos = dict(itertools.izip(all, repos.getTroveInfo(
            trove._TROVEINFO_TAG_SIGS, all)))
        for trv in all:
            assert(troves[trv].troveInfo.sigs == infos[trv])

        # limitedRepos shouldn't be able to "see all"
        self.assertRaises(errors.TroveMissing, limitedRepos.getTroveInfo,
                          trove._TROVEINFO_TAG_SOURCENAME, all)
        # test calling with a missing trove
        all.append(('double', versions.VersionFromString('/localhost@rpl:linux/1.0-1-2'), 
                    deps.Flavor()))
        self.assertRaises(errors.TroveMissing, repos.getTroveInfo,
                          trove._TROVEINFO_TAG_SOURCENAME, all)
        # check behavior for a trove-access-onoy repository
        d = taRepos.getTroveVersionList('localhost', {None:None})
        for i in taRepos.getTroveInfo(trove._TROVEINFO_TAG_SOURCENAME, list(self.asSet(d))):
            self.assertEqual(i(), "double:source")



        
    def testCompoundAcls(self):
        ### Mimic some of the basic Acl testing above
        rootLabel = versions.Label("localhost@rpl:linux")
        branchLabel = versions.Label("localhost@rpl:branch")

        rootBranch = versions.VersionFromString('/localhost@rpl:linux')

        self.makeSourceTrove('testcase', recipes.testSuiteRecipe)
        self.build(recipes.testSuiteRecipe, "TestSuiteRecipe")

        self.makeSourceTrove('double', recipes.doubleRecipe1)
        self.build(recipes.doubleRecipe1, "Double")

        repos = self.getRepositoryClient()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

        repeatRepos = self.setupUser(repos, rootLabel, 'repeat', 'bar',
                                      'double:runtime', None)
        repos.addAcl(rootLabel, 'repeat', '^testcase$', None)
        repos.addAcl(rootLabel, 'repeat', 'testcase:runtime', None)
        repos.addAcl(rootLabel, 'repeat', 'testcase:test', None)

        full = { 'double':         [ '/localhost@rpl:linux/1.0-1-1' ],
                 'double:runtime': [ '/localhost@rpl:linux/1.0-1-1' ],
                 'double:source':  [ '/localhost@rpl:linux/1.0-1' ],
                 'testcase:source': [ '/localhost@rpl:linux/1-1' ], 
                 'testcase': [ '/localhost@rpl:linux/1-1-1' ],
                 'testcase:test': [ '/localhost@rpl:linux/1-1-1' ],
                 'testcase:runtime': [ '/localhost@rpl:linux/1-1-1' ]
            }

        qd = { None : { self.cfg.buildLabel : None } }
        d = repos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, full)

        d = repeatRepos.getTroveLeavesByLabel(qd)
        self.cmpTroveVersionList(d, 
                            {
                              'double:runtime' : full['double:runtime'],
                              'testcase' : full['testcase'],
                              'testcase:runtime' : full['testcase:runtime'],
                              'testcase:test' : full['testcase:test'],
                            })

    def testAclChanges(self):
        ### Mimic some of the basic Acl testing above
        rootLabel = versions.Label("localhost@rpl:linux")
        branchLabel = versions.Label("localhost@rpl:branch")

        rootBranch = versions.VersionFromString('/localhost@rpl:linux')

        self.makeSourceTrove('double', recipes.doubleRecipe1)
        self.build(recipes.doubleRecipe1, "Double")

        repos = self.getRepositoryClient()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

        limitedRepos = self.setupUser(repos, rootLabel, 'limited', 'bar',
                                      '.*:runtime', branchLabel)

        both = [ 'double', 'double:runtime', 'double:source' ]
        runtime = [ 'double:runtime' ]

        # First ensure that the limitedRepos tests all work
        assert(limitedRepos.troveNames(branchLabel) == [])

        # Now create the branch
        self.mkbranch(self.cfg.buildLabel, branchLabel, 'double:source')
        branchVersion = versions.VersionFromString(
                                '/localhost@rpl:linux/1.0-1-0/branch')

        oldLabel = self.cfg.buildLabel
        self.cfg.buildLabel = branchLabel
        #self.build(recipes.doubleRecipe1, "Double")
        self.updateSourceTrove('double', recipes.doubleRecipe1_1)
        double1_1 = self.build(recipes.doubleRecipe1_1, "Double")
        self.cfg.buildLabel = oldLabel

        assert(limitedRepos.troveNames(branchLabel) == runtime)

        full = { 'double':
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                              '/localhost@rpl:linux/1.0-1-1',
                            ],
                 'double:runtime':
                            [ '/localhost@rpl:linux/1.0-1-0/branch/1.1-1-1',
                              '/localhost@rpl:linux/1.0-1-1',
                            ],
                 'double:source':
                            [
                              '/localhost@rpl:linux/1.0-1',
                              '/localhost@rpl:linux/1.0-1/branch/1',
                              '/localhost@rpl:linux/1.0-1/branch/1.1-1',
                            ],

            }

        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None,
                          'double:source' : None } )
        self.cmpTroveVersionList(d, { 'double:runtime' :
                                            full['double:runtime'][0:1] } )

        #change the permissions to match the rootRepo
        #Try as limitedUser
        #limitedRepos = self.setupUser(repos, rootLabel, 'limited', 'bar',
        #                              '.*:runtime', branchLabel)
        # XXX Fix the following test.  It should return back an exception,
        # but the exception isn't being handled properly by the framework
        try:
            limitedRepos.editAcl(rootLabel, 'limited', '.*:runtime', branchLabel, None, None, False)
        except errors.InsufficientPermission:
            pass
        else:
            assert(0)

        repos.editAcl(rootLabel, 'limited', '.*:runtime', branchLabel, None,
                      None, False)

        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None , 
                          'double:source' : None} )
        self.cmpTroveVersionList(d, full)

        #Try to delete the permission
        #    check the precondition
        assert repos.listAcls(rootLabel, 'limited') == [
            dict(label='ALL', item='ALL', canWrite=0,canRemove=0)]
        #    delete the precondition
        repos.deleteAcl(rootLabel, 'limited', None, None)
        assert repos.listAcls(rootLabel, 'limited') == []
        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None , 
                          'double:source' : None} )
        self.cmpTroveVersionList(d, {})

        repos.addAcl(rootLabel, 'limited', '.*:runtime', branchLabel)
        assert repos.listAcls(rootLabel, 'limited') == [
            dict(label=branchLabel.asString(), item='.*:runtime', canWrite=0,
                 canRemove=0)]
        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None,
                          'double:source' : None } )
        self.cmpTroveVersionList(d, { 'double:runtime' :
                                            full['double:runtime'][0:1] } )


        repos.deleteAcl(rootLabel, 'limited', '.*:runtime', branchLabel)
        assert repos.listAcls(rootLabel, 'limited') == []
        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None , 
                          'double:source' : None} )
        self.cmpTroveVersionList(d, {})

        #Check the above using strings instead of branches and None
        repos.addAcl(rootLabel, 'limited', '.*:runtime', branchLabel.asString())
        assert repos.listAcls(rootLabel, 'limited') == [
            dict(label=branchLabel.asString(), item='.*:runtime', canWrite=0,
                 canRemove=0)]
        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None,
                          'double:source' : None } )
        self.cmpTroveVersionList(d, { 'double:runtime' :
                                            full['double:runtime'][0:1] } )

        repos.editAcl(rootLabel, 'limited', '.*:runtime', branchLabel,
                      'ALL', 'ALL', False, False)

        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None , 
                          'double:source' : None} )
        self.cmpTroveVersionList(d, full)


        repos.deleteAcl(rootLabel, 'limited', 'ALL', 'ALL')
        assert repos.listAcls(rootLabel, 'limited') == []
        d = limitedRepos.getTroveVersionList('localhost',
                        { 'double' : None,
                          'double:runtime' : None , 
                          'double:source' : None} )
        self.cmpTroveVersionList(d, {})


    def testGetRoles(self):
        # XXX this test depends on specific rephelp.py behavior
        # (that the 'test' user exists), and should be extended
        # to add a new group and add 'test' to that group, but
        # that functionality is not yet exposed in the netclient.
        repos = self.openRepository()
        repos = self.getRepositoryClient()

        l = versions.Label("localhost@rpl:linux")
        assert set(repos.listRoles(l)) == set(['test', 'anonymous'])
        
        assert(repos.getRoles(l) == ['test'])

        repos.addRole(l, 'test1')
        assert set(repos.listRoles(l)) == set(['test', 'anonymous', 'test1'])

        repos.updateRoleMembers(l, 'test1', ['test'])
        assert(repos.getRoles(l) == ['test', 'test1'])
        repos.updateRoleMembers(l, 'test1', [])
        assert(repos.getRoles(l) == ['test'])

        # Test addRoleMember and getRoleMembers
        repos.addRoleMember(l, 'test1', 'test')
        self.assertEqual(repos.getRoles(l), ['test', 'test1'])
        self.assertEqual(repos.getRoleMembers(l, 'test1'), ['test'])
        repos.updateRoleMembers(l, 'test1', [])
        self.assertEqual(repos.getRoleMembers(l, 'test1'), [])

        #Now, delete the secondary group
        repos.updateRoleMembers(l, 'test1', ['test'])
        repos.deleteRole(l, 'test1')
        assert set(repos.listRoles(l)) == set(['test', 'anonymous'])
        assert(repos.getRoles(l) == ['test'])

    def testBadUser(self):
        # We should fall back to anonymous/anonymous automatically
        repos = self.openRepository()
        repos = self.getRepositoryClient(user = 'foo', password = 'bar')
        self.addComponent('foo:runtime', '1.0')
        results = repos.getTroveVersionList('localhost', { None : None } )
        assert('foo:runtime' in results)

    def testBadTrovepattern(self):
        repos = self.openRepository()
        user = 'foo'
        l = versions.Label("localhost@rpl:linux")
        self.addUserAndRole(repos, l, user, 'bar')
        # add a bad regex
        self.assertRaises(errors.InvalidRegex,
                          repos.addAcl, l, user, '*', '', False, False)

        # add something to edit
        repos.addAcl(l, user, '.*', '', False, False)

        # edit it ilegally
        self.assertRaises(errors.InvalidRegex,
                          repos.editAcl, l, user, '.*', '', '*', '',
                          False, False)


    def testBadUserTriesToCommit(self):
        # set up a user that can write to something, just not what we're
        # going to try to commit.  This will trigger the anonymous user
        # retry, but the first unsuccessful commit will have erased
        # the incoming changeset file already.
        user = 'limited'
        password = 'bar'
        bl = self.cfg.buildLabel

        repos = self.openRepository()
        self.addUserAndRole(repos, bl, user, password)
        repos.addAcl(bl, user, 'ALL', bl, False, False)
        repos.addAcl(bl, user, 'writestuff.*', bl, True, False)

        limitedRepos = self.getRepositoryClient(user = user,
                                                password = password)
        self.assertRaises(errors.InsufficientPermission, self.addComponent,
                          "test:runtime", "1.0-1-1", repos=limitedRepos)

    def testFallbackThenNeedUser(self):
        # we fall back to anonymous for command one, then need the 
        # user for command 2.
        user = 'limited'
        password = '@\\@/'
        bl = self.cfg.buildLabel

        repos = self.openRepository()
        self.addUserAndRole(repos, bl, user, password)
        #repos.addAcl(bl, user, 'ALL', bl, False, False, False)
        repos.addAcl(bl, user, 'writestuff.*', bl, write = True)
        trv = self.addComponent('foo:runtime', '1.0')

        limitedRepos = self.getRepositoryClient(user = user,
                                                password = password)

        # this should fall back to anonymous
        assert(limitedRepos.getTrove(*trv.getNameVersionFlavor()))

        # this should require the username/passwd
        self.addComponent("writestuff:runtime", "1.0", repos=limitedRepos)

        # but places where we don't have access should still raise
        # an InsufficientPermission error
        self.assertRaises(errors.InsufficientPermission, self.addComponent,
                          "test:runtime", "1.0", repos=limitedRepos)

    def testNonExistingUserTriesToCommit(self):
        user = 'doesnotexist'
        password = 'bar'

        repos = self.openRepository()
        limited = self.getRepositoryClient(user = user,
                                         password = password)

        self.assertRaises(errors.InsufficientPermission, self.addComponent,
                          "test:runtime", "1.0-1-1", repos=limited)

    def testUserPasswordQuoting(self):
        # check to make sure we can handle usernames and passwords with
        # / in them
        repos = self.openRepository()

        user = 'foo'
        password = 'abc/123'
        bl = self.cfg.buildLabel

        self.addUserAndRole(repos, bl, user, password)
        repos.addAcl(bl, user, 'ALL', bl, False, False)
        repos.addAcl(bl, user, 'writestuff.*', bl, True, False)

        limitedRepos = self.getRepositoryClient(user = user,
                                                password = password)
        self.addComponent("writestuff:runtime", "1.0-1-1", repos=limitedRepos)

        l = repos.getTroveVersionList('localhost', { None : None } )
        assert (l.keys() == ['writestuff:runtime'])

    @testhelp.context('entitlements')
    def testExternalAuthChecks(self):
        rootLabel = versions.Label("localhost@rpl:linux")
        self.stopRepository(0)

        pwServer = AuthorizationServer(PasswordHttpRequests)
        entServer = AuthorizationServer(EntitlementRequests)

        try:
            repos = self.openRepository(authCheck = pwServer.url() + 'pwcheck',
                                        entCheck = entServer.url() + 'entcheck')
            repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

            self.setupUser(repos, rootLabel, 'another', 'wrongpass', None, None)
            origEntClient = self.setupEntitlement(repos, 'group', 'ent',
                                                  rootLabel, None, None,
                                                  withClass = True)[0]

            # getting here means user test/foo works (the checkVersion() call
            # during repo setup succeeded)

            pwClient = self.getRepositoryClient(user = 'another', 
                                                password = 'pass')
            pwClient.c['localhost'].checkVersion()

            self.assertRaises(errors.CannotChangePassword, 
                              pwClient.changePassword,
                              rootLabel, 'another', 'newpass')

            pwClient = self.getRepositoryClient(user = 'another',
                                                password = 'wrongpass')

            self.assertRaises(errors.InsufficientPermission,
                              pwClient.c.__getitem__, 'localhost')

            # test this before we replace the broken entitlement with a
            # working one
            self.assertRaises(errors.InsufficientPermission,
                              origEntClient.c.__getitem__, 'localhost')

            entClient = self.getEntitlementClient(
                            [ ('localhost', 'othergrp', 'otherentitlement' ) ],
                            withClass = True).getRepos()
            entClient.c['localhost'].checkVersion()

            entClient = self.getEntitlementClient(
                            [ ('localhost', 'invalidgrp', 'nogroupent' ) ],
                            withClass = False).getRepos()
            entClient.c['localhost'].checkVersion()
        finally:
            pwServer.kill()
            entServer.kill()
            self.stopRepository(0)

    @testhelp.context('entitlements')
    def testThreadedEntitlementUpdates(self):
        # this checks in-memory entitlements as well
        rootLabel = self.cfg.buildLabel
        repos = self.openRepository()
        repos.deleteUserByName(rootLabel, 'anonymous')
        self.cfg.threaded = True
        origEntClient = self.setupEntitlement(repos, 'group', 'ent',
                                              rootLabel, None, None,
                                              onDisk = False)[1]
        self.cfg.threaded = False
        self.addComponent('foo:run', '1')
        self.addCollection('foo', '1', [':run'])
        self.addComponent('bar:run', '1', filePrimer=1)
        self.addCollection('bar', '1', [':run'])

        self.checkUpdate(['foo', 'bar'], ['foo', 'bar', 'foo:run', 'bar:run'],
                         client=origEntClient, apply=True)

    def testPermissionRevoked(self):
        self.openRepository(1)

        tFoo = self.addComponent('foo:run', '/localhost@foo:foo/1-1-1')
        tBar = self.addComponent('foo:run', '/localhost@foo:bar/2-2-2')
        tBar1 = self.addComponent('foo:run', '/localhost1@foo:bar/2-2-2')

        self.updatePkg('foo:run=localhost@foo:foo')

        repos = self.openRepository()
        repos.deleteUserByName(versions.Label('localhost@foo:foo'), 'anonymous')

        limitedRepos = self.setupUser(repos,
                                      versions.Label('localhost@foo:foo'),
                                      'limited', 'bar', None,
                                      versions.Label('localhost@foo:bar'))

        self.assertRaises(errors.InsufficientPermission,
                          limitedRepos.getTrove,
                          'foo:run', tFoo.getVersion(), tFoo.getFlavor() )

        # single repository
        limitedRepos.createChangeSet([
                ('foo:run', (tFoo.getVersion(), tFoo.getFlavor() ),
                            (tBar.getVersion(), tBar.getFlavor() ), False) ] )

        # distributed
        limitedRepos.createChangeSet([
                ('foo:run', (tFoo.getVersion(), tFoo.getFlavor() ),
                            (tBar1.getVersion(), tBar1.getFlavor() ), False) ] )

    def testComplexRegexp(self):
        # SUP-45
        self.addComponent('foo:source', '1')
        self.addComponent('foo:runtime', '1')
        self.addComponent('foo:debuginfo', '1')
        self.addCollection('foo', '1', [':runtime', ':debuginfo'])
        self.addComponent('bar:source', '1')
        self.addComponent('bar:runtime', '1')
        self.addComponent('bar:data', '1')
        self.addComponent('bar:debuginfo', '1')
        self.addCollection('bar', '1', [':runtime', ':debuginfo', ':data'])
        repos = self.getRepositoryClient()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        limitedRepos = self.setupUser(repos, self.cfg.buildLabel,
                                      'limited', 'bar',
                                      '[^:]+(:(?!debuginfo$|source$).*|$)',
                                      self.cfg.buildLabel)
        assert(sorted(limitedRepos.troveNames(self.cfg.buildLabel)) ==
               ['bar', 'bar:data', 'bar:runtime', 'foo', 'foo:runtime'])

    def testRecursiveGetChangeSetAcl(self):
        def _missing(cs, name, trv):
            trvCs = cs.getNewTroveVersion(name, trv.getVersion(),
                                          trv.getFlavor())
            trv = trove.Trove(trvCs)
            return trv.isMissing()

        repos = self.openRepository()
        self.addComponent('conary:runtime', '1')
        debug = self.addComponent('conary:debuginfo', '1')
        trv = self.addCollection('conary', '1', [(':runtime', True),
                                                 (':debuginfo', False)])
        label = versions.Label("localhost@rpl:linux")
        self.addUserAndRole(repos, label, "myuser", "pw")
        repos.deleteUserByName(label, 'anonymous')
        repos.addAcl(label, "myuser", "conary:runtime", label)
        repos.addAcl(label, "myuser", "conary", label)

        # also set up a user that is granted access to the conary
        # pkg via addTroveAccess *only* (CNY-2670)
        self.addUserAndRole(repos, self.cfg.buildLabel, 'ta', 'ta')
        ta = self.getRepositoryClient(user='ta', password='ta')
        repos.addTroveAccess('ta', [ trv.getNameVersionFlavor() ])

        limited = self.getRepositoryClient(user='myuser', password='pw')
        for rep in (limited, ta):
            cs = rep.createChangeSet( [('conary', (None, None),
                                           (trv.getVersion(), trv.getFlavor()),
                                            True)] )
            if rep != ta:
                # the troveaccess will give access to conary:debuginfo
                assert(_missing(cs, 'conary:debuginfo', trv))
            assert(not _missing(cs, 'conary:runtime', trv))

            # fetch it again to make sure it's correct coming from the cache
            cs = rep.createChangeSet( [('conary', (None, None),
                                           (trv.getVersion(), trv.getFlavor()),
                                            True)] )
            if rep != ta:
                # the troveaccess will give access to conary:debuginfo
                assert(_missing(cs, 'conary:debuginfo', trv))
            assert(not _missing(cs, 'conary:runtime', trv))

        # and of course the admin user should always see both.
        cs = repos.createChangeSet( [('conary', (None, None),
                                      (trv.getVersion(), trv.getFlavor()),
                                      True)] )
        assert(not _missing(cs, 'conary:debuginfo', trv))
        assert(not _missing(cs, 'conary:runtime', trv))

    @testhelp.context('entitlements')
    def testMultipleEntitlements(self):
        repos = self.openRepository()

        runtime = self.addComponent('foo:runtime', '1')
        lib = self.addComponent('foo:lib', '1')

        runtimeEnt = self.setupEntitlement(repos, 'rgroup', 'runtimeent',
                                           self.cfg.buildLabel, '.*:runtime',
                                           None, withClass = True,
                                           onDisk = False)[0]
        libEnt = self.setupEntitlement(repos, 'lgroup', 'libent',
                                       self.cfg.buildLabel, '.*:lib',
                                       None, withClass = True,
                                       onDisk = False)[0]

        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

        self.assertRaises(errors.InsufficientPermission, runtimeEnt.getTrove,
                          *lib.getNameVersionFlavor())
        self.assertRaises(errors.InsufficientPermission, libEnt.getTrove,
                          *runtime.getNameVersionFlavor())

        mixed = self.getEntitlementClient(
                [ ( '*', None, 'runtimeent' ),
                  ('localhost', None, 'libent') ], onDisk = False ).getRepos()
        mixed.getTroves( [ runtime.getNameVersionFlavor(),
                           lib.getNameVersionFlavor() ] )

        duplicate = self.getEntitlementClient(
                [ ('localhost', None, 'runtimeent' ),
                  ('localhost', None, 'libent') ], onDisk = False ).getRepos()
        duplicate.getTroves( [ runtime.getNameVersionFlavor(),
                           lib.getNameVersionFlavor() ] )

    @testhelp.context('entitlements')
    def testAnonymousAccess(self):
        # make sure being authentication doesn't stop us from seeing things
        # the anonymous user can see
        repos = self.openRepository()

        runtime = self.addComponent('foo:runtime', '1')
        lib = self.addComponent('foo:lib', '1')

        # set things up so runtime can see :runtime and anonymous can
        # see :lib. of course, this means runtime should see everything
        repos.deleteRole(self.cfg.buildLabel, 'anonymous')
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        anonUser = self.setupUser(repos, self.cfg.buildLabel, 'anonymous',
                                  'anonymous', '.*:lib', None)
        runtimeUser = self.setupUser(repos, self.cfg.buildLabel, 'user', 'pw',
                                     '.*:runtime', None)
        repos.setRoleCanMirror(self.cfg.buildLabel, 'anonymous', False)
        repos.setRoleCanMirror(self.cfg.buildLabel, 'user', False)
        # runtimeUser should be able to see both the troves since the
        # anonymous user can see :lib and he can see :runtime
        assert(runtimeUser.hasTrove(*lib.getNameVersionFlavor()))
        assert(runtimeUser.hasTrove(*runtime.getNameVersionFlavor()))

        # also grab a mirror user to test that anonymous fallback is
        # off for mirror users
        repos.setRoleCanMirror(self.cfg.buildLabel, 'user', True)
        # runtimeUser should only be able to see :runtime now, since
        # anonymous fallback is off for it
        assert(runtimeUser.hasTrove(*runtime.getNameVersionFlavor()))
        assert(not runtimeUser.hasTrove(*lib.getNameVersionFlavor()))

        # CNY-2964
        repos.setRoleCanMirror(self.cfg.buildLabel, 'user', False)
        repos.addRole(self.cfg.buildLabel, 'mirror')
        repos.setRoleCanMirror(self.cfg.buildLabel, 'mirror', True)
        repos.updateRoleMembers(self.cfg.buildLabel, 'mirror', ['user'])
        assert(runtimeUser.hasTrove(*runtime.getNameVersionFlavor()))
        assert(not runtimeUser.hasTrove(*lib.getNameVersionFlavor()))

    def testNonAnonymousAccess(self):
        # make sure various calls are not allowed via the anonymous
        # entitlement
        repos = self.openRepository()
        user = self.setupUser(repos, self.cfg.buildLabel, 'user', 'pw',
                              None, None)
        badUser = self.getRepositoryClient('user', 'badpw')

        self.assertRaises(errors.InsufficientPermission,
                          badUser.changePassword, 'localhost', 'user', 'newpw')

    @testhelp.context('entitlements')
    def testDynamicEntitlementDir(self):
        # do we pick up entitlements which show up in entitlementDir after
        # the client was created?
        repos = self.openRepository()

        runtime = self.addComponent('foo:runtime', '1')
        self.setupEntitlement(repos, "entGroup", "123456", self.cfg.buildLabel,
                              None, None, withClass = True)[0]
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

        cfg = copy.copy(self.cfg)
        cfg.resetToDefault('user')
        cfg.entitlementDirectory = self.workDir
        anonClient = netclient.NetworkRepositoryClient(cfg)

        open(self.workDir + "/localhost", "w").write(
                conarycfg.emitEntitlement('localhost', 'entGroup', '123456') )
        assert(anonClient.hasTrove(*runtime.getNameVersionFlavor()))

    def _entitlementTimeouts(self, RequestClass, authCacheTimeout = 0):
        self.stopRepository(0)
        entClient = self.getEntitlementClient(
                        [ ('localhost', None, 'nogroupent' ) ],
                        withClass = False, onDisk = True).getRepos()
        entServer = AuthorizationServer(RequestClass)
        try:
            repos = self.openRepository(
                    entCheck=entServer.url() + 'entcheck',
                    authTimeout=authCacheTimeout,
                    singleWorker=True,  # allow the cache to work
                    )

            self.setupEntitlement(repos, 'group', 'ent', self.cfg.buildLabel,
                                  None, None, onDisk = False)

            repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

            # does implicit checkVersion call, which causes the entitlement
            # to get cached for 1 second
            entClient.c['localhost']
            # force a timeout. One whole second doesn't seem to be enough,
            # especially in the 64-bit chroot (there are transient errors).
            # Changing to 1.1 seems to do the job here
            # The timeout cache logic in netauth.py seems sane
            self.sleep(1.1)
            self.assertRaises(errors.InsufficientPermission,
                              entClient.c['localhost'].checkVersion)

            # restart the auth server so we can reuse that entitlement
            entServer.kill()
            entServer.start()

            # this is the same test we did before; the only difference is
            # we write out a new entitlement in the middle
            entClient.c['localhost'].checkVersion()
            f = open(self.workDir + '/localhost', 'w')
            f.write(conarycfg.emitEntitlement('localhost',
                                              className = 'othergrp',
                                              key = 'otherentitlement'))
            f.close()
            # force a timeout
            self.sleep(1)
            entClient.c['localhost'].checkVersion()
        finally:
            entServer.kill()
            self.stopRepository(0)

    @testhelp.context('entitlements')
    def testEntitlementTimeouts(self):
        # fails if autoretry happens mistakenly
        self._entitlementTimeouts(OneTimeEntitlementRequests,
                                  authCacheTimeout = 1)

    @testhelp.context('entitlements')
    def testPerEntitlementTimeouts(self):
        # fails if autoretry happens mistakenly
        self._entitlementTimeouts(OneTimeEntitlementRequestsInternalTimeout,
                                  authCacheTimeout = 0)

    @testhelp.context('entitlements')
    def testEntitlementAutoRetry(self):
        # CNY-2060
        # fails if autoretry happens doesn't happen
        self.stopRepository(0)
        entClient = self.getEntitlementClient(
                        [ ('localhost', None, 'nogroupent' ) ],
                        withClass = False, onDisk = True).getRepos()
        entServer = AuthorizationServer(RetryEntitlementRequests)
        try:
            repos = self.openRepository(entCheck = entServer.url() + 'entcheck',
                                        authTimeout = 1)

            self.setupEntitlement(repos, 'group', 'ent', self.cfg.buildLabel,
                                  None, None, onDisk = False)

            repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

            # does implicit checkVersion call, which causes the entitlement
            # to get cached for 1 second
            entClient.c['localhost']
            # force a timeout
            self.sleep(1)

            entClient.c.entitlements.append(('localhost', (None, None)))

            # invalidate the entitlement we have; if this call works, there
            # must have been an autoretry
            self.mock(conarycfg, 'loadEntitlement',
                      lambda *args, **kwargs: 1/0)
            entClient.c['localhost'].checkVersion()
        finally:
            entServer.kill()
            self.stopRepository(0)

    def testOldAclCalls(self):
        # editAcl and addAcl don't work with protocols older than 60
        repos = self.openRepository()
        repos.c['localhost'].setProtocolVersion(59)
        self.assertRaises(errors.InvalidServerVersion,
                          repos.addAcl, self.cfg.buildLabel, 'test', [], [])
        try:
            repos.c['localhost'].addAcl(59)
        except errors.InvalidClientVersion, e:
            assert(str(e) == 'addAcl call only supports protocol versions '
                             '60 and later')
        else:
            assert(False)

        self.assertRaises(errors.InvalidServerVersion,
                          repos.editAcl, self.cfg.buildLabel, 'test', [], [],
                          False, False)
        try:
            repos.c['localhost'].editAcl(59)
        except errors.InvalidClientVersion, e:
            assert(str(e) == 'editAcl call only supports protocol versions '
                             '60 and later')
        else:
            assert(False)

    def testTroveAccess(self):
        repos = self.openRepository()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

        comp = self.addComponent('foo:runtime', '1.0')
        pkg = self.addCollection('foo', '1.0', [ ':runtime' ] )
        grp = self.addCollection('group-foo', '1.0', [ 'foo' ] )

        comp1 = self.addComponent("bar:runtime", "2.0")
        comp1 = self.addComponent("bar:lib", "2.0")
        pkg1 = self.addCollection("bar", "2.0", [":runtime", ":lib"])
        grp1 = self.addCollection("group-bar", "1.1", [("bar", "2.0")])
        grp2 = self.addCollection("group-baz", "1.2", [("foo", "1.0"),
                                                        ("bar:lib", "2.0")])
        
        self.addUserAndRole(repos, self.cfg.buildLabel, 'user', 'user')

        userClient = self.getRepositoryClient(user = 'user', password = 'user')

        # also set up a role with no old-style permissions that can mirror
        self.addUserAndRole(repos, self.cfg.buildLabel, 'mirror', 'mirror')
        repos.setRoleCanMirror(self.cfg.buildLabel, 'mirror', True)
        mirrorClient = self.getRepositoryClient(user='mirror',
                                                password='mirror')

        # Adding a third role here tests CNY-3469
        self.addUserAndRole(repos, self.cfg.buildLabel, 'other', 'other')

        assert(not userClient.hasTrove(*comp.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*comp1.getNameVersionFlavor()))
        repos.addTroveAccess('user', [ comp.getNameVersionFlavor()])
        repos.addTroveAccess('mirror', [ comp.getNameVersionFlavor()])
        repos.addTroveAccess('other', [ comp.getNameVersionFlavor()])
        assert(userClient.hasTrove(*comp.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*comp1.getNameVersionFlavor()))

        self.assertEqual(userClient.troveNames(self.cfg.buildLabel),
                             [ 'foo:runtime' ] )

        # test recursive adding
        self.assertEqual(userClient.hasTroves([
            pkg.getNameVersionFlavor(), grp.getNameVersionFlavor() ] ).values(),
                             [ False, False ] )
        repos.addTroveAccess('user', [ grp.getNameVersionFlavor()])
        # this has a side effect of testing CNY-2758
        repos.addTroveAccess('mirror', [ grp.getNameVersionFlavor()])

        self.assertEqual(userClient.hasTroves([
            pkg.getNameVersionFlavor(), grp.getNameVersionFlavor() ] ).values(),
                             [ True, True ] )
        self.assertEqual(userClient.hasTroves([
            pkg1.getNameVersionFlavor(), grp1.getNameVersionFlavor(), grp2.getNameVersionFlavor(),
            ] ).values(), [ False, False, False ] )

        self.assertEqual(sorted(userClient.troveNames(self.cfg.buildLabel)),
                        [ 'foo', 'foo:runtime', 'group-foo' ] )

        self.assertEqual(sorted(repos.listTroveAccess('localhost', 'user')),
                        [ comp.getNameVersionFlavor(), grp.getNameVersionFlavor() ] )

        expectNewTroves = [
            ('foo:runtime', '/localhost@rpl:linux/1.0-1-1'),
            ('foo', '/localhost@rpl:linux/1.0-1-1'),
            ('group-foo', '/localhost@rpl:linux/1.0-1-1')
            ]
        # test CNY-2755 - the mirror user is in the mirror role, which
        # has no label-based permissions.  The role has the canMirror
        # flag set and has access to group-foo recursively.
        newTroves = mirrorClient.getNewTroveList('localhost', 0)
        newTroves = [ (x[1][0], str(x[1][1])) for x in newTroves ]
        self.assertEqual(sorted(newTroves), sorted(expectNewTroves))

        repos.deleteTroveAccess('user', [ comp.getNameVersionFlavor() ] )
        self.assertEqual(sorted(repos.listTroveAccess('localhost', 'user')),
                             [ grp.getNameVersionFlavor() ] )

        # the group should still give us access to the component (it's
        # part of the group!)
        assert(userClient.hasTrove(*comp.getNameVersionFlavor()))

        # deleting an non-existent access map is a noop.
        repos.deleteTroveAccess('user', [ pkg.getNameVersionFlavor() ] )

        repos.deleteTroveAccess('user', [ grp.getNameVersionFlavor() ] )
        self.assertEqual(repos.listTroveAccess('localhost', 'user'), [] )
        self.assertEqual(userClient.hasTroves([
            pkg.getNameVersionFlavor(), grp.getNameVersionFlavor() ] ).values(),
                             [ False, False ] )
        self.assertEqual(userClient.troveNames(self.cfg.buildLabel), [])

    def testSimpleCommitsWithFiles(self):
        bl = self.cfg.buildLabel
        repos = self.openRepository()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        self.addUserAndRole(repos, self.cfg.buildLabel, 'user', 'user')
        userClient = self.getRepositoryClient(user = 'user', password = 'user')
        repos.addAcl(bl, "user", 'ALL', bl)
                                
        trv = self.addComponent('foo:run', 1, fileContents = [
            ('/usr/share/foo/%02d' % i, 'content %02d\n' % (i,))
            for i in range(5) ])
        ret1 = repos.getTroveVersionList("localhost", {"foo:run":None})
        trv1 = repos.getTrove(*trv.getNameVersionFlavor())
        ret2 = userClient.getTroveVersionList("localhost", {"foo:run":None})
        trv2 = userClient.getTrove(*trv.getNameVersionFlavor())
        self.assertEqual(ret1, ret2)
        self.assertEqual(trv1, trv2)
        
    def testTroveAccessSimple(self):
        repos = self.openRepository()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        self.addUserAndRole(repos, self.cfg.buildLabel, 'user', 'user')
        userClient = self.getRepositoryClient(user = 'user', password = 'user')

        # test mixed component accesses
        compa = self.addComponent('a:runtime', '1')
        compb = self.addComponent('b:runtime', '1')
        compc = self.addComponent('c:runtime', '1')

        pkga = self.addCollection('a', '1', [':runtime'] )
        pkgb = self.addCollection('b', '1', [':runtime'])
        pkgc = self.addCollection('c', '1', [':runtime'])

        grp1 = self.addCollection('group-1', '1', ['a', 'b'])
        grp2 = self.addCollection('group-2', '1', ['b', 'c'])

        # make sure we start with no accesses
        assert(not userClient.hasTrove(*compa.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkga.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp1.getNameVersionFlavor()))

        # add access to pkga
        repos.addTroveAccess('user', [ pkga.getNameVersionFlavor()])
        assert(userClient.hasTrove(*compa.getNameVersionFlavor()))
        assert(userClient.hasTrove(*pkga.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkgb.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkgc.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp1.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp2.getNameVersionFlavor()))
        # add access to group-1
        repos.addTroveAccess('user', [ grp1.getNameVersionFlavor()])
        assert(userClient.hasTrove(*pkga.getNameVersionFlavor()))
        assert(userClient.hasTrove(*pkgb.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkgc.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp2.getNameVersionFlavor()))
        # delete access to pkga
        repos.deleteTroveAccess('user', [ pkga.getNameVersionFlavor()])
        # pkga still has access through group-1
        assert(userClient.hasTrove(*pkga.getNameVersionFlavor()))
        assert(userClient.hasTrove(*pkgb.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkgc.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp2.getNameVersionFlavor()))
        # add access to group-2
        repos.addTroveAccess('user', [ grp2.getNameVersionFlavor()])
        assert(userClient.hasTrove(*pkgc.getNameVersionFlavor()))
        assert(userClient.hasTrove(*grp2.getNameVersionFlavor()))
        # remove access to group-1
        repos.deleteTroveAccess('user', [ grp1.getNameVersionFlavor()])
        assert(not userClient.hasTrove(*compa.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkga.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp1.getNameVersionFlavor()))
        assert(userClient.hasTrove(*pkgc.getNameVersionFlavor()))
        assert(userClient.hasTrove(*pkgb.getNameVersionFlavor()))
        assert(userClient.hasTrove(*grp2.getNameVersionFlavor()))
        # remove access to group-2, while keeping access for just a component
        repos.addTroveAccess('user', [ compc.getNameVersionFlavor()])
        repos.deleteTroveAccess('user', [ grp2.getNameVersionFlavor()])
        for x in [ compa, compb, pkga, pkgb, pkgc, grp1, grp2 ]:
            assert(not userClient.hasTrove(*x.getNameVersionFlavor()))
        assert(userClient.hasTrove(*compc.getNameVersionFlavor()))

        # MultiUser testing
        self.addUserAndRole(repos, self.cfg.buildLabel, 'user2', 'user')
        user2Client = self.getRepositoryClient(user = 'user2', password = 'user')
        for x in [ compa, compb, compc, pkga, pkgb, pkgc, grp1, grp2 ]:
            assert(not user2Client.hasTrove(*x.getNameVersionFlavor()))
        repos.addTroveAccess('user', [ grp1.getNameVersionFlavor()])
        repos.addTroveAccess('user2', [ grp2.getNameVersionFlavor()])
        assert(not user2Client.hasTrove(*compa.getNameVersionFlavor()))
        assert(not user2Client.hasTrove(*pkga.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkgc.getNameVersionFlavor()))
        
    def testTroveAccessMultiple(self):
        repos = self.openRepository()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')
        self.addUserAndRole(repos, self.cfg.buildLabel, 'user', 'user')
        userClient = self.getRepositoryClient(user = 'user', password = 'user')

        haves = []
        havenots = []
        for x in xrange(5):
            # test mixed component accesses
            compa = self.addComponent('a:runtime', str(x))
            compb = self.addComponent('b:runtime', str(x))
            compc = self.addComponent('c:runtime', str(x))
            
            pkga = self.addCollection('a', str(x), [':runtime'] )
            pkgb = self.addCollection('b', str(x), [':runtime'])
            pkgc = self.addCollection('c', str(x), [':runtime'])

            grp1 = self.addCollection('group-1', str(x), ['a', 'b'])
            grp2 = self.addCollection('group-2', str(x), ['b', 'c'])
            repos.addTroveAccess('user', [ grp1.getNameVersionFlavor()])
            haves.append(grp1.getNameVersionFlavor())
            haves.append(pkga.getNameVersionFlavor())
            haves.append(pkgb.getNameVersionFlavor())
            haves.append(compa.getNameVersionFlavor())
            haves.append(compb.getNameVersionFlavor())
            havenots.append(pkgc.getNameVersionFlavor())
            havenots.append(grp2.getNameVersionFlavor())
        for t in haves:
            assert(userClient.hasTrove(*t))
        for t in havenots:
            assert(not userClient.hasTrove(*t))
            if t[0] == 'group-2':
                repos.addTroveAccess('user', [ t ])
        for t in haves + havenots:
            assert(userClient.hasTrove(*t))

    def testTroveAccessMissing(self):
        '''
        Try to add access to a trove not on the repository.

        @tests: CNY-2624
        '''

        repos = self.openRepository()
        self.addUserAndRole(repos, self.cfg.buildLabel, 'user', 'user')
        userClient = self.getRepositoryClient(user = 'user', password = 'user')
        version = self._cvtVersion('1.0')
        try:
            repos.addTroveAccess('user', [
                ('not-there', version, deps.parseFlavor('yummy')) ])
        except errors.TroveMissing, e:
            self.assertEqual(e.version, version)
        else:
            self.fail('TroveMissing was not raised')

    def testUserRoleCreation(self):
        """ test changes in behavior mandated by CNY-2604 """
        repos = self.openRepository()
        repos.deleteUserByName(self.cfg.buildLabel, 'anonymous')

        comp = self.addComponent('foo:runtime', '1.0')
        pkg = self.addCollection('foo', '1.0', [ ':runtime' ] )
        grp = self.addCollection('group-foo', '1.0', [ 'foo' ] )

        # test that just by adding a new user or a role we don't get
        # new permissions or automatic memberships created
        repos.addUser(self.cfg.buildLabel, "user", "pass")
        userClient = self.getRepositoryClient(user = 'user', password = 'pass')
        
        # right now the user should not be a member of any roles, so
        # we expect InsufficientPermissions to be raised
        self.assertRaises(errors.InsufficientPermission,
                          userClient.hasTrove, *comp.getNameVersionFlavor())
        repos.addRole(self.cfg.buildLabel, "user")
        self.assertRaises(errors.InsufficientPermission,
                          userClient.hasTrove, *comp.getNameVersionFlavor())
        # once we add a user to a role, we shouldn't have access to
        # anything until we grant an explicit acl to that role
        repos.updateRoleMembers(self.cfg.buildLabel, "user", ["user"])
        assert(not userClient.hasTrove(*comp.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkg.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*grp.getNameVersionFlavor()))

        # and now add acls that grant limited access
        repos.addAcl(self.cfg.buildLabel, "user", "group-.*", "")
        assert(not userClient.hasTrove(*comp.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkg.getNameVersionFlavor()))
        assert(userClient.hasTrove(*grp.getNameVersionFlavor()))
        repos.addAcl(self.cfg.buildLabel, "user", "foo:.*", "")
        assert(userClient.hasTrove(*comp.getNameVersionFlavor()))
        assert(not userClient.hasTrove(*pkg.getNameVersionFlavor()))
        assert(userClient.hasTrove(*grp.getNameVersionFlavor()))

    def testRoleDeletion(self):
        """ test behavior for role deletion when a user is deleted (CNY-2775) """
        bl = self.cfg.buildLabel
        repos = self.openRepository()
        repos.deleteUserByName(bl, 'anonymous')
                
        repos.addRole(bl, "special")
        repos.addRole(bl, "special1")
        self.assertTrue("special" in repos.listRoles(bl))
        self.assertTrue("special1" in repos.listRoles(bl))
        repos.addUser(bl, "special", "pass")
        specialClient = self.getRepositoryClient(user = "special", password = "pass")
        # a user with no groups raises InsufficientPermission
        self.assertRaises(errors.InsufficientPermission, specialClient.getRoles, bl)
        repos.updateRoleMembers(bl, "special1", ["special"])
        self.assertEqual(specialClient.getRoles(bl), ["special1"])
        repos.updateRoleMembers(bl, "special", ["special"])
        self.assertEqual(set(specialClient.getRoles(bl)), set(["special", "special1"]))
        repos.deleteUserByName(bl, "special")
        self.assertTrue("special1" in repos.listRoles(bl))
        self.assertFalse("special" in repos.listRoles(bl))

        # a role with nothing special will not survive
        repos.addRole(bl, "special")
        repos.addUser(bl, "special", "pass")
        repos.updateRoleMembers(bl, "special", ["special"])
        repos.deleteUserByName(bl, "special")
        self.assertFalse("special" in repos.listRoles(bl))

        # a role with mirror bits should survive
        repos.addRole(bl, "special")
        repos.addUser(bl, "special", "pass")
        repos.updateRoleMembers(bl, "special", ["special"])
        repos.setRoleCanMirror(bl, "special", True)
        repos.deleteUserByName(bl, "special")
        self.assertTrue("special" in repos.listRoles(bl))
        repos.deleteRole(bl, "special")
        
        # a role with admin bits should survive
        repos.addRole(bl, "special")
        repos.addUser(bl, "special", "pass")
        repos.updateRoleMembers(bl, "special", ["special"])
        repos.setRoleIsAdmin(bl, "special", True)
        repos.deleteUserByName(bl, "special")
        self.assertTrue("special" in repos.listRoles(bl))
        repos.deleteRole(bl, "special")

        # a role with more than one user should survive
        repos.addRole(bl, "special")
        repos.addUser(bl, "special", "pass")
        repos.addUser(bl, "special1", "pass1")
        repos.updateRoleMembers(bl, "special", ["special", "special1"])
        repos.deleteUserByName(bl, "special")
        self.assertTrue("special" in repos.listRoles(bl))
        repos.deleteUserByName(bl, "special1")
        self.assertTrue("special" in repos.listRoles(bl))
        repos.deleteRole(bl, "special")
        
        # a role with acls should survive
        repos.addRole(bl, "special")
        repos.addUser(bl, "special", "pass")
        repos.updateRoleMembers(bl, "special", ["special"])
        repos.addAcl(bl, "special", "ALL", "ALL")
        repos.deleteUserByName(bl, "special")
        self.assertTrue("special" in repos.listRoles(bl))
        repos.deleteRole(bl, "special")

        # a role with trove permissions should survive
        repos.addRole(bl, "special")
        trv = self.addComponent("foo:runtime")
        repos.addUser(bl, "special", "pass")
        repos.updateRoleMembers(bl, "special", ["special"])
        repos.addTroveAccess("special", [trv.getNameVersionFlavor()])
        repos.deleteUserByName(bl, "special")
        self.assertTrue("special" in repos.listRoles(bl))
        repos.deleteRole(bl, "special")

        
    def testCommitCheck(self):
        bl = self.cfg.buildLabel

        repos = self.openRepository()
        self.addUserAndRole(repos, bl, "user", "userpass")
        repos.addAcl(bl, "user", 'ALL', bl, write=False)
        repos.addAcl(bl, "user", 'writestuff.*', bl, write=True)

        userRepos = self.getRepositoryClient(user = "user", password = "userpass")
        badRepos  = self.getRepositoryClient(user = "user", password = "badpass")

        trv1 = self.addComponent("readstuff:runtime", "1", repos=repos)
        trv2 = self.addComponent("writestuff:runtime", "1", repos=repos)

        self.assertTrue(repos.commitCheck([trv1.getNameVersionFlavor(), trv2.getNameVersionFlavor()]))
        self.assertTrue(userRepos.commitCheck([trv2.getNameVersionFlavor()]))
        self.assertRaises(errors.TroveAccessError, userRepos.commitCheck, [trv1.getNameVersionFlavor()])
        self.assertRaises(errors.TroveAccessError, badRepos.commitCheck, [trv1.getNameVersionFlavor()])
        self.assertRaises(errors.TroveAccessError, badRepos.commitCheck, [trv2.getNameVersionFlavor()])
        self.assertRaises(errors.TroveAccessError, badRepos.commitCheck,
                          [trv1.getNameVersionFlavor(), trv2.getNameVersionFlavor()])
        self.addComponent("writestuff:runtime", "2", repos = userRepos)

    def testCheckTroveCache(self):
        bl = self.cfg.buildLabel

        repos = self.openRepository()
        repos.deleteUserByName(bl, 'anonymous')
        self.addUserAndRole(repos, bl, "limited", "limited")
        self.addUserAndRole(repos, bl, "other", "other")
                
        repos.addAcl(bl, "limited", 'foo:.*', label=None)
        repos.addAcl(bl, "other", 'foo:runtime', label=None, write=True)
        repos.addAcl(bl, "other", 'foo:devel', label=None, write=True)

        t1 = self.addComponent("foo:lib")
        t2 = self.addComponent("foo:devel")
        t3 = self.addComponent("foo:runtime")
        self.addCollection("foo", "1", [":runtime", ":devel", ":lib"])

        # regular repos should have access to all
        ret = repos.getTroveVersionList(bl.getHost(), { None : None })
        self.assertEqual(set(ret.keys()), set(["foo", "foo:lib", "foo:devel", "foo:runtime"]))

        # limited has access to all foo:* components
        limRepos = self.getRepositoryClient(user = "limited", password = "limited")
        ret = limRepos.getTroveVersionList(bl.getHost(), { None : None })
        self.assertEqual(set(ret.keys()), set(["foo:devel", "foo:runtime", "foo:lib"]))
        self.assertRaises(errors.TroveAccessError, limRepos.commitCheck, [t1.getNameVersionFlavor(), t2.getNameVersionFlavor()])
        self.assertRaises(errors.TroveAccessError, limRepos.commitCheck, [t2.getNameVersionFlavor(), t3.getNameVersionFlavor()])

        # the other has access only to :devel and :runtime (and can commit those too)
        otherRepos = self.getRepositoryClient(user = "other", password = "other")
        ret = otherRepos.getTroveVersionList(bl.getHost(), { None : None })
        self.assertEqual(set(ret.keys()), set(["foo:devel", "foo:runtime"]))
        self.assertTrue(otherRepos.commitCheck([t2.getNameVersionFlavor(), t3.getNameVersionFlavor()]))
        # can't commit to foo:lib
        self.assertRaises(errors.TroveAccessError, otherRepos.commitCheck,
                          [t1.getNameVersionFlavor(), t2.getNameVersionFlavor(), t3.getNameVersionFlavor()])

    def testGetTrovesBySource(self):
        # getTrovesBySource should allow you access to the binary list even if you
        # can't access the source.
        repos = self.openRepository()
        bl = self.cfg.buildLabel
        repos.deleteUserByName(bl, 'anonymous')
        self.addUserAndRole(repos, bl, "limited", "limited")
        repos.addAcl(bl, "limited", '(foo:runtime|foo)', label=None)
        limRepos = self.getRepositoryClient(user = "limited", password = "limited")
        src = self.addComponent('foo:source')
        trv = self.addCollection('foo', [':runtime'], createComps=True, sourceName='foo:source')
        self.assertRaises(errors.InsufficientPermission,
                          limRepos.getTrove, *src.getNameVersionFlavor())
        lst = limRepos.getTrovesBySource('foo:source', src.getVersion())
        assert(sorted(x[0] for x in lst) == ['foo', 'foo:runtime'])

        self.addCollection('foo-other', [':runtime'], createComps=True, sourceName='foo:source')
        self.assertRaises(errors.InsufficientPermission,
                          limRepos.getTrovesBySource, 'foo:source', src.getVersion())
        lst = repos.getTrovesBySource('foo:source', src.getVersion())
        assert(sorted(x[0] for x in lst) == ['foo', 
                                             'foo-other', 'foo-other:runtime', 'foo:runtime'])

    def testGetTrovesByLabel(self):
        # make sure you can't see labels that you don't have access to.

        bl = self.cfg.buildLabel
        repos = self.openRepository()
        repos.deleteUserByName(bl, 'anonymous')
        self.addUserAndRole(repos, bl, "limited", "limited")
        repos.addAcl(bl, "limited", 'foo:runtime', label=None)
        limRepos = self.getRepositoryClient(user = "limited", password = "limited")

        self.addComponent("foo:runtime")
        self.addComponent("foo:runtime=@rpl:branch")
        self.addComponent("foo:data=@rpl:branch2")

        # regular repos should have access to all
        all = sorted(str(x) for x in repos.getLabelsForHost(bl.getHost()))
        limited = sorted(str(x) for x in limRepos.getLabelsForHost(bl.getHost()))
        assert(limited == ['localhost@rpl:branch', 'localhost@rpl:linux'])
        assert(all == ['localhost@rpl:branch', 
                       'localhost@rpl:branch2', 'localhost@rpl:linux'])

class PasswordHttpRequests(SimpleHTTPRequestHandler):

    valid = { ('test', 'foo') : True,
              ('another', 'pass') : True }

    allowNoIp = False

    def log_message(self, *args, **kw):
        pass

    def do_GET(self):
        url, args = self.path.split("?", 1)
        if url not in ("/pwcheck", '/pwCheck'):
            self.send_error(400)
            return

        args = cgi.parse_qs(args)
        if len(args) != 3 and not self.allowNoIp:
            self.send_error(404)
            return
        elif len(args) == 2:
            if not 'user' in args or not 'password' in args:
                self.send_error(404)
                return
            args['remote_ip'] = ['127.0.0.1']
        elif len(args) != 3:
            self.send_error(404)
            return

        if args['remote_ip'][0] not in httputils.LocalHosts:
            self.send_error(400)
            return

        if (args['user'][0], args['password'][0]) in self.valid:
            xml = "<auth valid=\"true\"/>\n"
        else:
            xml = "<auth valid=\"false\"/>\n"

        self.send_response(200)
        self.send_header("Content-type", "text/xml")
        self.send_header("Content-Length", len(xml))
        self.end_headers()
        self.wfile.write(xml)

class EntitlementRequests(SimpleHTTPRequestHandler):

    valid = { ('othergrp', 'otherentitlement') : ('group', 'ent', None, False),
              (None, 'nogroupent') : ('group', 'ent', None, False) }

    def log_message(self, *args, **kw):
        pass
    
    def check(self, entClass, entKey):
        return self.valid.get((entClass, entKey), None)

    def do_GET(self):
        url, args = self.path.split("?", 1)
        if url != "/entcheck":
            self.send_error(400)
            return

        args = cgi.parse_qs(args)
        if 'class' not in args:
            args['class'] = [ None ]

        if len(args) != 4:
            self.send_error(404)
            return

        if (args['server'] != [ 'localhost' ] or
                args['remote_ip'][0] not in httputils.LocalHosts):
            self.send_error(400)
            return

        mappedEnt = self.check(args['class'][0], args['key'][0])

        if mappedEnt is not None:
            xml = conarycfg.emitEntitlement('localhost', *mappedEnt)
        else:
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header("Content-type", "text/xml")
        self.send_header("Content-Length", len(xml))
        self.end_headers()
        self.wfile.write(xml)

class OneTimeEntitlementRequests(EntitlementRequests):

    seen = set()

    def check(self, entClass, entKey):
        if (entClass, entKey) in self.seen:
            return None

        self.seen.add((entClass, entKey))

        return EntitlementRequests.check(self, entClass, entKey)

class OneTimeEntitlementRequestsInternalTimeout(OneTimeEntitlementRequests):

    valid = dict(EntitlementRequests.valid)
    for key, val in valid.items():
        valid[key] = (val[0], val[1], 1, val[3])

class RetryEntitlementRequests(EntitlementRequests):

    valid = dict(EntitlementRequests.valid)
    for key, val in valid.items():
        valid[key] = (val[0], val[1])

class AuthorizationServer:

    def __init__(self, requestHandler):
        # this is racy :-(
        self.port = testhelp.findPorts(num = 1)[0]
        self.requestHandler = requestHandler
        self.start()

    def start(self):
        self.childPid = os.fork()
        if self.childPid > 0:
            # Wait for child to bind
            sock_utils.tryConnect("127.0.0.1", self.port)
            return

        try:
            httpServer = HTTPServer(('', self.port), self.requestHandler)
            httpServer.serve_forever()
        finally:
            os._exit(70)

    def kill(self):
        if self.childPid == 0:
            return
        os.kill(self.childPid, 15)
        pid, status = os.waitpid(self.childPid, 0)
        if not os.WIFSIGNALED(status):
            raise Exception("Not terminated by signal")
        if os.WTERMSIG(status) != 15:
            raise Exception("Not terminated by signal 15: %d" %
                            os.WTERMSIG(status))
        self.childPid = 0

    def url(self):
        return "http://localhost:%d/" % self.port
