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
from conary_test import rephelp

from conary_test import dbstoretest

from conary.repository import errors
from conary.repository.netrepos import netauth
from conary.repository.netrepos.auth_tokens import AuthToken
from conary.repository.netrepos.trovestore import TroveStore
from conary.server import schema
from conary import sqlite3
from conary import versions
from conary.deps import deps


class NetAuthTest(dbstoretest.DBStoreTestBase):

    def _setupDB(self):
        db = self.getDB()
        schema.createSchema(db)
        schema.setupTempTables(db)

        return db

    def _addUserRole(self, na, username, password):
        na.addRole(username)
        na.addUser(username, password)
        na.addRoleMember(username, username)
        
    def testManageAcls(self):
        db = self._setupDB()

        ts = TroveStore(db)
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        authToken = ("testuser", "testpass", [ (None, None) ], None )
        self._addUserRole(na, "testuser", "testpass")
        na.addAcl("testuser", None, None, write = True)

        ## TODO Test the trove/label aspects of ACL management
        na.deleteAcl("testuser", None, None)

        #If the delete above failed, this will throw an exception
        na.addAcl("testuser", None, None)
        assert(na.authCheck(authToken, admin=False) == True)
        assert(na.authCheck(authToken, admin=True) == False)

        #Now give the user has admin rights
        na.setAdmin("testuser", True)

        assert(na.authCheck(authToken, admin=True) == True)

    def testNetAuth(self):
        db = self._setupDB()
        ts = TroveStore(db)
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        self._addUserRole(na, "testuser", "testpass")
        na.addAcl("testuser", None, None, write = True, remove = True)
        self._addUserRole(na, "luser", "luserpass")
        na.addAcl("luser", None, None)
        self._addUserRole(na, "root", "rootpass")
        na.addAcl("root", None, None, write = True, remove = True)
        na.setAdmin("root", True)

        authToken = ("testuser", "testpass", [ (None, None) ], None )
        badAuthToken = ("testuser", "testPass", [ (None, None) ], None )
        luserToken = ("luser", "luserpass", [ (None, None) ], None )
        badLuserToken = ("luser", "luserfoo", [ (None, None) ], None )
        rootToken = ("root", "rootpass", [ (None, None) ], None )

        assert(na.check(authToken, write=False) != False)
        assert(na.check(authToken, write=True) != False)
        assert(na.check(badAuthToken, write=False) != True)
        assert(na.check(badAuthToken, write=True) != True)

        assert(na.check(luserToken, write=False) != False)
        assert(na.check(luserToken, write=True) != True)

        assert(na.authCheck(rootToken, admin=True) != False)
        assert(na.authCheck(luserToken, admin=True) != True)

        assert(na.check(rootToken, remove=True) != False)
        assert(na.check(luserToken, remove=True) != True)
        assert(na.check(authToken, remove=True) != False)

        assert(na.check(authToken) != False)
        assert(na.check(badAuthToken) != True)

        # Shim clients, with a ValidPasswordToken
        entitlements = [(None, None)]
        authShim = ("testuser", netauth.ValidPasswordToken, entitlements, None)
        luserShim = ("luser", netauth.ValidPasswordToken, entitlements, None)
        badShim = ("nobody", netauth.ValidPasswordToken, entitlements, None)

        assert na.check(authShim, write=False)
        assert na.check(authShim, write=True)
        assert na.check(authShim, remove=True)

        assert na.check(luserShim, write=False)
        assert not na.check(luserShim, write=True)
        assert not na.check(luserShim, remove=True)

        assert not na.check(badShim, write=False)

        # Shim clients, with a ValidUser()
        authRoleShim = (netauth.ValidUser('testuser'), None, entitlements, None)
        luserRoleShim = (netauth.ValidUser('luser'), None, entitlements, None)
        badRoleShim = (netauth.ValidUser('nobody'), None, entitlements, None)

        assert na.check(authRoleShim, write=False)
        assert na.check(authRoleShim, write=True)
        assert na.check(authRoleShim, remove=True)

        assert na.check(luserRoleShim, write=False)
        assert not na.check(luserRoleShim, write=True)
        assert not na.check(luserRoleShim, remove=True)

        assert not na.check(badRoleShim, write=False)

        try:
            na.addAcl("testuser", None, None, write = True)
        except errors.PermissionAlreadyExists:
            pass
        else:
            self.fail("PermissionAlreadyExists exception expected")

        try:
            na.addUser("teStUseR", 'TeStPaSs')
        except (errors.UserAlreadyExists, errors.RoleAlreadyExists):
            pass
        else:
            self.fail("UserAlreadyExists or RoleAlreadyExists exception expected")
        try:
            na.addRole("lUseR")
        except errors.RoleAlreadyExists:
            pass
        else:
            self.fail("RoleAlreadyExists exception expected")

    def testDeleteAuth(self):
        db = self._setupDB()
        ts = TroveStore(db)
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        # schema creation creates these for us - we need to get rid of them for this test
        for user in na.userAuth.getUserList():
            na.deleteUserByName(user)
        for group in na.getRoleList():
            na.deleteRoleByName(group)

        self._addUserRole(na, "deluser1", "delpass")
        na.addAcl("deluser1", None, None)

        self._addUserRole(na, "deluser2", "delpass")
        na.addAcl("deluser2", None, None, write = True)
        na.addRoleMember("deluser2", "deluser1")

        # Create another group and add deluser1 to said group
        na.addRole('delgroup1')
        na.addAcl('delgroup1', None, None, write = True)
        na.setAdmin('delgroup1', True)
        na.addRoleMember("delgroup1", "deluser1")
        na.addRole('delgroup2')
        na.addAcl('delgroup2', None, None, write = True)
        na.setAdmin('delgroup2', True)
        na.addRoleMember("delgroup2", "deluser1")
        na.addRoleMember("delgroup2", "deluser2")

        self.assertEqual(na.getRoles('deluser1'),
                             ['deluser1', 'deluser2', 'delgroup1', 'delgroup2'])
        self.assertEqual(na.getRoles('deluser2'),
                             ['deluser2', 'delgroup2'])

        # Delete user2 and see if group2 still lists user2
        na.deleteUserByName('deluser2')
        self.assertEqual(list(na.getRoleMembers('delgroup2')),
                             ['deluser1'] )
        self.assertEqual(list(na.userAuth.getRolesByUser('deluser1')),
                             [ 'deluser1', 'deluser2', 'delgroup1', 'delgroup2' ])
        na.deleteRole('delgroup1')

        self.assertEqual(list(na.userAuth.getRolesByUser('deluser1')),
                             [ 'deluser1', 'deluser2', 'delgroup2' ])
        # because deluser1 will have no acl, it should go too
        na.deleteAcl("deluser1", None, None)
        na.deleteUserByName('deluser1')
        self.assertEqual(list(na.getRoleMembers('delgroup2')), [])
        na.deleteRole('delgroup2')

        self.assertEqual(na.getRoleList(), ['deluser2'] )
        na.deleteRole('deluser2')

        try:
            na.deleteUserByName("nonexistentUser")
        except errors.UserNotFound:
            pass
        else:
            self.fail("UserNotFound exception expected")

        #try adding a user to make sure that it happens successfully
        self._addUserRole(na, 'user1afterdel', 'testpass')
        #delete the group, but not the user
        na.deleteRole('user1afterdel')
        self.assertEqual(na.getRoleList(), [] )
        self.assertEqual(list(na.userAuth.getUserList()),
                             [('user1afterdel')])
        self._addUserRole(na, 'user2afterdel', 'testpass')
        na.addAcl("user2afterdel", None, None)
        na.deleteUserByName('user1afterdel')

    def testChangePassword(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        self._addUserRole(na, "testuser", "testpass")
        na.addAcl("testuser", None, None)

        authToken = ("testuser", "testpass", [ (None, None) ], None )
        authToken2 = ("testuser", "newpass", [ (None, None) ], None )

        assert(na.check(authToken) != False)

        na.changePassword("testuser", "newpass")
        assert(na.check(authToken) != True)
        assert(na.check(authToken2) != False)

    def testNetAuthQueries(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        # schema creation creates these for us - we need to get rid of them for this test
        for user in na.userAuth.getUserList():
            na.deleteUserByName(user)
        for group in na.getRoleList():
            na.deleteRoleByName(group)

        self._addUserRole(na, "testuser", "testpass")
        na.addAcl("testuser", None, "conary.rpath.com@rpl:linux", write = True)

        users = na.userAuth.getUserList()
        groups = na.getRoleList()

        assert(users  == [('testuser')])
        assert(groups == [('testuser')])

        groupsByUser = list(na.userAuth.getRolesByUser(users[0]))
        perms = list(na.iterPermsByRole(groupsByUser[0]))

        assert(groupsByUser == [('testuser')])
        assert(perms == [("conary.rpath.com@rpl:linux", 'ALL', 1, 0)])

        dictperms = na.getPermsByRole(groupsByUser[0])
        assert perms[0][0] == dictperms[0]['label']
        assert perms[0][1] == dictperms[0]['item']
        assert perms[0][2] == dictperms[0]['canWrite']
        assert perms[0][3] == dictperms[0]['canRemove']

    def testAddRole(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        na.addUser("testuser", "testpass")
        na.addRole("testgroup")
        na.addRoleMember("testgroup", "testuser")

        groupsByUser = list(na.userAuth.getRolesByUser("testuser"))
        assert(groupsByUser == [ "testgroup" ])

    def testManageRole(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        self._addUserRole(na, "testuser", "testpass")
        self._addUserRole(na, "testuser1", "testpass")

        na.addRole("testgroup")

        na.renameRole("testuser", "renamedgrp")

        assert(list(na.userAuth.getRolesByUser("testuser")) ==
                        [ "renamedgrp" ])
        #Should do nothing, but definitely not throw an exception
        na.renameRole("testuser", "renamedgrp")
        #This will just change the case of the groupname
        na.renameRole("testuser", "reNameDgrp")

        try:
            na.renameRole("testgroup", "renamedgrp")
        except errors.RoleAlreadyExists:
            db.rollback()
        else:
            self.fail("RoleAlreadyExists exception expected")

        try:
            na.renameRole("testgroup", "reNameDgrp")
        except errors.RoleAlreadyExists:
            db.rollback()
        else:
            self.fail("RoleAlreadyExists exception expected")

        na.updateRoleMembers("testgroup", [ 'testuser', 'testuser1' ])
        assert(list(na.getRoleMembers("testgroup")) ==
                ['testuser', 'testuser1'])

        na.updateRoleMembers("testgroup", [])
        assert(list(na.getRoleMembers("testgroup")) == [])

    @testhelp.context('entitlements')
    def testManageEntitlements(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        self._addUserRole(na, "normal", "normalpass")
        na.addAcl("normal", None, None, write = True)
        normalToken = ("normal", "normalpass", [], None )

        self._addUserRole(na, "owner", "ownerpass")
        na.addAcl("owner", None, None, write = True)
        ownerToken = ("owner", "ownerpass", [], None )

        self._addUserRole(na, "root", "rootpass")
        na.addAcl("root", None, None, write = True)
        na.setAdmin("root", True)
        rootToken = ("root", "rootpass", [], None )

        na.addRole("specialReads")
        na.addRole("entOwner")
        na.addRoleMember("entOwner", "owner")

        self.assertRaises(errors.InsufficientPermission, na.addEntitlementClass,
                          normalToken, "cust1", "specialReads")

        self.assertRaises(errors.InsufficientPermission, na.addEntitlementClass,
                          (rootToken[0], "foo", [], None ),
                          "cust1", "specialReads")

        self.assertRaises(errors.RoleNotFound, na.addEntitlementClass,
                          rootToken, "cust1", "unknownRole")

        na.addEntitlementClass(rootToken, "cust1", "specialReads")
        self.assertRaises(errors.EntitlementClassAlreadyExists, na.addEntitlementClass,
                          rootToken, "cust1", "specialReads")

        self.assertRaises(errors.InsufficientPermission,
                          na.addEntitlementClassOwner,
                          normalToken, "specialReads", "cust1")
        na.addEntitlementClassOwner(rootToken, "entOwner", "cust1")

        self.assertRaises(errors.InsufficientPermission, na.addEntitlementKey,
                          normalToken, "cust1", "ENTITLEMENT0")
        na.addEntitlementKey(rootToken, "cust1", "ENTITLEMENT0")
        na.addEntitlementKey(ownerToken, "cust1", "ENTITLEMENT1")
        self.assertRaises(errors.EntitlementKeyAlreadyExists, na.addEntitlementKey,
                          ownerToken, "cust1", "ENTITLEMENT1")

        self.assertRaises(errors.InsufficientPermission, na.iterEntitlementKeys,
                          normalToken, "cust1")
        l1 = sorted(na.iterEntitlementKeys(rootToken, "cust1"))
        l2 = sorted(na.iterEntitlementKeys(ownerToken, "cust1"))
        self.assertEqual(l1, l2)
        self.assertEqual(l1, [ 'ENTITLEMENT0', 'ENTITLEMENT1' ])

    def testCheckTrove(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        assert(na.checkTrove("foo", "foo"))
        assert(na.checkTrove("^foo$", "foo"))
        assert(not na.checkTrove("foo", "barfoo"))
        assert(not na.checkTrove("foo", "foobar"))
        assert(na.checkTrove("foo.*", "foo:runtime"))

    def testNetAuthCheck(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        self._addUserRole(na, "testuser", "testpass")
        na.addAcl("testuser", ".*:runtime", "conary.rpath.com@label:1")
        tu = ("testuser", "testpass", [ (None, None) ], None )

        v1 = versions.VersionFromString("/conary.rpath.com@label:1/1-1")
        v2 = versions.VersionFromString("/conary.rpath.com@label:2/1-1")
        v3 = versions.VersionFromString("/conary.rpath.com@label:3/1-1")
        assert(na.check(tu, label=v1.branch().label(), trove="foo:runtime"))
        assert(not na.check(tu, label=v1.branch().label(), trove="foo:runtime", write=True))
        assert(not na.check(tu, label=v1.branch().label(), trove="foo:lib"))
        assert(not na.check(tu, label=v1.branch().label(), trove="fooruntime"))
        assert(not na.check(tu, label=v2.branch().label(), trove="foo:runtime"))

        # try old format of the authTokens
        assert(na.check(("testuser", "testpass", None, None),
                        label=v1.branch().label(), trove="foo:runtime"))
        assert(na.check(("testuser", "testpass", [ (None, None) ] ),
                        label=v1.branch().label(), trove="foo:runtime"))

        self._addUserRole(na, "gooduser", "goodpass")
        gu = ("gooduser", "goodpass", [ (None, None) ], None )
        na.addAcl("gooduser", None, None)
        na.addAcl("gooduser", ".*:devel", None, write=True)
        na.addAcl("gooduser", ".*:test", "conary.rpath.com@label:1", write=True)
        na.addAcl("gooduser", None, "conary.rpath.com@label:2", write=True, remove=True)
        assert(na.check(gu, label=v1.branch().label(), trove="foo"))
        assert(na.check(gu, label=v2.branch().label(), trove="foo"))
        assert(na.check(gu, label=v3.branch().label(), trove="foo"))
        assert(na.check(gu, label=v1.branch().label(), trove="foo:devel", write=True))
        assert(na.check(gu, label=v2.branch().label(), trove="bar:devel", write=True))
        assert(na.check(gu, label=v3.branch().label(), trove="baz:devel", write=True))
        assert(na.check(gu, label=v1.branch().label(), trove="foo:test", write=True))
        assert(na.check(gu, label=v2.branch().label(), trove="foo:test", write=True))
        assert(not na.check(gu, label=v3.branch().label(), trove="foo:test", write=True))
        assert(not na.check(gu, label=v1.branch().label(), trove="foo:runtime", write=True))

        self.assertEqual(na.commitCheck(gu, [("foo:devel",v1),("bar:devel",v2),("baz:devel",v3)]),
                             [True]*3)
        self.assertEqual(na.commitCheck(gu, [("foo:test",v1),("bar:test",v1),("baz:test",v1)]),
                             [True]*3)
        self.assertEqual(na.commitCheck(gu, [("foo:junk",v2),("bar:test",v2),("baz:lib",v2)]),
                             [True]*3)
        self.assertEqual(na.commitCheck(gu, [("foo:test",v1),("bar:test",v1),("baz:lib",v1)]),
                             [True,True,False])
        self._addUserRole(na, "zerouser", "zeropass")
        zu = ("zerouser", "zeropass", [ (None, None) ], None )
        assert(not na.check(zu, label=v1.branch().label(), trove="foo"))
        assert(not na.check(zu, label=v2.branch().label(), trove="ALL"))
        assert(not na.check(zu, label=v3.branch().label(), trove="foo:runtime"))
        self.assertEqual(na.commitCheck(zu, [("foo", v1)]), [False])

        # Try the shim bypass token
        bypass = ("gooduser", netauth.ValidPasswordToken,
            [ (None, None) ],None )
        self.assertTrue(na.check(bypass,
            label=v1.branch().label(), trove="foo"))
        self.assertTrue(na.check(bypass,
            label=v2.branch().label(), trove="foo:devel", write=True))
        self.assertTrue(na.check(bypass,
            label=v3.branch().label(), trove="foo:runtime"))

        bypass_zero = ("zerouser", netauth.ValidPasswordToken,
            [ (None, None) ],None )
        self.assertFalse(na.check(bypass_zero,
            label=v1.branch().label(), trove="foo"))
        self.assertFalse(na.check(bypass_zero,
            label=v2.branch().label(), trove="foo:devel", write=True))
        self.assertFalse(na.check(bypass_zero,
            label=v3.branch().label(), trove="foo:runtime"))

    def testInvalidNames(self):
        db = self.getDB()
        schema.createSchema(db)
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")

        try:
            na.addUser("test user", "testpass")
        except errors.InvalidName, e:
            self.assertEqual(str(e), 'InvalidName: test user')

        try:
            na.addRole("test group")
        except errors.InvalidName, e:
            self.assertEqual(str(e), 'InvalidName: test group')

    def testInvalidEntitlementClass(self):
        db = self.getDB()
        schema.createSchema(db)
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")
        self._addUserRole(na, "root", "rootpass")
        na.setAdmin("root", True)
        self.assertRaises(errors.UnknownEntitlementClass,
                          na.addEntitlementKey,
                          ("root", "rootpass", None, None), "group", "1234")

    def testRoleFilters(self):
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "conary.rpath.com")
        self._addUserRole(na, "testuser", "testpass")
        roleId = na._getRoleIdByName('testuser')
        geoip = {
                '1.2.3.4': deps.parseFlavor('country.XC'),
                '5.6.7.8': deps.parseFlavor('country.XB'),
                }
        na.geoIp.getFlags = lambda x: geoip[x]

        na.setRoleFilters({'testuser': (
            deps.parseFlavor('!country.XA,!country.XB'), None)})
        self.assertEqual(na.getRoleFilters(['testuser']),
                {'testuser': (
                    deps.parseFlavor('!country.XA,!country.XB'), deps.Flavor())})

        token = AuthToken('testuser', 'testpass', remote_ip='1.2.3.4')
        self.assertEqual(
                na.getAuthRoles(db.cursor(), token), set([roleId]))

        token = AuthToken('testuser', 'testpass', remote_ip='5.6.7.8')
        level = netauth.log.level
        netauth.log.setLevel(100)
        try:
            self.assertRaises(errors.InsufficientPermission,
                    na.getAuthRoles, db.cursor(), token)
        finally:
            netauth.log.setLevel(level)

class NetAuthTest2(rephelp.RepositoryHelper):
    def _setupDB(self):
        self.openRepository()
        db = self.servers.servers[0].reposDB.connect()
        schema.setupTempTables(db)
        return db

    def _addUserRole(self, na, username, password):
        na.addRole(username)
        na.addUser(username, password)
        na.addRoleMember(username, username)

    def testBatchCheck(self):
        if sqlite3.sqlite_version_info() < (3,7,0):
            raise testhelp.SkipTestException("buggy sqlite; use embedded sqlite")
        self.openRepository()
        db = self._setupDB()
        na = netauth.NetworkAuthorization(db, "localhost")

        db.transaction()

        self._addUserRole(na, "ro", "ro")
        na.addAcl("ro", "foo:.*", label=None, write=False)
        ro = ("ro", "ro", [ (None, None) ], None )

        self._addUserRole(na, "rw", "rw")
        na.addAcl("rw", "foo:.*", label=None, write=True)
        rw = ("rw", "rw", [ (None, None) ], None )
        
        self._addUserRole(na, "mixed", "mixed")
        na.addAcl("mixed", "foo:.*", label=None, write=False)
        na.addAcl("mixed", "foo:runtime", label=None, write=True)
        mixed = ("mixed", "mixed", [ (None, None) ], None )

        db.commit()
        
        fr = self.addComponent("foo:runtime")
        fd = self.addComponent("foo:devel")
        troveList = [ (fr.getName(), fr.getVersion().asString(), fr.getFlavor().freeze()),
                      (fd.getName(), fd.getVersion().asString(), fd.getFlavor().freeze())]
        self.assertEqual(na.batchCheck(ro, troveList), [True,True])
        self.assertEqual(na.batchCheck(ro, troveList, write=True), [False,False])
        self.assertEqual(na.batchCheck(rw, troveList), [True,True])
        self.assertEqual(na.batchCheck(rw, troveList, write=True), [True,True])
        self.assertEqual(na.batchCheck(mixed, troveList), [True,True])
        self.assertEqual(na.batchCheck(mixed, troveList, write=True), [True,False])
