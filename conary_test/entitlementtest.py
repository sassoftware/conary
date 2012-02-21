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


from testrunner import testhelp
from conary import versions
from conary.repository import errors
from conary.repository.netrepos.netauth import MAX_ENTITLEMENT_LENGTH

from conary_test import auth_helper


class EntitlementTest(auth_helper.AuthHelper):

    @testhelp.context('entitlements')
    def testEntitlements(self):
        develLabel = versions.Label("localhost@rpl:linux")

        rootRepos = self.openRepository()
        ownerRepos = self.setupUser(rootRepos, develLabel, 'owner', 'bar',
                                    None, None)
        # this creates a user group as a side effect
        userRepos = self.setupUser(rootRepos, develLabel, 'user', 'bar',
                                    None, None)

        rootRepos.addEntitlementClass('localhost', 'client2', 'user')
        assert(rootRepos.listEntitlementClasses('localhost') == [ 'client2' ])
        rootRepos.addEntitlementClass('localhost', 'client', 'user')
        assert(ownerRepos.listEntitlementClasses('localhost') == [ ])
        assert(set(rootRepos.listEntitlementClasses('localhost')) 
               == set([ 'client2', 'client' ]))
        rootRepos.deleteEntitlementClass('localhost', 'client2')
        assert(rootRepos.listEntitlementClasses('localhost') == [ 'client'])
        rootRepos.addEntitlementClassOwner('localhost', 'owner', 'client')
        ownerRepos.addEntitlementKeys('localhost', 'client', [ 'ENTITLEMENT' ])
        assert(ownerRepos.listEntitlementKeys('localhost', 'client') == 
               [ 'ENTITLEMENT' ])

        d = rootRepos.getEntitlementClassesRoles('localhost', [ 'client' ])
        assert(d == { 'client' : [ 'user' ] } )

        rootRepos.setEntitlementClassesRoles('localhost',
                                             { 'client' : [] } )
        d = rootRepos.getEntitlementClassesRoles('localhost', [ 'client' ])
        assert(d == { 'client' : [ ] } )
        rootRepos.setEntitlementClassesRoles('localhost',
                                                 { 'client' : [ 'user' ] } )

        self.assertRaises(errors.EntitlementKeyAlreadyExists,
                              ownerRepos.addEntitlementKeys,
                              'localhost', 'client', [ 'ENTITLEMENT' ])
        self.assertRaises(errors.InvalidEntitlement,
                              ownerRepos.addEntitlementKeys,
                              'localhost', 'client', [ '1' * (MAX_ENTITLEMENT_LENGTH+1) ])
        self.assertRaises(errors.InsufficientPermission,
                              userRepos.addEntitlementKeys,
                              'localhost', 'client', [ 'ENTITLEMENT2' ])
        self.assertRaises(errors.InvalidEntitlement,
                              ownerRepos.deleteEntitlementKeys,
                              'localhost', 'client', [ 'ENTITLEMENTDNE' ])
        self.assertRaises(errors.InsufficientPermission,
                              userRepos.listEntitlementKeys,
                              'localhost', 'client')

        ownerRepos.deleteEntitlementKeys('localhost', 'client', [ 'ENTITLEMENT' ])
        assert(ownerRepos.listEntitlementKeys('localhost', 'client') == [] )

        self.assertRaises(errors.InsufficientPermission,
                              ownerRepos.deleteEntitlementClassOwner,
                              'localhost', 'owner', 'client')

        assert(ownerRepos.listEntitlementClasses('localhost') == [ 'client' ])
        rootRepos.deleteEntitlementClassOwner('localhost', 'owner', 'client')
        assert(ownerRepos.listEntitlementClasses('localhost') == [ ])

        # try removing a ent group that never existed (CNY-692)
        self.assertRaises(errors.UnknownEntitlementClass,
                              rootRepos.deleteEntitlementClass,
                              'localhost', 'neverexisted')

        # and add an owner as a role that doesn't exist
        self.assertRaises(errors.RoleNotFound,
                              rootRepos.addEntitlementClassOwner,
                              'localhost', 'nouser', 'client')

        # and add an owner for a entclass that doesn't exist
        self.assertRaises(errors.UnknownEntitlementClass,
                              rootRepos.addEntitlementClassOwner,
                              'localhost', 'owner', 'noclass')

        # and delete an owner for a role that doesn't exist
        self.assertRaises(errors.RoleNotFound,
                              rootRepos.deleteEntitlementClassOwner,
                              'localhost', 'nouser', 'client')

        # and delete an owner for a entclass that doesn't exist
        self.assertRaises(errors.UnknownEntitlementClass,
                              rootRepos.deleteEntitlementClassOwner,
                              'localhost', 'owner', 'noclass')
