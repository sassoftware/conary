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
