#!/usr/bin/python
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

from conary import conarycfg
from conary import conaryclient

from conary_test import rephelp


class AuthHelper(rephelp.RepositoryHelper):

    def setupEntitlement(self, repos, group, ent, reposLabel, troves, label,
                         onDisk = True, withClass = False):
        # just create the group
        repos.addRole(reposLabel, group)
        repos.addAcl(reposLabel, group, troves, label, False, False)
        repos.setRoleCanMirror(reposLabel, group, True)
        repos.addEntitlementClass('localhost', group, group)
        repos.addEntitlementKeys('localhost', group, [ ent ])

        client = self.getEntitlementClient([ ('localhost', group, ent) ],
                                           onDisk = onDisk,
                                           withClass = withClass)
        return client.getRepos(), client

    def getEntitlementClient(self, entList, onDisk = True,
                             withClass = False):
        if onDisk:
            for server, group, ent in entList:
                thisCfg = copy.copy(self.cfg)
                thisCfg.entitlementDirectory = self.workDir
                thisCfg.entitlement = conarycfg.EntitlementList()
                thisCfg.user = thisCfg.user.__class__()
                if withClass:
                    classInfo = "<class>%s</class>" % group
                else:
                    classInfo = ""

                open(self.workDir + "/%s" % server, "w").write(
                            "<server>%s</server>\n"
                            "%s\n"
                            "<key>%s</key>\n" % (server, classInfo, ent))

            thisCfg.readEntitlementDirectory()

            client = conaryclient.ConaryClient(thisCfg)
        else:
            if not withClass:
                group = None

            thisCfg = copy.copy(self.cfg)
            thisCfg.entitlement = conarycfg.EntitlementList()
            for server, group, ent in entList:
                thisCfg.entitlement.addEntitlement(server, ent,
                                                   entClass = group)
            thisCfg.user = thisCfg.user.__class__()
            client = conaryclient.ConaryClient(thisCfg)
        return client

    def cmpTroveVersionList(self, d, targ):
        newD = {}
        for (troveName, versionList) in d.iteritems():
            newD[troveName] = [ x.asString() for x in versionList ]
            # canoncialize it
            newD[troveName].sort()

        assert(newD == targ)

    def asSet(self, d):
        s = set()
        for name, vDict in d.iteritems():
            for version, fList in vDict.iteritems():
                s.update((name, version, f) for f in fList)

        return s
