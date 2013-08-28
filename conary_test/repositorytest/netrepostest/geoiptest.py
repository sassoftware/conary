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
from testutils import mock

from conary.deps import deps
from conary.repository.netrepos import geoip


class GeoIPTest(testhelp.TestCase):

    def testLookup(self):
        g = geoip.GeoIPLookup([])
        g.dbs = [mock.MockObject(), mock.MockObject()]
        g.dbs[0].country_code_by_addr._mock.setReturn('AA', '1.2.3.4')
        self.assertEqual(g.getFlags('1.2.3.4'), deps.parseFlavor('country.AA'))

        g.dbs[0].country_code_by_addr._mock.raiseErrorOnAccess(geoip.GeoIPError)
        g.dbs[1].country_code_by_addr._mock.setReturn('XX', 'f00::b47')
        self.assertEqual(g.getFlags('f00::b47'), deps.parseFlavor('country.XX'))

    def testReservedLookups(self):
        g = geoip.GeoIPLookup([])
        for ip, flags in [
                ('127.0.0.1', 'reserved.loopback'),
                ('172.16.1.1', 'reserved.site-local'),
                ('fe80::f00', 'reserved.link-local'),
                ]:
            self.assertEqual(g.getFlags(ip), deps.parseFlavor(flags))
