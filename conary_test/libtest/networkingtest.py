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

import copy
import cPickle
import pickle
from testrunner import testhelp
from conary.lib import networking


class NetworkingTest(testhelp.TestCase):

    def testPickling(self):
        things = [
                'foo.bar',
                '*.bar',
                'foo.bar:1234',
                '*.bar:1234',
                '192.168.4.5',
                '192.168.4.5:1234',
                '192.168.4.5/24',
                '192.168.4.5/24:1234',
                '[::1]',
                '[::1]:1234',
                '[::1/48]:1234',
                ]
        for name, mangler in [
                ('copy', copy.copy),
                ('deepcopy', copy.deepcopy),
                ('pickle v0', lambda x: pickle.loads(pickle.dumps(x, 0))),
                ('pickle v2', lambda x: pickle.loads(pickle.dumps(x, 2))),
                ('cPickle v0', lambda x: cPickle.loads(cPickle.dumps(x, 0))),
                ('cPickle v2', lambda x: cPickle.loads(cPickle.dumps(x, 2))),
                ]:
            try:
                mangled = [str(mangler(networking.HostPort(x))) for x in things]
                self.assertEqual(mangled, things)
            except:
                print 'Error testing pickling using %r' % (name,)
                raise
