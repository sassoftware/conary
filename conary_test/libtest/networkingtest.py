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
