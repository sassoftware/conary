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


import unittest

from conary.lib import enum

class EnumTest(unittest.TestCase):

    def testEnum(self):
        theEnum = enum.EnumeratedType("testenum", "foo", "bar")
        assert(theEnum.foo == "testenum-foo")
        assert(theEnum.values() == [ "testenum-foo", "testenum-bar" ])
        self.assertRaises(AttributeError, theEnum.__getattr__, "bang")
