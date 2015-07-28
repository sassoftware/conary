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

from conary.lib import pydeps


class BasicTest(testhelp.TestCase):

    def test_deunicode(self):
        for input, expected in (
                (u"foo", "foo"),
                ("foo", "foo"),
                ((u"foo", "bar"), ("foo", "bar")),
                ([u"foo", "bar"], ["foo", "bar"]),
                (set([u"foo", "bar"]), set(["foo", "bar"])),
                (dict(foo=u"foo", bar="bar"), dict(foo="foo", bar="bar")),
                ({u"foo": u"foo", u"bar": "bar", "baz": u"baz", "spam": "spam"},
                 {"foo": "foo", "bar": "bar", "baz": "baz", "spam": "spam"}),
                (dict(foo=set([u"foo", "bar"]), bar=(u"foo", "bar"),
                      baz=[u"foo", "bar"]),
                 dict(foo=set(["foo", "bar"]), bar=("foo", "bar"),
                      baz=["foo", "bar"])),
                ([dict(foo=u"foo", bar="bar")], [dict(foo="foo", bar="bar")]),
                ):
            actual = pydeps._deunicode(input)
            self.assertEquals(
                expected,
                actual,
                "pydeps._deunicode(%s) == %s, wanted %s" % (
                    input, actual, expected)
                )



