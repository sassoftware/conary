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

from conary_test import rephelp

from conary.lib import api

class ApiTest(rephelp.RepositoryHelper):
    def testPublicApiDecorator(self):
        class A:
            def a(self):
                "function test a"
            @api.publicApi
            def b(self):
                "function test b"
        foo = A()
        self.failIf(foo.a.__doc__.endswith("(PUBLIC API)"))
        if not foo.b.__doc__.endswith("(PUBLIC API)"):
            raise RuntimeError('foo.b.__doc__ == %r' % foo.b.__doc__)
