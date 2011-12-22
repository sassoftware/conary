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

import StringIO
import sys

from conary.lib import formattrace

class ReprTest(testhelp.TestCase):
    def testFormatLocals(self):
        # The pretty printer works on the representation of the object, so
        # we need to include the quotes in the calculation
        stringobj1 = '0123456789' * 159 + 'abcdefgh'
        stringobj2 = stringobj1 + 'i'
        unicodeobj1 = u'0123456789' * 159 + u'abcdefgh'
        unicodeobj2 = unicodeobj1 + u'i'
        listobj1 = [ 1 ] * 20
        listobj2 = listobj1 + [ 'a' ]
        sio = StringIO.StringIO()
        frame = sys._getframe()
        formattrace.formatLocals(frame, sio)

        sio.seek(0)

        for line in sio:
            varName, varVal = [ x.strip() for x in line.split(':', 2) ]
            if varName == 'stringobj1':
                self.failUnlessEqual(varVal, repr(stringobj1))
            elif varName == 'unocodeobj1':
                self.failUnlessEqual(varVal, repr(unicodeobj1))
            elif varName == 'listobj1':
                self.failUnlessEqual(varVal, repr(listobj1))
            elif varName == 'stringobj2':
                self.failIfEqual(varVal, repr(stringobj2))
            elif varName == 'unocodeobj2':
                self.failIfEqual(varVal, repr(unicodeobj2))
            elif varName == 'listobj2':
                self.failIfEqual(varVal, repr(listobj2))
