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

import smtplib

from testutils import mock
from conary import changemail
from conary_test import rephelp


class ChangemailTest(rephelp.RepositoryHelper):
    def testMaxsize(self):
        # scripts should not normally be in the path!
        # This whole workaround is ugly -- all the changemail
        # bits should be in conary, not in scripts.
        f = file(self.workDir+'/changemailtmp', 'w+')
        f.write(' '*1024)
        f.flush()
        SMTP = mock.MockObject()
        self.mock(smtplib, 'SMTP', SMTP)
        changemail.sendMail(f, 'subject', 'from', 40, ['to'])
        f.seek(0)
        self.assertEquals(
            f.read(),
            ' '*34 + '\n...\n')
        SMTP().sendmail._mock.assertCalled(
            'from', ['to'],
            'Content-Type: text/plain; charset="us-ascii"\n'
            'MIME-Version: 1.0\n'
            'Content-Transfer-Encoding: 7bit\n'
            'Subject: subject\n'
            'From: from\n'
            'To: to\n\n'
            '                                  \n...\n')
        SMTP().connect._mock.assertCalled('localhost')

        # now test some binary bytes
        f = file(self.workDir+'/changemailtmp', 'w+')
        # Unfortunately, literal UTF-8 characters confuse our
        # changemail.
        f.write('Th\xc3\xads \xc3\xacs a te\xc5\x9ft of \xc3\x9fome'
                ' r\xc3\xa4th\xc3\xaar\xc3\xbf unlikely bytes\n')
        f.flush()
        changemail.sendMail(f, 'subject', 'from', 2048, ['to'])
        SMTP().sendmail._mock.assertCalled(
            'from',
            ['to'],
            'Content-Type: text/plain; charset="us-ascii"\n'
            'MIME-Version: 1.0\n'
            'Content-Transfer-Encoding: 8bit\n'
            'Subject: subject\n'
            'From: from\n'
            'To: to\n\n'
            'Th\xc3\xads \xc3\xacs a te\xc5\x9ft of \xc3\x9fome'
            ' r\xc3\xa4th\xc3\xaar\xc3\xbf unlikely bytes\n')
        SMTP().connect._mock.assertCalled('localhost')
