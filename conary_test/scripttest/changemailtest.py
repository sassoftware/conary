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
