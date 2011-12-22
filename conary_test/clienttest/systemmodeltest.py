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


"""
Tests for functions in the systemmodel module
"""
import os
import stat

from testutils import mock
from testrunner.testhelp import context

from conary_test import rephelp

from conary.conaryclient import cml, systemmodel
from conary.deps import deps
from conary.lib import util


class SystemModelFileTest(rephelp.RepositoryHelper):

    def getSystemModel(self, *args):
        cfg = mock.MockObject()
        cfg._mock.set(installLabelPath = ['a@b:c', 'd@e:f' ])
        cfg._mock.set(flavor = deps.parseFlavor(''))
        cfg._mock.set(root = self.rootDir)
        model = cml.CML(cfg)
        return systemmodel.SystemModelFile(model, *args)

    @context('sysmodel')
    def testInit(self):
        smf = self.getSystemModel('/fake')
        self.assertEquals(smf.fileName, '/fake')
        self.assertEquals(smf.fileFullName, self.rootDir + '/fake')
        self.assertEquals(smf.model.filedata, [])
        file(self.rootDir + '/fake', 'w').write('# comment\n')
        smf.read()
        self.assertEquals(smf.model.filedata, ['# comment\n'])
        smf.parse() # does not raise an exception

        smf = self.getSystemModel('/fake')
        self.assertEquals(smf.model.filedata, ['# comment\n'])
        self.assertEquals(smf.snapshotExists(), False)

    @context('sysmodel')
    def testSnapshot(self):
        file(self.rootDir + '/fake', 'w').write('# comment\n')
        smf = self.getSystemModel('/fake')
        self.assertEquals(smf.model.filedata, ['# comment\n'])
        self.assertEquals(smf.snapshotExists(), False)
        self.assertEquals(smf.exists(), True)

        smf.writeSnapshot()
        self.assertEquals(smf.snapshotExists(), True)
        self.assertEquals(file(self.rootDir + '/fake.next', 'r').read(),
            '# comment\n')
        smf.closeSnapshot()
        self.assertEquals(smf.snapshotExists(), False)
        self.assertEquals(util.exists(self.rootDir + '/fake.next'), False)

        file(self.rootDir + '/fake.next', 'w').write('# comment\ninstall foo\n')
        smf = self.getSystemModel('/fake')
        self.assertEquals(smf.model.filedata, ['# comment\n', 'install foo\n'])
        self.assertEquals(file(self.rootDir + '/fake.next', 'r').read(),
            '# comment\n'
            'install foo\n')
        self.assertEquals(smf.snapshotExists(), True)
        self.assertEquals(smf.exists(), True)
        smf.closeSnapshot()
        self.assertEquals(smf.snapshotExists(), False)
        self.assertEquals(util.exists(self.rootDir + '/fake.next'), False)

        smf.writeSnapshot()
        self.assertEquals(smf.snapshotExists(), True)
        smf.deleteSnapshot()
        self.assertEquals(smf.snapshotExists(), False)

    @context('sysmodel')
    def testStartFromScratch(self):
        smf = self.getSystemModel('/fake')

        smf.parse(fileData=['# an initial comment\n'])
        smf.write()
        self.assertEquals(file(self.rootDir + '/fake').read(),
            '# an initial comment\n')

        smf.model.appendOpByName('update', 'foo')
        smf.write()
        self.assertEquals(file(self.rootDir + '/fake').read(),
            '# an initial comment\n'
            'update foo\n')

        smf.model.appendOp(cml.SearchLabel('a@b:c'))
        smf.write()
        self.assertEquals(file(self.rootDir + '/fake').read(),
            '# an initial comment\n'
            'update foo\n'
            'search a@b:c\n')

        smf.write('/asdf')
        self.assertEquals(file(self.rootDir + '/asdf').read(),
            '# an initial comment\n'
            'update foo\n'
            'search a@b:c\n')
        self.assertEquals(
            stat.S_IMODE(os.stat(self.rootDir + '/asdf')[stat.ST_MODE]),
            0644)

        os.chmod(self.rootDir + '/asdf', 0640)
        smf.write('/asdf')
        self.assertEquals(
            stat.S_IMODE(os.stat(self.rootDir + '/asdf')[stat.ST_MODE]),
            0640)

    @context('sysmodel')
    def testParseWrite(self):
        fileData = '\n'.join((
            '# Initial comment',
            'search group-foo=a@b:c/1-1-1',
            '# comment 2',
            'update foo #disappearing act',
            '# comment 3',
            'install bar==a@b:c/1-1-1',
            '# comment 4',
            'patch baz',
            '# comment 5',
            'erase blah',
            '# comment 6',
            '',
        ))
        file(self.rootDir + '/real', 'w').write(fileData)
        smf = self.getSystemModel('/real')
        self.assertEquals(smf.model.format(), fileData)
        smf.write('/copy')
        self.assertEquals(file(self.rootDir + '/copy').read(), fileData)
        smf.model.modelOps[1].modified=True
        modFileData = fileData.replace(' #disappearing act', '')
        self.assertEquals(smf.model.format(), modFileData)
        smf.model.appendOp(cml.UpdateTroveOperation('newtrove'))
        modFileData = modFileData.replace('erase blah\n',
                                          'erase blah\nupdate newtrove\n')
        self.assertEquals(smf.model.format(), modFileData)
        smf.model.appendOp(cml.SearchLabel('d@e:f'))
        modFileData = modFileData.replace('update newtrove\n',
                                          'update newtrove\nsearch d@e:f\n')
        self.assertEquals(smf.model.format(), modFileData)

    @context('sysmodel')
    def testParseFail(self):
        
        file(self.rootDir + '/real', 'w').write('\n'.join((
            'badverb noun',
        )))
        e = self.assertRaises(cml.CMError,
            self.getSystemModel, '/real')
        self.assertEquals(str(e), '/real:1: Unrecognized command "badverb"')

        file(self.rootDir + '/real', 'w').write('\n'.join((
            'search foo=bar=baz@blah@blah:1-1-1-1-1',
        )))
        e = self.assertRaises(cml.CMError,
            self.getSystemModel, '/real')
        self.assertEquals(str(e),
            '/real:1: Error with spec "foo=bar=baz@blah@blah:1-1-1-1-1":'
            " Too many ='s")



    @context('sysmodel')
    def testEmptyEverything(self):
        smf = self.getSystemModel('/fake')
        smf.write()
        self.assertEquals(file(self.rootDir + '/fake').read(), '')
