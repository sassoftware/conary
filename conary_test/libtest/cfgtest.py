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


import os
import pickle
import cPickle
import re
from testrunner import testhelp
import shutil

import tempfile
from StringIO import StringIO

#conary
#from conary.conarycfg import *
from conary.conarycfg import (
        CfgFingerPrintMap,
        CfgFlavor,
        CfgLabel,
        )
from conary.lib import cfgtypes
from conary.lib.cfg import (
        ConfigFile,
        ConfigSection,
        ParseError,
        SectionedConfigFile,
        )
from conary.lib.cfgtypes import (
        CfgBool,
        CfgEnum,
        CfgEnumDict,
        CfgInt,
        CfgList,
        CfgPath,
        CfgQuotedLineList,
        CfgRegExpList,
        CfgString,
        CfgEnvironmentError
        )
from conary import versions


class ConfigTest(testhelp.TestCase):

    def _getDisplayLines(self, cfg):
        out = StringIO()
        cfg.display(out)
        lines = out.getvalue()
        # remove extra formatting spaces 
        lines = re.sub(' +', ' ', lines)
        # remove ending \n to avoid an extra '' in the resulting list
        lines = lines[:-1]
        return lines.split('\n')

    def _getFileLines(self, cfg, includeDocs=True):
        out = StringIO()
        cfg._write(out, cfg._displayOptions, includeDocs=includeDocs)
        lines = out.getvalue()
        # remove extra formatting spaces 
        lines = re.sub(' +', ' ', lines)
        # remove ending \n to avoid an extra '' in the resulting list
        lines = lines[:-1]
        return lines.split('\n')

    def testConfigComments(self):
        class TestCfgFile(ConfigFile):
            foo = None

        cfg = TestCfgFile()
        assert(cfg.foo == None)
        cfg.configLine('foo newval#comment')
        assert(cfg.foo == 'newval')
        cfg.configLine('foo newval#comment')
        assert(cfg.foo == 'newval')
        cfg.configLine(r'foo newval\#comment')
        assert(cfg.foo == 'newval#comment')
        cfg.configLine(r'foo newval\\#comment')
        assert(cfg.foo == 'newval\\')

    def testConfigBool(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgBool, False) 

        cfg = TestCfgFile()
        assert(cfg.foo == False)
        assert(cfg['foo'] == False)
        cfg.configLine('foo True')
        assert(cfg.foo == True)
        cfg.foo = False
        assert(cfg.foo == False)
        cfg.configLine('foo True')
        assert(cfg.foo == True)
        assert(self._getDisplayLines(cfg) == ['foo True'])

    def testConfigPath(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgPath, False) 

        oldEnv = os.environ.get('FOOBAR', None)
        oldHome = os.environ.get('HOME', None)
        os.environ['FOOBAR'] = 'foobar'
        os.environ['HOME'] = '/tmp'
        try:
            cfg = TestCfgFile()
            cfg.configLine('foo $FOOBAR/bash')
            assert(self._getDisplayLines(cfg) == ['foo $FOOBAR/bash'])

            cfg = TestCfgFile()
            cfg.configLine('foo ~/bash')
            assert(self._getDisplayLines(cfg) == ['foo ~/bash'])
            cfg.setDisplayOptions(expandPaths=True)
            assert(self._getDisplayLines(cfg) == ['foo %s/bash' % os.environ['HOME']])
        finally:
            if oldEnv is None:
                del os.environ['FOOBAR']
            else:
                os.environ['FOOBAR'] = oldEnv
            if oldHome is None:
                del os.environ['HOME']
            else:
                os.environ['HOME'] = oldHome

    def testConfigPathCache(self):
        oldHome = os.environ.get('HOME', None)
        oldEnv = os.environ.get('FOOBAR', None)
        try:
            os.environ['HOME'] = '/tmp'
            p = cfgtypes.Path('~/foo')
            assert(p == '/tmp/foo')
            os.environ['HOME'] = '/tmp2'
            p = cfgtypes.Path('~/foo')
            assert(p == '/tmp2/foo')

            os.environ['FOOBAR'] = '/tmp'
            p = cfgtypes.Path('$FOOBAR/foo')
            assert(p == '/tmp/foo')
            os.environ['FOOBAR'] = '/tmp2'
            p = cfgtypes.Path('$FOOBAR/foo')
            assert(p == '/tmp2/foo')
        finally:
            if oldEnv is None:
                del os.environ['FOOBAR']
            else:
                os.environ['FOOBAR'] = oldEnv
            if oldHome is None:
                del os.environ['HOME']
            else:
                os.environ['HOME'] = oldHome



    def testConfigInt(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgInt, 0) 

        cfg = TestCfgFile()
        assert(cfg.foo == 0)
        cfg.configLine('foo 1 # bar')
        assert(cfg.foo == 1)
        assert(self._getDisplayLines(cfg) == ['foo 1'])

    def testConfigRegExp(self):
        class TestCfgFile(ConfigFile):
            foo  = CfgRegExpList

        cfg = TestCfgFile()
        assert(cfg.foo == [])
        cfg.configLine('foo re1.* re2.*# comments')
        assert([x[0] for x in cfg.foo] == ['re1.*', 're2.*'])
        lines = self._getDisplayLines(cfg)
        lines.remove('foo re1.*')
        lines.remove('foo re2.*')
        assert(not lines)
        
    def testConfigLabel(self):
        class TestCfgFile(ConfigFile):
            foo  = (CfgLabel, versions.Label('localhost@local:local'))

        cfg = TestCfgFile()
        assert(cfg.foo.asString() == 'localhost@local:local')
        cfg.configLine('foo conary.rpath.com@rpl:devel')
        assert(cfg.foo.asString() == 'conary.rpath.com@rpl:devel')

    def testConfigFlavor(self):
        class TestCfgFile(ConfigFile):
            foo = CfgFlavor

        cfg = TestCfgFile()
        assert(str(cfg.foo) == '')
        cfg.configLine('foo ~foo1, ~!foo2, foo3, foo4, foo5, foo6, foo7, foo8, foo9, foo10, foo11, foo12, foo13')
        assert(str(cfg.foo) == '~foo1,foo10,foo11,foo12,foo13,'
                               '~!foo2,foo3,foo4,foo5,foo6,foo7,foo8,foo9')
        cfg.setDisplayOptions(prettyPrint=True)
        lines = self._getDisplayLines(cfg)
        lines.remove('foo ~foo1, foo10, foo11, foo12, foo13, ~!foo2, foo3,')
        lines.remove(' foo4, foo5, foo6, foo7, foo8, foo9')
        assert(not lines)
        cfg.setDisplayOptions(prettyPrint=False)
        lines = self._getDisplayLines(cfg)
        lines.remove('foo ~foo1, foo10, foo11, foo12, foo13, ~!foo2, foo3, foo4, foo5, foo6, foo7, foo8, foo9')

    def testConfigString(self):
        class TestCfgFile(ConfigFile):
            foo = CfgString

        cfg = TestCfgFile()
        assert(cfg.foo == None)
        cfg.configLine('foo blah blah')
        assert(cfg.foo == 'blah blah')

    def testConfigStringList(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgList(CfgString), ['default'])

        cfg = TestCfgFile()
        assert(cfg.foo == ['default'])
        cfg.foo.append('bar')
        assert(cfg.foo == ['default', 'bar'])
        cfg.configLine('foo blah blah')
        assert(cfg.foo == ['default', 'bar', 'blah blah'])
        cfg.configLine('foo more')
        assert(cfg.foo == ['default', 'bar', 'blah blah', 'more'])
        lines = self._getDisplayLines(cfg)
        assert(lines == ['foo default', 'foo bar', 'foo blah blah', 'foo more'])

    def testConfigQuotedLineList(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgQuotedLineList(CfgString), ['default'])

        cfg = TestCfgFile()
        assert(cfg.foo == ['default'])
        cfg.configLine("foo 'bar bam' blah         bz")
        assert(cfg.foo == ['bar bam', 'blah', 'bz'])
        lines = self._getDisplayLines(cfg)
        assert(lines == ["foo 'bar bam' 'blah' 'bz'"])

    def testConfigLabelList(self):
        defaultLabel = versions.Label('conary.rpath.com@rpl:devel')
        localLabel = versions.Label('localhost@rpl:devel')

        class TestCfgFile(ConfigFile):
            foo = (CfgList(CfgLabel), [defaultLabel])

        cfg = TestCfgFile()
        assert(cfg.foo == [defaultLabel])
        cfg.configLine('foo localhost@rpl:devel')
        assert(cfg.foo == [localLabel])
        cfg.configLine('foo conary.rpath.com@rpl:devel')
        assert(cfg.foo == [localLabel, defaultLabel])
        lines = self._getDisplayLines(cfg)
        assert(lines == ['foo localhost@rpl:devel', 
                         'foo conary.rpath.com@rpl:devel'])

    def testDocs(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgString, '/tmp/foo', "Bogus Path")

        cfg = TestCfgFile()
        cfg.foo == '/tmp/foo'
        lines = self._getFileLines(cfg)
        assert(lines == ['# foo (Default: /tmp/foo)', 
                         '# Bogus Path', 
                         'foo /tmp/foo'])

    def testLineOrigins(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgString, '/tmp/foo')
            bar = (CfgList(CfgString), None)
            bam = (CfgString, '')
        cfg = TestCfgFile()
        cfg.configLine('foo /bar', fileName='/blah', lineno=32)
        cfg.configLine('foo /bam', fileName='/blah', lineno=33)
        cfg.configLine('bar one', fileName='<override>', lineno='<No Line>')
        cfg.configLine('bar two', fileName='blam', lineno=3)
        cfg.setDisplayOptions(showLineOrigins=True)
        lines = self._getFileLines(cfg, includeDocs=False)
        assert(lines == ['bam ',
                         '# bar: <override> blam',
                         'bar one',
                         'bar two',
                         '# foo: /blah',
                         'foo /bam'])

    def testCfgEnum(self):
        class TestCfgEnum(CfgEnum):
            # exersice enum list codepath
            validValues = ['FOO', 'BAR', 'BAZ']

        class TestCfgFile(ConfigFile):
            foo = (TestCfgEnum(), 'BAR')

        cfg = TestCfgFile()
        self.assertFalse(cfg.foo != 'BAR',
                    'Initial value not set to default')

        cfg.configLine('foo baz')
        self.assertFalse(cfg.foo != 'BAZ',
                    'Enum value not translated to validValue')

        tCfg = TestCfgEnum()
        self.assertFalse(tCfg.format('FOO', {}) != 'FOO',
                    "validValue not translated to origValue")

        self.assertRaises(ParseError, tCfg.parseString, 'splart')
        self.assertRaises(ParseError, tCfg.format, 'foo', {})

        self.assertFalse(tCfg.parseString('bar') != 'BAR',
                    "Enum value translating not case insensitive")

        self.assertFalse(tCfg.validValues != \
                    {'baz': 'BAZ', 'foo': 'FOO', 'bar': 'BAR'},
                    "validValues Dict incorrect")
        self.assertFalse(tCfg.origName != \
                    {'BAZ': 'BAZ', 'FOO': 'FOO', 'BAR': 'BAR'},
                    "origValues Dict incorrect")

    def testCfgEnum2(self):
        class TestCfgEnum(CfgEnum):
            # exercise enum dict codepath
            validValues = {'FOO': 1, 'BAR': 2, 'BAZ': 3}

        class TestCfgFile(ConfigFile):
            foo = (TestCfgEnum(), 2)

        testEnum = TestCfgEnum()
        self.assertFalse(testEnum.parseString('foo') != 1,
                    "string not parsed correctly")

        self.assertFalse(testEnum.format(1, {}) != 'FOO',
                    "Enumerated value was not translated to original value")

        tCfg = TestCfgEnum()
        self.assertFalse(tCfg.validValues != \
                    {'baz': 3, 'foo': 1, 'bar': 2},
                    "validValues Dict incorrect")
        self.assertFalse(tCfg.origName != \
                    {3: 'BAZ', 1: 'FOO', 2: 'BAR'},
                    "origValues Dict incorrect")

    def testConfigEnumDict(self):
        class CfgEnumDictFoo(CfgEnumDict):
            validValues = {'key1': ('val1', 'val2'), 'key2': ('val3', 'val4')}

        class TestCfgFile(ConfigFile):
            foo = CfgEnumDictFoo

        cfg = TestCfgFile()
        cfg.configLine('foo key1 val1')
        cfg.configLine('foo key2 val3')
        assert(cfg.foo == {'key1' : 'val1', 'key2' : 'val3'})

    def testRegularExp(self):
        class TestCfgFile(ConfigFile):
            foo = CfgRegExpList

        cfg = TestCfgFile()
        cfg.configLine('foo bar.*')
        cfg.configLine('foo baz.*')
        assert(cfg.foo.match('bar'))
        assert(cfg.foo.match('baz'))
        assert(not cfg.foo.match('blammo'))

    def testFingerprintMap(self):
        class TestCfgFile(ConfigFile):
            foo = CfgFingerPrintMap

        cfg = TestCfgFile()
        cfg.configLine('foo .* None')
        assert (cfg.foo == [('.*', None)])
        self.assertRaises(ParseError, cfg.configLine, 'foo * None')

    def testSectionedCfg(self):
        class TestCfgSection(ConfigSection):
            foo = CfgBool

        class TestCfgFile(SectionedConfigFile):
            bar = TestCfgSection, None, 'Docs'

        cfg = TestCfgFile()
        self.assertRaises(ParseError, cfg.configLine, 'bar')
        self.assertRaises(ParseError, cfg.configLine, 'foo')
        assert(not cfg.bar.foo)
        cfg.configLine('[bar]')
        cfg.configLine('foo True')
        assert(cfg.bar.foo)

    def testIncludeConfigFile(self):
        class TestCfgFile(ConfigFile):
            bam = CfgBool
            biff = CfgBool

        dir = tempfile.mkdtemp()
        _home = os.getenv('HOME', '')
        try:
            open('%s/foo' % dir, 'w').write('inCLUDEconFIGFile bar\n'
                'includeCONFIGfile ~/baz\n')
            open('%s/bar' % dir, 'w').write('bam True\n')

            os.mkdir(os.path.join(dir, 'abode'))
            open('%s/abode/baz' % dir, 'w').write('biff True\n')
            os.environ['HOME'] = os.path.join(dir, 'abode')

            cfg = TestCfgFile()
            assert(not cfg.bam)
            cfg.read('%s/foo' % dir)
            assert(cfg.bam and cfg.biff)
        finally:
            os.environ['HOME'] = _home
            shutil.rmtree(dir)

    def testIncludeConfigFileFails(self):
        class TestCfgFile(ConfigFile):
            bam = CfgBool

        dir = tempfile.mkdtemp()
        try:
            open('%s/foo' % dir, 'w').write('inCLUDEconFIGFile http://nonesuchrediculoushostexists//bar\n')
            cfg = TestCfgFile()
            assert(not cfg.bam)
            try:
                cfg.read('%s/foo' % dir)
                assert(0)
            except Exception, e:
                if 'failure in name resolution' in str(e):
                    raise testhelp.SkipTestException('requires default route')
                desc, sep, err = str(e).partition('bar: ')
                self.assertEqual(desc, "%s/foo:1: when processing "
                    "inCLUDEconFIGFile: Error reading config file "
                    "http://nonesuchrediculoushostexists//" %dir)
                self.assertIn(err, [
                    "Name or service not known",
                    "No address associated with hostname"])

            cfg = TestCfgFile()
            cfg.ignoreUrlIncludes()
            cfg.read('%s/foo' % dir)
            cfg = TestCfgFile()
            cfg.ignoreUrlIncludes()
            cfg.ignoreUrlIncludes(False)
            self.assertRaises(Exception, cfg.read,'%s/foo' % dir)
            cfg = TestCfgFile()
            cfg.setIgnoreErrors()
            cfg.read('%s/foo' % dir)
        finally:
            shutil.rmtree(dir)

    def testLimitKeys(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgString, 'foo')
            bar = (CfgString, 'bar')
        cfg = TestCfgFile()
        cfg.limitToKeys('foo')
        cfg.configLine('bar bam')
        assert(cfg.bar == 'bar')
        cfg.configLine('foo bam')
        assert(cfg.foo == 'bam')
        cfg.limitToKeys(False)
        cfg.configLine('bar bam')
        assert(cfg.bar == 'bam')

    def testStringListOverrideDefault(self):
        class TestCfgFile(ConfigFile):
            bam = CfgList(CfgString, default=['foo'])
        dir = tempfile.mkdtemp()
        try:
            open('%s/foo' % dir, 'w').write('bam foo\nbam bar\n')
            cfg = TestCfgFile()
            cfg.read('%s/foo' % dir)
            assert(cfg.bam == ['foo', 'bar'])
        finally:
            shutil.rmtree(dir)

    def testDefaultFlag(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgString, 'foo')
        cfg = TestCfgFile()
        assert cfg.isDefault('foo')

        cfg['foo'] = 'bar'
        assert not cfg.isDefault('foo')

        cfg.resetToDefault('foo')
        self.assertEquals(cfg.foo, 'foo')
        assert cfg.isDefault('foo')

        cfg.foo = 'bar'
        assert not cfg.isDefault('foo')

    def testReadFileDoesntExist(self):
        class TestCfgFile(ConfigFile):
            foo = (CfgString, 'foo')
        cfg = TestCfgFile()
        ex = self.assertRaises(CfgEnvironmentError, cfg.read,
                '/tmp/doesntexist/foo', exception=True)
        assert(str(ex) == "Error reading config file /tmp/doesntexist/foo: No "
            "such file or directory")

    def testPickle(self):
        cfg = PickleTestConfig()
        cfg.bar.append(3)
        cfg.addListener('foo', lambda x: None)

        sio = StringIO()
        cfg.store(sio)
        reference = sio.getvalue()

        for mod in (pickle, cPickle):
            for level in (0, 1, 2):
                pickled = mod.dumps(cfg, level)
                restored = mod.loads(pickled)

                sio = StringIO()
                restored.store(sio)
                self.assertEquals(sio.getvalue(), reference)


# This has to be at module level or pickle can't find it.
class PickleTestConfig(ConfigFile):
    foo = (CfgList(CfgInt), [1])
    bar = (CfgList(CfgInt), [2])
