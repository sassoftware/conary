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


import os
import sys

import pydoc

from conary_test import rephelp

from conary.build import explain

class ExplainTest(rephelp.RepositoryHelper):
    def testTextPager(self):
        class MockStdout(object):
            "Dummy doc string"
            isatty = lambda x: False
            write = lambda x, y: None
        def goodPager(*args, **kwargs):
            self.called = True
        def badPager(*args, **kwargs):
            raise RuntimeError("Pipe pager should not have been called")
        self.called = False
        self.mock(sys, 'stdout', MockStdout())
        self.mock(pydoc, 'pager', goodPager)
        self.mock(pydoc, 'pipepager', badPager)
        explain._formatDoc('Stdout', MockStdout)

    def testPipePager(self):
        class MockStdout(object):
            "Dummy doc string"
            isatty = lambda x: True
            write = lambda x, y: None
        def goodPager(*args, **kwargs):
            self.called = True
        def badPager(*args, **kwargs):
            raise RuntimeError("Text pager should not have been called")
        self.called = False
        self.mock(sys, 'stdout', MockStdout())
        self.mock(pydoc, 'pager', badPager)
        self.mock(pydoc, 'pipepager', goodPager)
        explain._formatDoc('Stdout', MockStdout)

    def testFormatDoc(self):
        'test various formatting: reindent, headers, tags'
        class MockStdout(object):
            """
            NAME
            ====
            B{Dummy} {doc} I{string} B{with B{nested} text}
            """
            isatty = lambda x: False
            write = lambda x, y: None
        def mockPager(text):
            refText = (
              'Conary API Documentation: \x1b[1mStdout.MockStdout\x1b[21m\n\n'
              '\x1b[1mNAME\x1b[21m\n'
              '    \x1b[1mDummy\x1b[21m {doc} \x1b[7mstring\x1b[27m \x1b[1mwith nested text\x1b[21m\n    ')
            self.assertEquals(text, refText)
        self.mock(sys, 'stdout', MockStdout())
        self.mock(pydoc, 'pager', mockPager)
        explain._formatDoc('Stdout', MockStdout)

    def testFormatString(self):
        oldPager = os.environ.pop('PAGER', '')
        # test that italic formatting uses reverse video
        self.assertEquals(explain._formatString('I{italic}'),
            '\x1b[7mitalic\x1b[27m')
        # test that constant width formatting uses underscores
        self.assertEquals(explain._formatString('C{cw}'),
            '\x1b[4mcw\x1b[24m')
        # test that bold formatting uses bold
        self.assertEquals(explain._formatString('B{foo}'),
            '\x1b[1mfoo\x1b[21m')
        # test that formatting stacks
        self.assertEquals(explain._formatDocString('C{B{foo}}'),
            '\x1b[4m\x1b[1mfoo\x1b[21m\x1b[24m')
        # inner bold is erased by outer bold because CSR codes do not nest
        self.assertEquals(explain._formatDocString('B{C{B{foo}}}'),
            '\x1b[1m\x1b[4mfoo\x1b[24m\x1b[21m')
        # test that bold text is not re-bolded
        self.assertEquals(explain._formatString('B{\x1b[1mfoo\x1b[21m}'),
                '\x1b[1mfoo\x1b[21m')
        self.assertEquals(explain._formatDocString('B{B{foo}}'),
                '\x1b[1mfoo\x1b[21m')
        os.environ['PAGER'] = oldPager

    def testNoDoc(self):
        class NoDocs(object):
            pass
        oldPager = os.environ.pop('PAGER', '')
        rc, output = self.captureOutput(explain._formatDoc, 'NoDocs', NoDocs)
        expected = ('Conary API Documentation: \x1b[1mNoDocs.NoDocs\x1b[21m\n'
                    'No documentation available.')
        self.assertEqual(output, expected)
        os.environ['PAGER'] = 'more'
        rc, output = self.captureOutput(explain._formatDoc, 'NoDocs', NoDocs)
        expected = ('Conary API Documentation: NoDocs.NoDocs\n'
                    'No documentation available.')
        os.environ['PAGER'] = oldPager
        self.assertEqual(output, expected)

    def testExplainRecipe(self):
        def mockFormatDoc(className, obj):
            self.assertFalse(not 'r.Move' in obj.__doc__)
            self.assertEquals(obj.__module__, 'conary.build.build')
        self.mock(explain, '_formatDoc', mockFormatDoc)
        explain.docObject(self.cfg, 'Move')

    def testCvcExplainSource(self):
        def mockFormatDoc(className, obj):
            self.assertFalse(not 'r.addSource' in obj.__doc__)
            self.assertEquals(obj.__module__, 'conary.build.source')
        self.mock(explain, '_formatDoc', mockFormatDoc)
        res = self.captureOutput(explain.docObject, self.cfg, 'addSource')
        self.assertFalse('Ambiguous recipe method "addSource"' in res[1])

    def testCvcExplainPolicy(self):
        def mockFormatDoc(className, obj):
            self.assertFalse(not 'r.Config' in obj.__doc__)
            self.assertEquals(obj.__module__, 'conary.build.packagepolicy')
        self.mock(explain, '_formatDoc', mockFormatDoc)
        explain.docObject(self.cfg, 'Config')

    def testCvcExplainGroup(self):
        def mockFormatDoc(className, obj):
            self.assertFalse(not 'r.addCopy' in obj.__doc__)
            self.assertEquals(obj.__module__, 'conary.build.grouprecipe')
        self.mock(explain, '_formatDoc', mockFormatDoc)
        explain.docObject(self.cfg, 'addCopy')

    def testCvcExplainGroupPolicy(self):
        def mockFormatDoc(className, obj):
            self.assertFalse(not 'r.VersionConflicts' in obj.__doc__)
            self.assertEquals(obj.__module__, 'group_versionconflicts')
        self.mock(explain, '_formatDoc', mockFormatDoc)
        explain.docObject(self.cfg, 'VersionConflicts')

    def testCvcExplainInfo(self):
        def mockFormatDoc(className, obj):
            self.assertFalse(not 'r.User' in obj.__doc__)
            self.assertEquals(obj.__module__, 'conary.build.build')
        self.mock(explain, '_formatDoc', mockFormatDoc)
        explain.docObject(self.cfg, 'UserInfoRecipe.User')

    def testCvcExplainFailure(self):
        #rc, txt = self.captureOutput(explain.docObject, self.cfg,
                                     #'AClassNotImplemented')
        #self.assertEqual(txt,
                #'Unknown recipe method "AClassNotImplemented"\n')

        rc, txt = self.captureOutput(explain.docObject, self.cfg,
                                     'Requires')
        self.assertEqual(txt,
                     'Ambiguous recipe method "Requires" is defined by the '
                     'following classes:\n'
                     '    GroupRecipe, PackageRecipe\n'
                     'Specify one of: GroupRecipe.Requires, '
                     'PackageRecipe.Requires\n')

        rc, txt = self.captureOutput(explain.docObject, self.cfg,
                                     'User.foo.bar')
        self.assertEqual(txt,
                             'Too may "." specified in "User.foo.bar"\n')

    def testExplainAll(self):
        def mockPageDoc(className, obj):
            self.assertEquals('All Classes', className)
            self.assertFalse('Available Classes' not in obj)
        self.mock(explain, '_pageDoc', mockPageDoc)
        explain.docAll(self.cfg)

    def testExplainPkgRecipe(self):
        def mockPageDoc(className, obj):
            self.assertEquals('PackageRecipe', className)
            self.assertFalse('PackageRecipe' not in obj)
        self.mock(explain, '_pageDoc', mockPageDoc)
        explain.docObject(self.cfg, 'PackageRecipe')

    def testExplainGroupRecipe(self):
        def mockPageDoc(className, obj):
            self.assertEquals('GroupRecipe', className)
            self.assertFalse('GroupRecipe' not in obj)
        self.mock(explain, '_pageDoc', mockPageDoc)
        explain.docObject(self.cfg, 'GroupRecipe')

    def testExplainUserInfoRecipe(self):
        def mockPageDoc(className, obj):
            self.assertEquals('UserInfoRecipe', className)
            self.assertFalse('Build Actions' not in obj)
        self.mock(explain, '_pageDoc', mockPageDoc)
        explain.docObject(self.cfg, 'UserInfoRecipe')

    def testExplainGroupInfoRecipe(self):
        def mockPageDoc(className, obj):
            self.assertEquals('GroupInfoRecipe', className)
            self.assertFalse('Build Actions' not in obj)
        self.mock(explain, '_pageDoc', mockPageDoc)
        explain.docObject(self.cfg, 'GroupInfoRecipe')
