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
import unittest
import difflib

from conary.lib.patch import patch, reverse, unifiedDiff
from conary_test import resources


class PatchTest(unittest.TestCase):
    def runTest(self, fileName, sedCmd, changeCmd = None):
        new = os.popen("sed '%s' < %s" % (sedCmd, fileName)).readlines()

        orig = open(fileName).readlines()

        if changeCmd:
            target = os.popen("sed '%s' < %s" % (changeCmd, fileName)).readlines()
            final = os.popen("sed '%s' < %s | sed '%s'" % (sedCmd, fileName, 
                             changeCmd)).readlines()
            check = os.popen("sed '%s' < %s | sed '%s'" % (changeCmd, fileName, 
                             sedCmd)).readlines()
            if "".join(final) != "".join(check):
                self.fail("sed scripts have conflicting results")
        else:
            target = orig
            final = new

        diff = difflib.unified_diff(orig, new)
        # advance past header
        diff.next()
        diff.next()
        d = [ x for x in diff ]

        (new2, conflicts) = patch(target, d)

        diff = difflib.unified_diff(final, new2)
        try:
            diff.next()
            print "%s '%s' failed:" % (fileName, sedCmd)
            diff.next()
            for line in diff:
                line = line[:-1]
                print "\t%s" % line
        except StopIteration:
            pass

    def test1(self):
        self.runTest(resources.get_archive() + "/services", "s/ircd/foobard/")

    def test2(self):
        self.runTest(resources.get_archive() + "/services", "s/chargen/mygen/;s/tftp/mytftp/")

    def test3(self):
        self.runTest(resources.get_archive() + "/services", "/chargen/ahello there")

    def test4(self):
        self.runTest(resources.get_archive() + "/services", "/tftp/d")

    def test5(self):
        self.runTest(resources.get_archive() + "/services", "s/tftp/mytftp/", "s/telnet/mytelnet/")

    def test6(self):
        self.runTest(resources.get_archive() + "/services", "s/tftp/mytftp/", "/chargen/d")

    def test7(self):
        self.runTest(resources.get_archive() + "/services", "s/tftp/mytftp/",
                     "/chargen/asome new lines")

    def test8(self):
        self.runTest(resources.get_archive() + "/tmpwatch", "/done/a # local changes",
                     "s/720/360/")

    def test9(self):
        old = []
        new = [ "some lines", "of text", "to be patched" ]

        diff = difflib.unified_diff(old, new, lineterm = "")
        # advance past header
        diff.next()
        diff.next()

        (new2, conflicts) = patch(old, diff)
        assert(not conflicts)
        diff = difflib.unified_diff(new, new2)
        self.assertRaises(StopIteration, diff.next)

    def test10(self):
        """test reversing a diff and applying it to the new file, check
        too make sure you get the old file"""
        old = [ "a", "b", "c", "same" ]
        new = [ "1", "2", "3", "same" ]

        diff = difflib.unified_diff(old, new, lineterm = "")
        # advance past header
        diff.next()
        diff.next()
        diff = reverse(diff)
        
        (old2, conflicts) = patch(new, diff)
        if old != old2:
            raise AssertionError

    def test11(self):
        """
        test that a patch that appends to a file which has shrunk
        works
        """
        # orig file is 10 lines of "1"
        old = [ '1' ] * 10
        # local modification removes two of the lines of "1"
        oldchanged = [ '1' ] * 8
        # new file adds two lines
        new = old + [ '2', '3' ]
        # we expect for the result to be the local change plus two lines
        newmerged = oldchanged + [ '2', '3' ]

        diff = difflib.unified_diff(old, new, lineterm = "")
        # advance past header
        diff.next()
        diff.next()

        (results, conflicts) = patch(oldchanged, diff)
        assert(results == newmerged)
        assert(not conflicts)

    def testConverge(self):
        """
        tests applying a patch to a file when applying a patch to a
        file that already has the change.  This is used in cvc merge
        """
        # orig file is 10 lines of "1"
        base = [ '1' ] * 10
        # the file changes the middle line to "2" on the branch
        tip = base[:]
        tip[5] = '2'
        # the file on my local brach already has the change
        trunk = tip[:]

        diff = difflib.unified_diff(base, tip, lineterm = "")
        # advance past header
        diff.next()
        diff.next()

        # this should be like "patch appears to already be applied"
        (results, conflicts) = patch(trunk, diff)
        assert(results == tip)
        assert(not conflicts)

    def testEraseConflict(self):
        base = """\
            useradd
                ${UIDARG}
                ${HOMEDIR:+-d "${HOMEDIR}"}
                $USER >/dev/null 2>&1 || :
            ;;
            1
            2
            3
""".split('\n')
        tip = """\
            useradd
                ${UIDARG}
                -M -d "${HOMEDIR}"
                $USER >/dev/null 2>&1
            ;;
            1
            2
            3
""".split('\n')
        local = """\
            useradd
                ${UIDARG} \
                ${HOMEDIR:+-d "${HOMEDIR}"}
                ${PASSWORD:+-s "${PASSWORD}"}
                $USER >/dev/null 2>&1 || :
            ;;
            1
            2
            3
""".split('\n')

        diff = list(difflib.unified_diff(base, tip))
        (results, conflicts) = patch(local, diff[2:])
        assert(results == local)
        assert(conflicts)

    def testEraseAlreadyApplied(self):
        first = [ "%s\n" % x for x in xrange(10) ]
        second = first[:]
        second.remove("4\n")

        diff = list(difflib.unified_diff(first, second))
        (results, conflicts) = patch(second, diff[2:])
        assert(results == second)
        assert(not conflicts)

    def testMergeAtEnd(self):
        first = [ "%s\n" % x for x in xrange(10) ]
        second = first[:]
        second.remove("7\n")
        diff = list(difflib.unified_diff(first, second))

        third = first[:]
        third.remove("9\n")
        result = patch(third, diff[2:])
        assert(not result[1])
        assert(result[0] == [ '0\n', '1\n', '2\n', '3\n', '4\n', '5\n',
                              '6\n', '8\n'])

    def testNoTrailingNewline(self):
        first = [ "line\n" ]
        second = [ "line\n", "another" ]
        diff = list(unifiedDiff(first, second))
        assert(diff[-1] == '\ No newline at end of file\n')
        assert(diff[-2][-1] == '\n')

        result = patch(first, diff[2:])
        assert(not result[1])
        assert(result[0] == second)

        first = [ "first" ]
        second = [ "second" ]
        diff = list(unifiedDiff(first, second))
        result = patch(first, diff[2:])
        assert(not result[1])
        assert(result[0] == second)

        first = [ "first" ]
        second = [ "second\n" ]
        diff = list(unifiedDiff(first, second))
        result = patch(first, diff[2:])
        assert(not result[1])
        assert(result[0] == second)
