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

import bz2
import gzip
import os
import StringIO
import tempfile
import time

from conary.lib import util, logger


class XmlLogParseTest(testhelp.TestCase):
    def testOpenPath(self):
        tmpDir = tempfile.mkdtemp()
        try:
            uncomp = os.path.join(tmpDir, 'foo.log')
            gzipComp = os.path.join(tmpDir, 'foo.log.gz')
            bz2Comp = os.path.join(tmpDir, 'foo.log.bz2')
            for fn in (uncomp, gzipComp, bz2Comp):
                f = logger.openPath(fn)
                f.write('test')
                f.close()
            data = open(uncomp).read()
            self.assertEquals(data, 'test')

            data = gzip.GzipFile(gzipComp, 'r').read()
            self.assertEquals(data, 'test')

            data = bz2.BZ2File(bz2Comp, 'r').read()
            self.assertEquals(data, 'test')
        finally:
            util.rmtree(tmpDir)

    def doStreamLogger(self):
        writer = logger.StreamLogWriter()
        lgr = logger.Logger(withStdin = False, writers = [writer])
        self.assertEquals(lgr.marker, lgr.lexer.marker)
        lgr.startLog()
        lgr.pushDescriptor('cook')
        lgr.write('shown message')
        lgr.pushDescriptor('environment')
        lgr.write('hidden message')
        lgr.popDescriptor('environment')
        self.assertRaises(RuntimeError, lgr.popDescriptor, 'environment')
        lgr.popDescriptor('cook')
        lgr.close()

    def testStreamLogger(self):
        if not (('built-in' in str(os.fork)) or ('fork_wrapper' in str(os.fork))):
            self.fail("unexpected fork class: %s" % str(os.fork))
        exc, data = self.captureOutput(self.doStreamLogger)
        self.assertEquals(exc, None)
        self.assertEquals(data, 'shown message')


class FakeLogWriter(logger.LogWriter):
    def __init__(self):
        self.record = []

    def freetext(self, text):
        self.record.append(text)

    @logger.callable
    def brokenCommand(self, data):
        # not a keyword argument
        pass

    @logger.callable
    def raiseUnknown(self, data):
        raise IOError, 'asdf'

class WritersTest(testhelp.TestCase):
    def testBadCommands(self):
        # CNY-2904
        writer = FakeLogWriter()
        writer.handleToken((logger.COMMAND, ('brokenCommand',)))
        writer.handleToken((logger.COMMAND, ('raiseUnknown', 'foo')))
        self.assertEquals(writer.record, [
            '\nERROR: failed attempt to call function brokenCommand with arguments ()\n',
            "\nERROR: unhandled exception %s: asdf calling function raiseUnknown with arguments ('foo',)\n" % str(IOError),
            ])

    def testBasicFileLogWriter(self):
        tmpDir = tempfile.mkdtemp()
        try:
            logPath = os.path.join(tmpDir, 'log')
            writer = logger.FileLogWriter(logPath)
            writer.start()
            writer.freetext('message 1')
            writer.newline()
            writer.freetext('message 2')
            writer.carriageReturn()
            writer.newline()
            writer.freetext('message 3')
            writer.carriageReturn()
            writer.reportMissingBuildRequires('foo:runtime bar:lib')
            writer.close()
            data = open(logPath).read()
        finally:
            util.rmtree(tmpDir)
        self.assertEquals(data, 'message 1\nmessage 2\n\nmessage 3\n'
            "warning: Suggested buildRequires additions: ['foo:runtime', 'bar:lib']\n")

    def testBasicStreamLogWriter(self):
        msg1 = "message one"
        msg2 = "msg two"
        msg3 = "three"
        msg4 = "four"
        msg5 = 85 * "a"
        stream = StringIO.StringIO()
        writer = logger.StreamLogWriter(stream)
        writer.freetext(msg1)
        writer.newline()
        writer.freetext(msg2)
        writer.carriageReturn()
        writer.newline()
        writer.freetext(msg3)
        writer.carriageReturn()
        writer.freetext(msg4)
        writer.carriageReturn()
        writer.freetext(msg5)
        writer.carriageReturn()
        data = stream.getvalue()
        spaces2 = (78 - len(msg2)) * ' '
        spaces3 = (78 - len(msg3)) * ' '
        spaces4 = (78 - len(msg4)) * ' '
        # check that we wrap at 80 columns
        spaces5 = 73 * ' '
        self.assertEquals(data, '%s\n%s%s\r\n%s%s\r%s%s\r%s%s\r' % \
                (msg1, msg2, spaces2, msg3, spaces3, msg4, spaces4,
                    msg5, spaces5))

    def testStreamLogWriterCR(self):
        # prove that carriage return doesn't add extra whitespace to blank lines
        stream = StringIO.StringIO()
        writer = logger.StreamLogWriter(stream)
        writer.freetext('a')
        writer.newline()
        writer.carriageReturn()
        data = stream.getvalue()
        self.assertEquals(data, 'a\n\r')

    def testStreamLogWriterCR2(self):
        # prove that carriage return adds extra whitespace to non-blank lines
        stream = StringIO.StringIO()
        writer = logger.StreamLogWriter(stream)
        writer.freetext('a')
        writer.carriageReturn()
        writer.newline()
        data = stream.getvalue()
        self.assertEquals(data, 'a' + (77 * ' ') + '\r\n')

    def testXmlLogWriter(self):
        tmpDir = tempfile.mkdtemp()
        try:
            logPath = os.path.join(tmpDir, 'log')
            writer = logger.XmlLogWriter(logPath)
            writer.start()
            writer.freetext('message 1')
            writer.newline()
            writer.freetext('message 2')
            writer.carriageReturn()
            writer.newline()
            writer.freetext('message 3')
            writer.carriageReturn()
            writer.reportMissingBuildRequires('foo:runtime bar:lib')
            writer.close()
            data = open(logPath).read()
        finally:
            util.rmtree(tmpDir)

        lines = data.splitlines()
        self.assertEquals(len(lines), 9)
        self.assertEquals(lines[-1], '</log>')
        recordsMatch = min(x.startswith('<record>') and x.endswith('</record>')
                for x in lines[2:-1])
        self.assertEquals(recordsMatch, True)
        assert(x for x in lines
               if 'missingBuildRequires</descriptor><level>WARNING</level><message>foo:runtime bar:lib</message>' in x)

    def testXmlWriterLexerInteraction(self):
        marker = 'MARKER'
        tmpDir = tempfile.mkdtemp()
        try:
            logPath = os.path.join(tmpDir, 'log')
            writer = logger.XmlLogWriter(logPath)
            writer.start()
            lexer = logger.Lexer(marker)
            lexer.registerCallback(writer.handleToken)
            lexer.scan('message\n%s addRecordData foo bar baz\nmessage2\n' %
                    marker)
            lexer.close()
            data = open(logPath).read()
        finally:
            util.rmtree(tmpDir)

        lines = data.splitlines()
        messageLine = [x for x in lines if 'message2' in x]
        self.assertFalse('<foo>bar baz</foo>' not in messageLine[0])

    def testSubscriptionLog(self):
        marker = 'IGNOREME'
        tempFd, tmpFile = tempfile.mkstemp()
        realtime = time.time
        def mockTime():
            # CNY-3019
            return realtime() + 11111111111.0
        self.mock(time, 'time', mockTime)
        try:
            writer = logger.SubscriptionLogWriter(tmpFile)
            lgr = logger.Logger(withStdin = False, writers = [writer],
                                syncPath = tmpFile)
            lgr.startLog()
            # make sure that subscription applies only to following lines
            lgr.write('match one ignore\n')
            # make sure that synchronize works as the first item
            lgr.synchronize()
            lgr.subscribe('.*match.*one.*')
            lgr.write('match one find\n')
            lgr.write('match \\\n')
            lgr.write('one continued find\n')
            lgr.synchronize()

            lines = file(tmpFile).readlines()
            self.assertFalse('ignore' in lines[0])
            self.assertTrue('.' in lines[0])
            self.assertTrue('one find' in lines[1])
            self.assertTrue('one continued find' in lines[2])
            self.assertTrue('.' in lines[3])

        finally:
            self.unmock() # do not confuse time logging
            os.remove(tmpFile)


    def testLexerOperation(self):
        marker = '43227894372910'
        tmpDir = tempfile.mkdtemp()
        try:
            lexer = logger.Lexer(marker)
            logPath = os.path.join(tmpDir, 'log')
            writer = logger.FileLogWriter(logPath)
            writer.start()
            lexer.registerCallback(writer.handleToken)
            lexer.scan('test\n43227894372910 foo\ntext 2\n')
            lexer.close()
            data = open(logPath).read()
            # test that close flushed the lexer
            self.assertEquals(data, 'testtext 2\n')
        finally:
            util.rmtree(tmpDir)

class LexerRulesTest(testhelp.TestCase):
    def setUp(self):
        self.marker = '012334457889'

        self.tokens = []
        def recordToken(token):
            self.tokens.append(token)

        lexer = logger.Lexer(self.marker)
        lexer.registerCallback(recordToken)
        self.lexer = lexer

        testhelp.TestCase.setUp(self)

    @testhelp.context('logLexer')
    def testBasicLexerRules(self):
        # test basic freetext. note the newline is not emitted until we know
        # it's not a marker
        self.lexer.scan('freetext\n')
        self.assertEquals(self.tokens, [(logger.FREETEXT, 'freetext')])
        self.assertEquals(self.lexer.state, logger.NEWLINE)

    @testhelp.context('logLexer')
    def testAggregatdNewlines(self):
        # test that newlines are aggregated
        self.lexer.scan('\n')
        self.assertEquals(self.tokens, [])
        self.assertEquals(self.lexer.state, logger.NEWLINE)
        self.lexer.scan('\ntest')
        self.assertEquals(self.tokens, [(logger.NEWLINE, None),
                                        (logger.NEWLINE, None),
                                        (logger.FREETEXT, 'test')])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testEatCommandSequence(self):
        # test a command sequence. note both newlines get eaten
        self.lexer.scan('\n%s pushDescriptor foo\n' % self.marker)
        self.assertEquals(self.tokens, [(logger.COMMAND,
                                        ['pushDescriptor', 'foo'])])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testEatCommandSequence2(self):
        # test a command sequence. note both newlines get eaten
        self.lexer.scan('\n%s addRecordData foo bar\n' % self.marker)
        self.assertEquals(self.tokens, [(logger.COMMAND,
                                        ['addRecordData', 'foo bar'])])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testCompositeStream(self):
        # test all of that together
        self.lexer.scan('freetext\n\n%s junk arg1  arg2\n\nb' % self.marker)
        self.assertEquals(self.tokens, [(logger.FREETEXT, 'freetext'),
                                        (logger.NEWLINE, None),
                                        (logger.COMMAND, ['junk', 'arg1  arg2']),
                                        (logger.NEWLINE, None),
                                        (logger.FREETEXT, 'b')])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testCarriageReturns(self):
        # test carriage returns, they aggregate with newlines,
        # but they don't delimit markers. they defer for aggregation.
        self.lexer.scan('foo\rbar\r\r\nbaz\r')
        self.assertEquals(self.tokens, [(logger.FREETEXT, 'foo'),
                                         (logger.CARRIAGE_RETURN, None),
                                         (logger.FREETEXT, 'bar'),
                                         (logger.CARRIAGE_RETURN, None),
                                         (logger.CARRIAGE_RETURN, None),
                                         (logger.NEWLINE, None),
                                         (logger.FREETEXT, 'baz'),
                                         (logger.CARRIAGE_RETURN, None)])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testImmediateCarriageReturn(self):
        # test deferring of carriage return
        self.lexer.scan('\r')
        self.assertEquals(self.tokens, [(logger.CARRIAGE_RETURN, None)])
        self.lexer.scan('\ra')
        self.assertEquals(self.tokens, [(logger.CARRIAGE_RETURN, None),
                                        (logger.CARRIAGE_RETURN, None),
                                        (logger.FREETEXT, 'a')])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testMarkerComparison(self):
        # test marker comparison
        self.lexer.scan('\n')
        self.assertEquals(self.tokens, [])
        for char in ('0', '1', '2', '3'):
            self.lexer.scan(char)
            self.assertEquals(self.tokens, [])
            self.assertEquals(self.lexer.state, logger.MARKER)
        self.lexer.scan('4')
        self.assertEquals(self.tokens, [(logger.NEWLINE, None),
                                        (logger.FREETEXT, '01234')])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testFragmentedFreetext(self):
        # test fragmented freetext
        self.lexer.scan('foo')
        self.lexer.scan('bar')
        self.assertEquals(self.tokens, [(logger.FREETEXT, 'foo'),
                                        (logger.FREETEXT, 'bar')])

    @testhelp.context('logLexer')
    def testNewlineAbortsMarker(self):
        # test newline aborts marker start
        self.lexer.scan('\n')
        self.assertEquals(self.tokens, [])
        self.assertEquals(self.lexer.state, logger.NEWLINE)
        self.lexer.scan('0123')
        self.assertEquals(self.tokens, [])
        self.assertEquals(self.lexer.state, logger.MARKER)

        self.lexer.scan('\n')
        self.assertEquals(self.tokens, [(logger.NEWLINE, None),
                                        (logger.FREETEXT, '0123')])
        self.assertEquals(self.lexer.state, logger.NEWLINE)

    @testhelp.context('logLexer')
    def testNewlineAbortsMarker2(self):
        # test newline aborts marker start
        self.lexer.scan('\n')
        self.assertEquals(self.tokens, [])
        self.assertEquals(self.lexer.state, logger.NEWLINE)
        self.lexer.scan('0123')
        self.assertEquals(self.tokens, [])
        self.assertEquals(self.lexer.state, logger.MARKER)

        self.lexer.scan('\r')
        self.assertEquals(self.tokens, [(logger.NEWLINE, None),
                                        (logger.FREETEXT, '0123'),
                                        (logger.CARRIAGE_RETURN, None)])
        self.assertEquals(self.lexer.state, logger.FREETEXT)

    @testhelp.context('logLexer')
    def testLexerNewlineFlush(self):
        def recordToken(token):
            self.tokens.append(token)

        lex = logger.Lexer('foo')
        lex.registerCallback(recordToken)
        lex.scan('\n')
        self.assertEquals(self.tokens, [])
        lex.close()
        self.assertEquals(self.tokens, [(logger.NEWLINE, None),
                                        (logger.CLOSE, None)])

class MiscTest(testhelp.TestCase):
    def testEscapeMessage(self):
        testStrings = [
            "abc",
            "abc\ndef",
            "abc\\ndef\\nghi\\n",
            "abc\ndef\nghi\n",
            "abc\\ndef\\nghi\\nabc\ndef\nghi\n",
        ]
        for s in testStrings:
            self.assertEqual(s,
                logger.unescapeMessage(logger.escapeMessage(s)))
