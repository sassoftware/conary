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

import bz2
import gzip
import logging
import os
import tempfile
import threading
import time

from conary.lib import xmllog, util, log, logger

class XmlLogParseTest(testhelp.TestCase):
    def setUp(self):
        fd, self.logfile = tempfile.mkstemp()
        os.close(fd)
        self.hdlr = xmllog.XmlHandler(self.logfile)
        self.logger = logging.getLogger('xmllogtest')
        self.logger.setLevel(1)
        self.logger.addHandler(self.hdlr)
        testhelp.TestCase.setUp(self)

    def tearDown(self):
        self.hdlr.close()
        self.logger.handlers.remove(self.hdlr)
        util.rmtree(self.logfile)
        testhelp.TestCase.tearDown(self)

    def getLogData(self):
        self.hdlr.flush()
        return open(self.logfile).read()

    def assertSubstring(self, substr, data):
        self.assertFalse(substr not in data,
                "Expected '%s' to be in '%s'" % (substr, data))

    def testBasicAttributes(self):
        self.logger.info('test')
        data = self.getLogData()
        self.assertFalse('descriptor' in data,
                "undefined descriptor should be omitted")
        for messageId in (0, 1):
            self.assertSubstring('<messageId>%d</messageId>' % messageId, data)
        record = data.splitlines()[-1]
        self.assertFalse(not record.startswith('<record>'))
        self.assertFalse(not record.endswith('</record>'))
        for kw in ('level', 'message', 'messageId', 'pid', 'time'):
            self.assertSubstring("<%s>" % kw, record)
            self.assertSubstring("</%s>" % kw, record)

    def testDescriptor(self):
        self.hdlr.pushDescriptor('test')
        self.hdlr.pushDescriptor('case')
        self.logger.info('test')
        data = self.getLogData().splitlines()
        self.assertSubstring('<descriptor>test.case</descriptor>', data[-1])
        self.hdlr.popDescriptor()
        self.hdlr.pushDescriptor('foo')
        self.logger.info('test')
        data = self.getLogData().splitlines()
        self.assertSubstring('<descriptor>test.foo</descriptor>', data[-1])

    def testRecordData(self):
        self.hdlr.addRecordData('jobId', 4125)
        self.logger.info('test')
        data = self.getLogData().splitlines()
        self.assertSubstring('<jobId>4125</jobId>', data[-1])

        self.assertRaises(RuntimeError, self.hdlr.addRecordData, '<bad>', 'key')
        self.hdlr.addRecordData('good', '<data>')
        self.logger.info('test')
        data = self.getLogData().splitlines()
        self.assertSubstring('<good>&lt;data&gt;</good>', data[-1])

        self.hdlr.delRecordData('jobId')
        self.logger.info('test')
        data = self.getLogData().splitlines()
        self.assertFalse('jobId' in data[-1], "expected jobId to be removed")

    def testTimeFormat(self):
        # check ISO8601 format
        self.mock(time, 'time', lambda: 946728000.536)
        ts = logger.getTime()
        # test timestamp for ISO8601 based on UTC
        # this is the preferred dateTime format in XML
        self.assertEquals(ts, '2000-01-01T12:00:00.536Z')
        # unmock now, or else the test suite timings can get wonky
        # (the end time is recorded before the unmock happens)
        self.unmock()

    def testThreadName(self):
        class Foo(threading.Thread):
            def run(x):
                self.logger.info('test\n')
        foo = Foo()
        foo.start()
        foo.join()
        data = self.getLogData().splitlines()
        self.assertSubstring('<threadName>', data[-1])
        self.assertSubstring('</threadName>', data[-1])

    def testThreadData(self):
        class Foo(threading.Thread):
            def run(x):
                self.hdlr.addRecordData('foo', 'bar')
                self.logger.info('test')
        foo = Foo()
        foo.start()
        foo.join()
        data = self.getLogData().splitlines()
        self.assertSubstring('<foo>bar</foo>', data[-1])

    def testThreadDescriptor(self):
        class Foo(threading.Thread):
            def run(x):
                self.hdlr.pushDescriptor('foo')
                self.hdlr.pushDescriptor('bar')
                self.logger.info('test')
        foo = Foo()
        foo.start()
        foo.join()
        data = self.getLogData().splitlines()
        self.assertSubstring('<descriptor>foo.bar</descriptor>', data[-1])

    def testMakeRecord(self):
        record = logger.makeRecord({'foo': 'bar', 'test': 'case', 'baz': 1})
        self.assertEquals(record,
                "<record><baz>1</baz><foo>bar</foo><test>case</test></record>")

    def testClose(self):
        self.hdlr.close()
        self.logger.handlers.remove(self.hdlr)
        data = open(self.logfile).read().splitlines()
        self.hdlr = xmllog.XmlHandler(self.logfile)
        self.logger.addHandler(self.hdlr)
        self.assertEquals(len(data), 5)
        self.assertEquals(data[-1], '</log>')
        self.assertSubstring('<message>end log</message>', data[-2])

    def testFmtdLog(self):
        tmpDir = tempfile.mkdtemp()
        try:
            origHandlers = log.fmtLogger.handlers[:]
            logPath = os.path.join(tmpDir, 'log.xml')
            log.openFormattedLog(logPath)
            log.pushLogDescriptor('foo')
            self.captureOutput(log.debug, 'debug message')
            log.pushLogDescriptor('bar')
            self.captureOutput(log.info, 'info message')
            log.popLogDescriptor()
            self.captureOutput(log.warning, 'warning message')
            log.popLogDescriptor()
            self.captureOutput(log.error, 'error message')
            log.pushLogDescriptor('bad_descriptor')
            hdlr = [x for x in log.fmtLogger.handlers \
                    if x not in origHandlers][0]
            hdlr.close()
            log.fmtLogger.handlers.remove(hdlr)
            data = open(logPath).read().splitlines()

            self.assertFalse('bad_descriptor' in data[-2],
                    "descriptor stack wasn't cleared on log close.")
            self.assertEquals(data[-1], '</log>')
            self.assertEquals(len(data), 9)
            self.assertSubstring('<level>DEBUG</level>', data[2])
            self.assertSubstring('<message>begin log</message>', data[2])
            self.assertSubstring('<level>DEBUG</level>', data[-2])
            self.assertSubstring('<message>end log</message>', data[-2])
        finally:
            util.rmtree(tmpDir)

    def testDescriptorPop(self):
        self.hdlr.pushDescriptor('foo')
        self.hdlr.popDescriptor('foo')

        self.hdlr.pushDescriptor('foo')
        self.hdlr.pushDescriptor('bar')
        self.hdlr.popDescriptor('bar')
        self.assertRaises(AssertionError, self.hdlr.popDescriptor, 'bar')

    def testCompressedLogs(self):
        tmpDir = tempfile.mkdtemp()
        try:
            bz2Path = os.path.join(tmpDir, 'log.xml.bz2')
            bz2Hdlr = xmllog.XmlHandler(bz2Path)
            self.logger.addHandler(bz2Hdlr)

            gzPath = os.path.join(tmpDir, 'log.xml.gz')
            gzHdlr = xmllog.XmlHandler(gzPath)
            self.logger.addHandler(gzHdlr)

            logPath = os.path.join(tmpDir, 'log.xml')
            logHdlr = xmllog.XmlHandler(logPath)
            self.logger.addHandler(logHdlr)

            self.logger.info('test')

            bz2Hdlr.close()
            gzHdlr.close()
            logHdlr.close()

            self.logger.handlers.remove(bz2Hdlr)
            self.logger.handlers.remove(gzHdlr)
            self.logger.handlers.remove(logHdlr)

            # only inspect the first two lines. they won't have timestamps
            logData = open(logPath).read().splitlines()[:2]
            gzData = gzip.GzipFile(gzPath, 'r').read().splitlines()[:2]
            bzData = bz2.BZ2File(bz2Path, 'r').read().splitlines()[:2]
            self.assertFalse(not logData, "expected log content")
            self.assertEquals(logData, gzData)
            self.assertEquals(logData, bzData)
        finally:
            util.rmtree(tmpDir)

    def testCloseData(self):
        self.hdlr.addRecordData('foo', 'bar')
        self.hdlr.close()
        self.logger.handlers.remove(self.hdlr)
        data = open(self.logfile).read().splitlines()
        self.hdlr = xmllog.XmlHandler(self.logfile)
        self.logger.addHandler(self.hdlr)

        self.assertSubstring('<message>end log</message>', data[-2])
        self.assertFalse('<foo>bar</foo>' in data[-2],
                "record data should not apply to close message")

    def testEscapedNewlines(self):
        self.logger.info('multi\nline\nmessage')
        data = self.getLogData()
        lastline = data.splitlines()[-1]
        self.assertFalse(not lastline.startswith('<record>'),
            "multi line log output was not escaped")
