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


import struct
import os

from conary_test import rephelp
import cPickle

from conary.repository.netrepos import reposlog

class CallLogTest(rephelp.RepositoryHelper):
    def testReposCallLoggerEmptyFile(self):
        # CNY-2252

        logPath = os.path.join(self.workDir, "logfile")
        # Create empty file
        file(logPath, "w+")

        log = reposlog.RepositoryCallLogger(logPath, None, readOnly = True)
        self.assertEqual(len([ x for x in log ]), 0)

    def testReposcallLoggerNoSpace(self):
        # CNY-2739
        logPath = os.path.join(self.workDir, "logfile")
        log = reposlog.RepositoryCallLogger(logPath, None)
        class MockFile(object):
            def write(*args):
                import errno
                raise IOError(errno.ENOSPC, "No space left on file")
        log.fobj = MockFile()
        # this should not result in a blowup because of no space left on device...
        rc, s = self.captureOutput(log.log, "0", ("user", "pass", []), "someMethod", [1,2], {"a":"a"}, "testing", 1)
        self.assertEqual(s, "warning: '[Errno 28] No space left on file' while logging call from (0,user) to someMethod\n\n")

    def testCallLoggerSimple(self):
        logPath = os.path.join(self.workDir, "logfile")
        # Create empty file
        stream = file(logPath, "w+")

        infos = [
            ( 1, 'serverName', 123412341, '172.31.254.254',
                ('user', 'entClass'), 'someMethod', [1, 2], 'exception'),
            ( 2, 'serverName', 123412341, '172.31.254.254',
                ('user', 'entClass', 'entKey'), 'someMethod', [1, 2], 'exception'),
            ( 3, 'serverName', 123412341, '172.31.254.254',
                ('user', [('a', 'a'), ('b', 'b')]), 'someMethod', [1, 2], 'exception'),
            ( 4, 'serverName', 123412341, '172.31.254.254',
                ('user', [('a', 'a'), ('b', 'b')]), 'someMethod', [1, 2],
                {'a' : 'a', 'b' : 'b'}, 'exception'),
            ( 5, 'serverName', 123412341, '172.31.254.254',
                ('user', [('a', 'a'), ('b', 'b')]), 'someMethod', [1, 2],
                {'a' : 'a', 'b' : 'b'}, 'exception', 1000),
            ( 6, 'serverName', 123412341, '172.31.254.254',
                ('user', [('a', 'a'), ('b', 'b')]), 'someMethod', [1, 2],
                {'a' : 'a', 'b' : 'b'}, 'exception', 1000, 'system_1234'),
        ]

        for info in infos:
            entData = cPickle.dumps(info)
            stream.write(struct.pack("!I", len(entData)))
            stream.write(entData)
        stream.close()

        log = reposlog.RepositoryCallLogger(logPath, None, readOnly = True)
        ents = [ x for x in log ]
        self.assertEqual(len(ents), len(infos))

        for entry, info in zip(ents, infos):
            self.assertEqual(entry.revision, info[0])
            self.assertEqual(entry.serverName, info[1])
            self.assertEqual(entry.timeStamp, info[2])
            self.assertEqual(entry.remoteIp, info[3])
            self.assertEqual(entry.methodName, info[5])
            self.assertEqual(entry.args, info[6])
            if entry.revision < 4:
                self.assertEqual(entry.exceptionStr, info[7])
            else:
                self.assertEqual(entry.kwArgs, info[7])
                self.assertEqual(entry.exceptionStr, info[8])
                if entry.revision >= 5:
                    self.assertEqual(entry.latency, info[9])
                if entry.revision >= 6:
                    self.assertEqual(entry.systemId, info[10])

            if entry.revision == 1:
                x = (entry.user, entry.entClass)
            elif entry.revision == 2:
                x = (entry.user, entry.entClass, entry.entKey)
            elif entry.revision in (3, 4, 5, 6):
                x = (entry.user, entry.entitlements)
            else:
                assert(0)
            self.assertEqual(x, info[4])



        # And add an entry the "normal" way
        serverNameList =  ['server1', 'server2']
        log = reposlog.RepositoryCallLogger(logPath, serverNameList, readOnly = False)

        remoteIp = '10.10.10.10'
        authToken = ['someUser', 'ignored', [('a', 'a'), ('b', 'b')]]
        methodName = 'someMethod'
        args = [1, 2, 3]
        kwargs = {1 : 1, 2 : 2}
        exception = 'exception text'
        latency = .23
        systemId = 'system_foo'

        log.log(remoteIp, authToken, methodName, args, kwargs, exception,
                latency, systemId)

        log = reposlog.RepositoryCallLogger(logPath, None, readOnly = True)
        ents = [ x for x in log ]
        self.assertEqual(len(ents), len(infos) + 1)

        entry = ents[-1]
        self.assertEqual(entry.revision, 6)
        self.assertEqual(entry.serverName, serverNameList)
        self.assertEqual(entry.remoteIp, remoteIp)
        self.assertEqual(entry.methodName, methodName)
        self.assertEqual(entry.args, args)
        self.assertEqual(entry.kwArgs, kwargs)
        self.assertEqual(entry.exceptionStr, exception)
        self.assertEqual(entry.exceptionStr, exception)
        self.assertEqual(entry.latency, latency)
        self.assertEqual(entry.user, authToken[0])
        self.assertEqual(entry.entitlements, authToken[2])
        self.assertEqual(entry.systemId, systemId)
