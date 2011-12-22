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
        self.failUnlessEqual(len([ x for x in log ]), 0)

    def testReposcallLoggerNoSpace(self):
        # CNY-2739
        logPath = os.path.join(self.workDir, "logfile")
        log = reposlog.RepositoryCallLogger(logPath, None)
        def mock_write(*args):
            import errno
            raise OSError(errno.ENOSPC, "No space left on file")
        self.mock(os, "write", lambda *args: mock_write(*args))
        # this should not result in a blowup because of no space left on device...
        rc, s = self.captureOutput(log.log, "0", ("user", "pass", []), "someMethod", [1,2], {"a":"a"}, "testing", 1)
        self.failUnlessEqual(s, "warning: '[Errno 28] No space left on file' while logging call from (0,user) to someMethod\n\n")

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
        ]

        for info in infos:
            entData = cPickle.dumps(info)
            stream.write(struct.pack("!I", len(entData)))
            stream.write(entData)
        stream.close()

        log = reposlog.RepositoryCallLogger(logPath, None, readOnly = True)
        ents = [ x for x in log ]
        self.failUnlessEqual(len(ents), len(infos))

        for entry, info in zip(ents, infos):
            self.failUnlessEqual(entry.revision, info[0])
            self.failUnlessEqual(entry.serverName, info[1])
            self.failUnlessEqual(entry.timeStamp, info[2])
            self.failUnlessEqual(entry.remoteIp, info[3])
            self.failUnlessEqual(entry.methodName, info[5])
            self.failUnlessEqual(entry.args, info[6])
            if entry.revision < 4:
                self.failUnlessEqual(entry.exceptionStr, info[7])
            else:
                self.failUnlessEqual(entry.kwArgs, info[7])
                self.failUnlessEqual(entry.exceptionStr, info[8])
                if entry.revision == 5:
                    self.failUnlessEqual(entry.latency, info[9])

            if entry.revision == 1:
                x = (entry.user, entry.entClass)
            elif entry.revision == 2:
                x = (entry.user, entry.entClass, entry.entKey)
            elif entry.revision in [3, 4, 5]:
                x = (entry.user, entry.entitlements)
            else:
                assert(0)
            self.failUnlessEqual(x, info[4])



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

        log.log(remoteIp, authToken, methodName, args, kwargs, exception,
                latency)

        log = reposlog.RepositoryCallLogger(logPath, None, readOnly = True)
        ents = [ x for x in log ]
        self.failUnlessEqual(len(ents), len(infos) + 1)

        entry = ents[-1]
        self.failUnlessEqual(entry.revision, 5)
        self.failUnlessEqual(entry.serverName, serverNameList)
        self.failUnlessEqual(entry.remoteIp, remoteIp)
        self.failUnlessEqual(entry.methodName, methodName)
        self.failUnlessEqual(entry.args, args)
        self.failUnlessEqual(entry.kwArgs, kwargs)
        self.failUnlessEqual(entry.exceptionStr, exception)
        self.failUnlessEqual(entry.exceptionStr, exception)
        self.failUnlessEqual(entry.latency, latency)
        self.failUnlessEqual(entry.user, authToken[0])
        self.failUnlessEqual(entry.entitlements, authToken[2])
