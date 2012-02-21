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


import cPickle
import os
import struct

from conary_test import rephelp

from conary.repository import calllog

class CallLogTest(rephelp.RepositoryHelper):
    def testCallLoggerEmptyFile(self):
        # CNY-2252

        logPath = os.path.join(self.workDir, "logfile")
        # Create empty file
        file(logPath, "w+")

        log = calllog.ClientCallLogger(logPath, readOnly = True)
        self.assertEqual(len([ x for x in log ]), 0)

    def testCallLoggerSimple(self):
        logPath = os.path.join(self.workDir, "logfile")
        # Create empty file
        stream = file(logPath, "w+")

        infos = [
            (1000001, 'http://localhost', [('a', 'a'), ('b', 'b')],
                'someMethod', [1, 2], 3, .23),
        ]

        for info in infos:
            entData = cPickle.dumps(info)
            stream.write(struct.pack("!I", len(entData)))
            stream.write(entData)
        stream.close()

        log = calllog.ClientCallLogger(logPath, readOnly = True)
        ents = [ x for x in log ]
        self.assertEqual(len(ents), len(infos))

        for entry, info in zip(ents, infos):
            self.assertEqual(entry.revision, info[0])
            self.assertEqual(entry.url, info[1])
            self.assertEqual(entry.entitlement, info[2])
            self.assertEqual(entry.methodName, info[3])
            self.assertEqual(entry.args, info[4])
            self.assertEqual(entry.result, info[5])
            self.assertEqual(entry.latency, info[6])


        # And add an entry the "normal" way
        log = calllog.ClientCallLogger(logPath, readOnly = False)

        url = 'http://localhost/ABCD'
        entitlement = [('a', 'a'), ('b', 'b')]
        methodName = 'someMethod'
        args = [1, 2, 3]
        result = 6
        latency = .23

        log.log(url, entitlement, methodName, args, result, latency)

        log = calllog.ClientCallLogger(logPath, readOnly = True)
        ents = [ x for x in log ]
        self.assertEqual(len(ents), len(infos) + 1)

        entry = ents[-1]
        self.assertEqual(entry.revision, 1000001)
        self.assertEqual(entry.url, url)
        self.assertEqual(entry.entitlement, entitlement)
        self.assertEqual(entry.methodName, methodName)
        self.assertEqual(entry.args, args)
        self.assertEqual(entry.result, result)
        self.assertEqual(entry.latency, latency)
