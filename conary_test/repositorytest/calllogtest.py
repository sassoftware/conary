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
