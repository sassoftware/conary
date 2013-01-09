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
import shutil
import tempfile
import unittest

from conary.repository.datastore import DataStore, DataStoreSet
from conary.local.localrep import SqlDataStore
from conary.lib import util
from conary import dbstore

class DataStoreTest(unittest.TestCase):

    def addFile(self, store, contents, hash, precompressed = False):
        (fd, name) = tempfile.mkstemp()
        os.close(fd)
        f = open(name, "w+")
        os.unlink(name)
        f.write(contents)

        f.seek(0)
        store.addFile(f, hash, precompressed = precompressed)
        f.close()

        return name

    def checkFile(self, store, contents, hash):
        f = store.openFile(hash)
        str = f.read()
        f.close()
        if str != contents: raise AssertionError

    def setUp(self):
        self.top = tempfile.mkdtemp()
        
    def testNoDirectory(self):
        # we shouldn't be able to open directories which don't exist
        if os.path.exists(self.top):
            shutil.rmtree(self.top)
        self.assertRaises(IOError, DataStore, self.top)

    def testDataStore(self):
        self._testDataStore(DataStore(self.top))

    def testSqlDataStore(self):
        db = dbstore.connect(':memory:', driver='sqlite')
        db.loadSchema()
        self._testDataStore(SqlDataStore(db))

    def _testDataStore(self, d):
        # create a data store and place three files in it
        list = []

        gzipStream = '\x1f\x8b\x08\x00|\xc1\x83B\x02\x03\xcbH\xcd\xc9\xc9' \
                     '\x07\x00\x86\xa6\x106\x05\x00\x00\x00' 

        self.addFile(d, "test file 1\n", 
                     "5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")
        self.addFile(d, "test file 2\n", 
                     "48091aae70bd2bd56ffcd2e5ed4b1ded56511b69")
        self.addFile(d, "test file 3\n", 
                     "d94f97fec5188ca5ca38981303aa6a364bdf3283")
        self.addFile(d, gzipStream,
                     "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d",
                     precompressed = True)

        self.checkFile(d, "test file 1\n", 
                       "5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")
        self.checkFile(d, "test file 2\n",
                       "48091aae70bd2bd56ffcd2e5ed4b1ded56511b69")
        self.checkFile(d, "test file 3\n",
                       "d94f97fec5188ca5ca38981303aa6a364bdf3283")
        self.checkFile(d, "hello",
                       "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d")

        if isinstance(d, DataStore):
            assert(os.path.exists(self.top + 
                     "/5e/2b/d4918bd3bf0a32be16ea85c74d52bfa27cc3"))
            assert(os.path.exists(self.top + 
                     "/48/09/1aae70bd2bd56ffcd2e5ed4b1ded56511b69"))
            assert(os.path.exists(self.top + 
                     "/d9/4f/97fec5188ca5ca38981303aa6a364bdf3283"))
            assert(os.path.exists(self.top + 
                     "/aa/f4/c61ddcc5e8a2dabede0f3b482cd9aea9434d"))

        assert(d.hasFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3"))
        assert(d.hasFile("48091aae70bd2bd56ffcd2e5ed4b1ded56511b69"))
        assert(d.hasFile("d94f97fec5188ca5ca38981303aa6a364bdf3283"))
        assert(d.hasFile("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"))
        assert(not d.hasFile("d94f97fec5188ca5ca38981303aa6a364bdf3284"))

        # duplicate file
        self.addFile(d, "test file 1\n", 
                     "5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")

        if isinstance(d, DataStore) or isinstance(d, DataStoreSet):
            # not reference counted
            d.removeFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")
            if d.hasFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3"):
                raise AssertionError
            self.assertRaises(OSError, d.removeFile,
                              "5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")
        else:
            # reference counted
            d.removeFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")
            if not d.hasFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3"):
                raise AssertionError
            d.removeFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3")
            if d.hasFile("5e2bd4918bd3bf0a32be16ea85c74d52bfa27cc3"):
                raise AssertionError

    def tearDown(self):
        if os.path.exists(self.top):
            shutil.rmtree(self.top)

    def testSet(self):
        util.mkdirChain(self.top + "/first")
        util.mkdirChain(self.top + "/second")
        first = DataStore(self.top + "/first")
        second = DataStore(self.top + "/second")

        self._testDataStore(DataStoreSet(first, second))
