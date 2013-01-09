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


'''
Support for reading ar archives (classid or GNU extended)
'''

import os
import time

from conary.lib import util

class ArchiveError(Exception): pass

class ArFile(object):
    '''
    Objects representing files in an ar archive
    @ivar data: file-like object supporting read()
    @ivar name: name (string) as stored in archive
    @ivar mtime: mtime (int) as stored in archive
    @ivar uid: uid (int) as stored in archive
    @ivar gid: gid (int) as stored in archive
    @ivar mode: mode (int) as stored in archive
    @ivar size: size of file (int)
    '''
    __slots__ = ['name', 'mtime', 'uid', 'gid', 'mode', 'size', 'data']
    def __init__(self, **kw):
        for x, y in kw.items():
            setattr(self, x, y)
    def __repr__(self):
        return '<ArFile 0%0o %d:%d %10d %s %s>' %(self.mode%0777, self.uid,
            self.gid, self.size, time.ctime(self.mtime), self.name)

def Archive(fileObj):
    '''
    Iterator which takes a single argument of a file-like object containing
    a classic or GNU-extended ar archive, and yields objects representing
    entries in that archive.
    '''
    fileObj.seek(0, 2)
    length = fileObj.tell()
    fileObj.seek(0)
    magic = fileObj.read(8)
    if len(magic) < 8 or magic != '!<arch>\n':
        raise ArchiveError('Unrecognized format')
    pFileObj = util.ExtendedFdopen(os.dup(fileObj.fileno()))
    longNameData = None

    while fileObj.tell() <= (length-1):
        hdr = fileObj.read(60)
        if len(hdr) < 60 or hdr[58:60] != '`\n':
            raise ArchiveError('Unrecognized header')
        size = int(hdr[48:58])
        name = hdr[0:16].strip()
        start = fileObj.tell()
        data = util.SeekableNestedFile(pFileObj, size, start)
        fileObj.seek(size + size%2, 1) # padded to even size

        if name == '//' and longNameData is None:
            # GNU extended name data block
            longNameData = data.read()
            continue

        if name.startswith('/') and len(name) > 1 and longNameData is not None:
            # reference into data block
            nameIndex = int(name[1:])
            nameEndIndex = longNameData[nameIndex:].index('\n')
            name = longNameData[nameIndex:nameIndex+nameEndIndex].rstrip('/')

        if '/' in name and len(name) > 1:
            # GNU adds trailing / to names; '/' is the name of ranlib table
            name = name.rstrip('/')

        yield ArFile(
            name = name,
            mtime = int(hdr[16:28]),
            uid = int(hdr[28:34]),
            gid = int(hdr[34:40]),
            mode = int(hdr[40:48], 8),
            size = size,
            data = data)
