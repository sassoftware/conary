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


"""
Contains functions to assist in dealing with deb files.
"""

import os
import gzip
import rfc822
import tarfile

from conary.lib import ar

#{ Constants
NAME = 1
VERSION = 2
RELEASE = 3
ARCH = 4
LICENSE = 5
SUMMARY = 6
DESCRIPTION = 7
#}

class ControlFileParser(object):
    "Parse a Debian control file"
    __slots__ = [ '_prevVal', ]

    def __init__(self):
        self._prevVal = None

    def parse(self, fileObj):
        """
        Yields tuples (key, valueLines)
        """
        m = rfc822.Message(fileObj)
        for k, v in m.items():
            arr = v.split('\n')
            rarr = [ arr[0] ]
            for l in arr[1:]:
                l = l[1:]
                if l == '.':
                    l = ''
                rarr.append(l)
            yield (k, rarr)


class Error(Exception):
    """Generic error"""

class DebianPackageHeader(object):
    def __init__(self, fileObj):
        self._fileObj = fileObj
        self._data = dict()

        ctrl = self._getControlFileStream()

        cfp = ControlFileParser()
        for k, v in cfp.parse(ctrl):
            if k in self._headerMap:
                # These are single-line values
                self._data[self._headerMap[k]] = v[0]
                continue
            if k == 'version':
                arr = v[0].split('-', 1)
                if len(arr) == 1:
                    arr.append('0')
                self._data[VERSION] = arr[0]
                self._data[RELEASE] = arr[1]
                continue
            if k == 'description':
                self._data[SUMMARY] = v[0]
                self._data[DESCRIPTION] = '\n'.join(v[1:])
                continue

    def __getitem__(self, key):
        return self._data.__getitem__(key)

    def __setitem__(self, key, value):
        return self._data.__setitem__(key, value)

    def _getControlFileStream(self):
        self._fileObj.seek(0)
        arch = ar.Archive(self._fileObj)
        arr = [ x for x in arch if x.name == 'control.tar.gz' ]
        if not arr:
            raise Error("Unable to find control archive")
        arFile = arr[0]

        try:
            gf = gzip.GzipFile(fileobj=arFile.data)
            tf = tarfile.TarFile(fileobj=gf)
        except IOError, e:
            raise Error("control.tar.gz is not readable: %s" %str(e))
        # Look for a 'control' file
        arr = [ x for x in tf if os.path.basename(x.name) == 'control' ]
        if not arr:
            raise Error("Control file not found")
        cf = tf.extractfile(arr[0])
        return cf

    _headerMap = dict(package = NAME, architecture = ARCH, license = LICENSE)
