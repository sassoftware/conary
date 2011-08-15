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
Module implementing the "Mirror" class
"""

import os.path

from conary.build.errors import MirrorError
from conary.lib import util

class Mirror(list):

    def __init__(self, mirrorDirs, name, multiurlMap = None):
        if multiurlMap and name in multiurlMap:
            self.extend(multiurlMap[name])
            return

        for mirrorDir in mirrorDirs:
            self._readFile(os.path.join(mirrorDir, name))

        if len(self) == 0:
            raise MirrorError("Can't find mirror servers for '%s'" % name)

    def _readFile(self, path):

        if not util.exists(path):
            return

        for line in file(path, 'rU').readlines():
            line = line.strip().rstrip('/')
            if not line or line.startswith('#'):
                continue
            self.append(line)
