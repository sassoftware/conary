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
