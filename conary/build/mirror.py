#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Module implementing the "Mirror" class
"""

import os.path

from conary.build.errors import MirrorError
from conary.lib import util

class Mirror(list):

    def __init__(self, cfg, name):

        for mirrorDir in cfg.mirrorDirs:
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
