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

import os.path
import imp
import modulefinder
from modulefinder import READ_MODE

class DirBasedModuleFinder(modulefinder.ModuleFinder):
    def __init__(self, baseDir, *args, **kw):
        self.caller = None
        self.deps = {}
        self.baseDir = baseDir
        modulefinder.ModuleFinder.__init__(self, *args, **kw)

    def scan_code(self, co, m):
        if not m.__file__.startswith(self.baseDir):
            return
        else:
            return modulefinder.ModuleFinder.scan_code(self, co, m)

    def import_hook(self, name, caller=None, fromlist=None):
        oldCaller = self.caller
        if caller:
            self.caller = caller.__file__
        else:
            self.caller = None

        try:
            modulefinder.ModuleFinder.import_hook(self, name, caller, fromlist)
        finally:
            self.caller = oldCaller

    def import_module(self, partname, fqname, parent):
        m = modulefinder.ModuleFinder.import_module(self, partname, fqname,
                                                    parent)
        if self.caller and m and m.__file__:
            self.deps.setdefault(self.caller, set()).add(m.__file__)
        return m

    def load_file(self, pathname):
        dir, name = os.path.split(pathname)
        name, ext = os.path.splitext(name)
        if pathname.endswith('.pyc'):
            fileType = imp.PY_COMPILED
            mode = 'rb'
        else:
            fileType = imp.PY_SOURCE
            mode = READ_MODE
        fp = open(pathname, mode)
        stuff = (ext, mode, fileType)
        self.load_module(name, fp, pathname, stuff)

    def getDepsForPath(self, path):
        return self.deps.get(path, [])

    def getSysPath(self):
        return self.path
