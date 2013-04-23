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
import imp
import modulefinder
from modulefinder import READ_MODE
import struct
import sys

if __name__ != "__main__":
    # We may not be able to find these when being run as a program
    # via the moduleFinderProxy
    from conary.lib import coveragehook
    from conary import errors

    # only in the conary module case do we care about
    # ModuleFinderProtocolError inheriting from
    # conary.errors.InternalConaryError
    class ModuleFinderProtocolError(errors.InternalConaryError):
        pass
    class ModuleFinderProtocolErrorNoData(errors.InternalConaryError):
        pass
    class ModuleFinderInitializationError(errors.InternalConaryError):
        pass
else:
    class ModuleFinderProtocolError(IOError):
        pass
    class ModuleFinderProtocolErrorNoData(IOError):
        pass
    class ModuleFinderInitializationError(IOError):
        pass



class DirBasedModuleFinder(modulefinder.ModuleFinder):
    def __init__(self, baseDir, *args, **kw):
        self.caller = None
        self.deps = {}
        self.missing = {}
        self.baseDir = baseDir
        modulefinder.ModuleFinder.__init__(self, *args, **kw)

    def scan_code(self, co, m):
        # caller is the module doing the importing; remember it for when
        # import_module is called further down the stack.
        oldCaller = self.caller
        self.caller = m.__file__
        try:
            return modulefinder.ModuleFinder.scan_code(self, co, m)
        finally:
            self.caller = oldCaller

    def import_module(self, partname, fqname, parent):
        m = modulefinder.ModuleFinder.import_module(self, partname, fqname,
                                                    parent)
        if self.caller and m and m.__file__:
            self.deps.setdefault(self.caller, set()).add(m.__file__)
        return m

    def load_file(self, pathname):
        ext = os.path.splitext(pathname)[1]
        if pathname.endswith('.pyc'):
            fileType = imp.PY_COMPILED
            mode = 'rb'
        else:
            fileType = imp.PY_SOURCE
            mode = READ_MODE
        fp = open(pathname, mode)
        stuff = (ext, mode, fileType)
        missing = self.missing.setdefault(pathname, set())
        for name in self.guess_name(pathname):
            fp.seek(0)
            self.load_module(name, fp, pathname, stuff)
            missing.update(self.get_missing(name))
            # Need to clear out refs to __main__ since other scripts will reuse
            # that name. Otherwise any missing module referenced by a script
            # will show up as referenced by other scripts.
            self.badmodules = {}

    def guess_name(self, pathname):
        """Try to figure out the fully-qualified module name for this file"""
        dir, name = os.path.split(pathname)
        name, ext = os.path.splitext(name)
        base = os.path.join(dir, name)
        out = []
        if ext in ('.py', '.pyc', '.pyo'):
            for pkgroot in self.path:
                if not pkgroot.endswith('/'):
                    pkgroot += '/'
                if not base.startswith(pkgroot):
                    continue
                subpath = base[len(pkgroot):]
                fqname = subpath.replace('/', '.')
                if fqname.endswith('.__init__'):
                    fqname = fqname[:-9]
                # Prepopulate self.modules with all parent packages
                parts = fqname.split('.')[:-1]
                parent = None
                for n, part in enumerate(parts):
                    parent = self.import_module(
                            partname=part,
                            fqname='.'.join(parts[:n+1]),
                            parent=parent)
                out.append(fqname)
        if out:
            return out
        else:
            # If it's not in the pythonpath then treat it like a script
            return ['__main__']

    def get_missing(self, fqname):
        # adapted from any_missing in modulefinder, but reorganized to filter
        # to just direct references from fqname and to handle some false
        # positives.
        missing = []
        for name, refs in self.badmodules.items():
            if fqname not in refs or name in self.excludes:
                continue
            i = name.rfind(".")
            if i < 0:
                missing.append(name)
                continue
            subname = name[i+1:]
            pkgname = name[:i]
            pkg = self.modules.get(pkgname)
            if pkg is not None:
                if subname in pkg.globalnames:
                    # It's a global in the package: definitely not missing.
                    pass
                elif pkgname in self.badmodules[name]:
                    # The package tried to import this module itself and
                    # failed. It's definitely missing.
                    missing.append(name)
                elif pkg.starimports:
                    # It could be missing, but the package did an "import *"
                    # from a non-Python module, so we simply can't be sure.
                    pass
                else:
                    # It's not a global in the package, the package didn't
                    # do funny star imports, it's very likely to be missing.
                    # The symbol could be inserted into the package from the
                    # outside, but since that's not good style we simply list
                    # it missing.
                    missing.append(name)
            else:
                missing.append(name)
        return set(missing)

    def getDepsForPath(self, path):
        deps = self.deps.get(path, set())
        missing = self.missing.get(path, set())
        # Since a module might have multiple possible fully-qualified names
        # when one sys.path entry is nested inside another, it might be in both
        # self.deps and self.missing. Only return modules that are actually
        # missing and didn't match any path entry.
        return {'paths': deps, 'missing': missing - deps}

    def getSysPath(self):
        return self.path

    def close(self):
        pass


def getData(inFile):
    def readAll(remaining):
        data = ''
        while remaining > 0:
            partial = inFile.read(remaining)
            if not partial:
                raise ModuleFinderProtocolErrorNoData('No data available to read')
            remaining -= len(partial)
            data += partial
        return data
    size = readAll(4)
    if len(size) != 4:
        raise ModuleFinderProtocolError('Wrong length prefix %s' %len(size))
    size = struct.unpack('!I', size)[0]
    data = readAll(size)
    if len(data) != size:
        raise ModuleFinderProtocolError(
            'Insufficient data: got %s expected %s', len(data), size)
    return cPickle.loads(data)


def putData(outFile, data):
    data = cPickle.dumps(data)
    size = len(data)
    size = struct.pack('!I', size)
    outFile.write(size+data)
    outFile.flush()


class moduleFinderProxy:
    def __init__(self, pythonPath, destdir, libdir, sysPath, error):
        # this object is always instantiated in python 2.4 or later context
        import subprocess
        self.error = error
        environment = os.environ.copy()
        ldLibraryPath = os.getenv('LD_LIBRARY_PATH')
        if ldLibraryPath is not None:
            ldLibraryPath = ldLibraryPath.split(':')
        else:
            ldLibraryPath = []
        ldLibraryPath[0:0] = [destdir+libdir, libdir]
        ldLibraryPath = ':'.join(ldLibraryPath)
        environment['LD_LIBRARY_PATH'] = ldLibraryPath
        scriptFile = __file__.replace('.pyc', '.py').replace('.pyo', '.py')
        self.proxyProcess = subprocess.Popen(
            (pythonPath, scriptFile),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=environment,
            bufsize=0, close_fds=True)
        data = {'cmd': 'init', 'destdir': destdir, 'sysPath': sysPath}
        try:
            putData(self.proxyProcess.stdin, data)
        except IOError, e:
            # failure to write to pipe means child did not initialize
            raise ModuleFinderInitializationError(e)
        try:
            ack = getData(self.proxyProcess.stdout)
            if ack['result'] != 'ready':
                raise ModuleFinderProtocolError('Wrong initial response from'
                    ' dependency discovery process')
        except ModuleFinderProtocolErrorNoData, e:
            # no data available now means child did not initialize
            raise ModuleFinderInitializationError(e)
        self.poll()

    def poll(self):
        if self.proxyProcess.poll() is not None:
            raise ModuleFinderProtocolError(
                'Python dependency discovery process died unexpectedly'
                ' with exit code %n' %self.proxyProcess.returncode)

    def close(self):
        putData(self.proxyProcess.stdin, {'cmd': 'exit'})
        if self.proxyProcess.wait() != 0:
            self.error('Python dependency process failed: %d',
                       self.proxyProcess.returncode)

    def load_file(self, path):
        putData(self.proxyProcess.stdin, {'cmd': 'file', 'path': path})
        self.poll()

    def getDepsForPath(self, path):
        return getData(self.proxyProcess.stdout)


def main():
    # Proxy process that does the actual scanning using the target python
    destDir = sysPath = finder = None

    while True:
        data = getData(sys.stdin)
        type = data['cmd']
        if type == 'init':
            destdir = data['destdir']
            # set sys.path in order to find modules outside the bootstrap
            sys.path = sysPath = data['sysPath']
            finder = DirBasedModuleFinder(destdir, sysPath)
            sys.stderr.write('pydeps bootstrap proxy initializing: '
                             'sys.path %r\n' %sysPath)
            sys.stderr.flush()
            putData(sys.stdout, {'result': 'ready'})
            continue
        if type == 'file':
            path = data['path']
            sys.stderr.write('pydeps inspecting %s (%s)\n' %(path, type))
            sys.stderr.flush()
        elif type == 'file':
            path = data['path']
            sys.stderr.write('pydeps inspecting %s (%s)\n' %(path, type))
            sys.stderr.flush()
        elif type == 'exit':
            sys.stderr.write('dep proxy closing\n')
            sys.stderr.flush()
            os._exit(0)
        else:
            sys.stderr.write('dep proxy terminating:unknown type %s (%s)\n'
                             %(type, path))
            sys.stderr.flush()
            os._exit(2)

        if not path:
            os._exit(3)

        try:
            finder.load_file(path)
        except:
            putData(sys.stdout, {'result': 'invalid'})
            continue

        data = finder.getDepsForPath(path)
        data['result'] = 'ok'
        putData(sys.stdout, data)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ModuleFinderProtocolError, e:
        os._exit(1)
    except:
        os._exit(4)
