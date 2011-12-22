#!/usr/bin/python
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


# CNY-2261

import os
import sys
import weakref


class Pipe(object):
    BUFFER_SIZE = 16384

    def __init__(self, pread, pwrite):
        self.pread = os.fdopen(pread, "r")
        self.pwrite = os.fdopen(pwrite, "w")

    def read(self, amt=None):
        return self.pread.read(amt)

    def readline(self):
        return self.pread.readline()

    def write(self, data):
        return self.pwrite.write(data)

    def fileno(self):
        return self.pread.fileno()

    def close(self):
        self.pread.close()
        self.pwrite.close()

    def flush(self):
        self.pwrite.flush()

class _Method(object):
    def __init__(self, send, name):
        self._send = send
        self._name = name

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        return _Method(self._send, "%s.%s" % (self._name, name))

    def __call__(self, *args, **kwargs):
        return self._send(self._name, args, kwargs)

def dumpData(stream, data):
    import struct
    import cPickle
    data = cPickle.dumps(data)
    stream.write(struct.pack("!H", len(data)))
    stream.write(data)
    stream.flush()

def loadData(stream):
    import struct
    import cPickle
    #print "%s: reading" % os.getpid()
    dlen = stream.read(2)
    if len(dlen) != 2:
        raise Exception("Expected %s bytes, got %s" % (2, len(dlen)))
    dlen = struct.unpack("!H", dlen)[0]
    #print "%s: reading %s bytes" % (os.getpid(), dlen)
    data = stream.read(dlen)
    data = cPickle.loads(data)
    #print "%s: methodname=%s params=%s" % (os.getpid(), methodname, params)
    return data

class BaseProxy(object):
    __slots__ = ['_methods']

    def __init__(self, methods):
        self._methods = methods

    def _request(self, methodname, args, kwargs):
        self._dumpRequest(methodname, args, kwargs)
        sdict = self._loadResponse()
        return self._decodeResponse(sdict)

    def _dumpRequest(self, methodname, args, kwargs):
        pass

    def _decodeResponse(self, sdict):
        ret, handled = self._decodeResponseSimple(sdict)
        if handled:
            return ret
        raise Exception("Unknown response %s" % sdict)

    def _decodeResponseSimple(self, sdict):
        if 'exception' in sdict:
            exc = sdict['exception']
            tb = sdict.get('tb', None)
            if tb is None:
                raise exc
            raise exc, tb[0], tb[1]
        if 'result' in sdict:
            return sdict['result'], True
        return None, False

    def __getattr__(self, name):
        if name.startswith('_'):
            raise AttributeError(name)
        meth = _Method(self._request, name)
        if name not in self._methods:
            return meth()
        return meth

    def __setattr__(self, name, value):
        if name.startswith('_'):
            return object.__setattr__(self, name, value)
        return self._request('__setattr__', (name, value), {})

class ServerProxy(BaseProxy):
    __slots__ = ['_idx', '_sc']
    def __init__(self, sc, idx, methods):
        BaseProxy.__init__(self, methods)
        self._idx = idx
        self._sc = weakref.ref(sc)

    def _dumpRequest(self, methodname, args, kwargs):
        #print "%s: _request" % os.getpid()
        dumpData(self._sc()._pipe, dict(server=self._idx, message=methodname,
                 args=args, kwargs=kwargs))

    def _loadResponse(self):
        return loadData(self._sc()._pipe)


class ServerCacheProxy(BaseProxy):
    def __init__(self, cwd=None, env=None):
        BaseProxy.__init__(self, set())
        if env is None:
            env = os.environ.copy()
        self._servers = [ None ] * 5
        p0, p1 = os.pipe(), os.pipe()

        self._pid = os.fork()
        if self._pid == 0:
            os.close(p1[1])
            os.close(p0[0])
            if not cwd:
                if 'conary_test.serverCacheProxy' in sys.modules:
                    fpath = sys.modules['conary_test.serverCacheProxy'].__file__
                else:
                    fpath = sys.argv[0]
                cwd = os.path.dirname(fpath)
            cmd = ["/usr/bin/python", "-c",
                "from conary_test import serverCacheProxy; "
                "serverCacheProxy.Child(%d, %d)" %
                    (p1[0], p0[1])]
            os.chdir(cwd)
            os.execve(cmd[0], cmd, env)
            os._exit(0)

        try:
            os.close(p0[1])
            os.close(p1[0])
            self._pipe = Pipe(p0[0], p1[1])
            sdict = loadData(self._pipe)
            self._methods = sdict['methods']
            #print "Child started", methodname, params
        except:
            os.waitpid(self._pid, 0)
            raise

    def _dumpRequest(self, methodname, args, kwargs):
        #print "%s: _request" % os.getpid()
        dumpData(self._pipe, dict(message=methodname, args=args, kwargs=kwargs))

    def _loadResponse(self):
        return loadData(self._pipe)

    def _decodeResponseSimple(self, sdict):
        ret, handled = BaseProxy._decodeResponseSimple(self, sdict)
        if handled:
            return ret, handled
        if 'server' in sdict:
            serverIdx, methods = sdict['server']
            self._servers[serverIdx] = ServerProxy(self, serverIdx, methods)
            return self._servers[serverIdx], True
        return None, False

    def _stopClient(self):
        dumpData(self._pipe, dict(action='stop'))

    def _close(self):
        if self._pid == 0:
            return
        try:
            self._stopClient()
            for i in range(10):
                ret = os.waitpid(self._pid, os.WNOHANG)
                if os.WIFEXITED(ret[1]):
                    self._pid = 0
                    break
                os.sleep(.1)
            if self._pid:
                try:
                    os.kill(self._pid, 15)
                    os.sleep(.1)
                    os.kill(self._pid, 9)
                    os.waitpid(self._pid, os.WNOHANG)
                except OSError, e:
                    if e.errno != 10:
                        raise
        finally:
            self._pid = 0

    def __getattr__(self, name):
        if name == 'servers':
            return self._servers[:]
        return BaseProxy.__getattr__(self, name)

    def __del__(self):
        if self._pid != 0:
            print 'warning: %r was not stopped before freeing' % self
            try:
                self._close()
            except:
                print 'warning: failed to stop %r in __del__' % self

    def stopServer(self, serverIdx):
        self._request('stopServer', (serverIdx, ), {})
        self._servers[serverIdx] = None

class Child(object):
    def __init__(self, pread, pwrite):
        p = Pipe(pread, pwrite)
        #import testsetup
        #testsetup.main()
        from conary_test import rephelp
        self._sc = rephelp.ServerCache()
        methods = [ x for x in dir(self._sc)
                    if not x.startswith('_')
                       and hasattr(getattr(self._sc, x), '__call__') ]

        dumpData(p, dict(message='started', methods=set(methods)))
        while 1:
            sdict = loadData(p)
            action = sdict.get('action', None)
            if action == 'stop':
                break
            message = sdict.get('message', None)
            if message is None:
                dumpData(p, dict(exception=Exception("message missing")))
                continue
            params = sdict.get('args', ())
            kwparams = sdict.get('kwargs', {})
            try:
                if 'server' in sdict:
                    remoteObj = self._sc.servers[sdict['server']]
                else:
                    remoteObj = self._sc
                if message == '__setattr__':
                    # New-style objects have a __setattr__, while old ones
                    # don't
                    name, value = params
                    if hasattr(remoteObj, message):
                        ret = remoteObj.__setattr__(name, value)
                    else:
                        ret = remoteObj.__dict__[name] = value
                else:
                    obj = getattr(remoteObj, message)
                    if hasattr(obj, '__call__'):
                        ret = obj(*params, **kwparams)
                    else:
                        ret = obj
            except Exception, e:
                dumpData(p, dict(exception=e))
            else:
                if isinstance(ret, rephelp.RepositoryServer):
                    methods = [ x for x in dir(ret)
                        if not x.startswith('_')
                           and hasattr(getattr(ret, x), '__call__') ]
                    dumpData(p, dict(server=(ret.serverIdx, set(methods))))
                else:
                    dumpData(p, dict(result=ret))

if __name__ == '__main__':
    print "Parent:", os.getpid()
    xxx = ServerCacheProxy()
    print xxx.stopServer(1)
    print xxx.startServer('/tmp/reposdir', '/home/misa/hg/conary--1.2',
                          serverIdx=1)
    foo = xxx.getCachedServer(1)
    print foo.getMap()
    print xxx.servers[1].getMap()
    print foo.serverDir
    xxx._close()
