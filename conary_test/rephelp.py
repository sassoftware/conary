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


#python
import BaseHTTPServer
import copy
import errno
import inspect
import itertools
import new
import os
import re
import shutil
import shlex
import signal
import socket
import stat
import StringIO
import subprocess
import sys
import time
import traceback
import testsuite

from M2Crypto import SSL

#conary
from conary.cmds import branch
from conary import callbacks
from conary import checkin
from conary.cmds import clone
from conary import conarycfg
from conary import conaryclient
from conary.cmds import cscmd
from conary import errors
from conary import files
from conary import rpmhelper
from conary import trove
from conary import versions
from conary.build import cook, loadrecipe, use
from conary.cmds import conarycmd
from conary.cmds import cvccmd
from conary.cmds import rollbacks
from conary.cmds import updatecmd
from conary.cmds import verify
from conary.conaryclient import cmdline, filetypes
from conary.deps import arch, deps
from conary.lib import cfg as cfgmod
from conary.lib import cfgtypes, log
from conary.lib import httputils
from conary.lib import openpgpkey
from conary.lib import sha1helper
from conary.lib import util
from conary.local import database
from conary.repository import errors as repo_errors
from conary.repository import netclient, changeset, filecontents, trovesource
from conary.repository import searchsource
from conary.server.server import SecureHTTPServer
#test
from testrunner import testhelp
from testutils import apache_server, base_server, os_utils, sock_utils
from testutils import sqlharness
from conary_test import recipes
from conary_test import resources

# make tryConnect available
tryConnect = sock_utils.tryConnect

class _NoneArg:
    pass

NoneArg = _NoneArg()

_File = filetypes._File

def _isIndividual():
# Make it compatible with testsuites that are not class-based yet
    individual = getattr(testsuite, '_individual', None)
    if individual is None:
        individual = getattr(testsuite, 'Suite').individual
    global _individual
    _individual = individual
    return individual

class Symlink(filetypes.Symlink):
    pass

class RegularFile(filetypes.RegularFile):
    pass

class BlockDevice(filetypes.BlockDevice):
    pass

class CharacterDevice(filetypes.CharacterDevice):
    pass

class Directory(filetypes.Directory):
    pass

_fileClasses = [Symlink, RegularFile, BlockDevice, CharacterDevice, Directory]
if hasattr(filetypes, 'NamedPipe'):
    class NamedPipe(filetypes.NamedPipe):
        pass
    _fileClasses.append(NamedPipe)

if hasattr(filetypes, 'Socket'):
    class Socket(filetypes.Socket):
        pass
    _fileClasses.append(Socket)

for klass in _fileClasses:
    klass.kwargs['version'] = None
    klass.kwargs['config'] = None
    klass.kwargs['pathId'] = None

# Support for specifying trove scripts
class TroveScript:

    def __init__(self, script = None, conversions = None):
        self.script = script
        self.conversions = conversions

class RollbackScript(TroveScript):

    pass

# Let's us load recipes from strings using the class name to find the right
# recipe instead of the class's "name" attribute
class LoaderFromString(loadrecipe.RecipeLoaderFromString):

    @staticmethod
    def _validateName(recipeClass, name):
        return recipeClass.__name__ == name

# this is an override for arch.x86flags -- we never want to use
# any system flags (normally gathered from places like /proc/cpuinfo)
# because they could change the results from run to run
def x86flags(archTag, *args):
    # always pretend we're on i686 for x86 machines
    if archTag == 'x86':
        flags = []
        for f in ('i486', 'i586', 'i686'):
            flags.append((f, deps.FLAG_SENSE_PREFERRED))
        return deps.Dependency(archTag, flags)
    # otherwise, just use the archTag with no flags
    return deps.Dependency(archTag)
# override the existing x86flags() function
arch.x86flags = x86flags
# reinitialize arch
arch.initializeArch()

class IdGen0(cook._IdGen):

    formatStr = "%s"

    def __call__(self, path, version, flavor):
        if self.map.has_key(path):
            return self.map[path]

        fileid = sha1helper.md5String(self.formatStr % path)
        self.map[(path, flavor)] = (fileid, None, None)
        return (fileid, None, None)

class IdGen1(IdGen0):

    formatStr = "1%s"

class IdGen3(IdGen0):

    formatStr = "111%s"


class ContentStore:

    def getPath(self):
        return self.path

    def reset(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)
        util.mkdirChain(self.path)

    def __init__(self, path):
        self.path = path

class RepositoryServer(base_server.BaseServer):
    """
    sets up a repository that will be run as a child process
    """
    def setNeedsReset(self):
        self.needsReset = True

    def reset(self):
        self.contents.reset()
        if self.reposDB:
            self.reposDB.reset()
        if self.cache:
            self.cache.reset()
        self.needsPGPKey = True
        self.needsReset = False
        super(RepositoryServer, self).reset()

    def resetIfNeeded(self):
        if self.needsReset:
            self.reset()

    def getMap(self):
        if self.sslEnabled and self.useSSL:
            dest = 'https://localhost:%d/conary/' % self.port
        else:
            dest = 'http://localhost:%d/conary/' % self.port
        d = dict((name, dest) for name in self.nameList)
        return d

    # assume the first entry in a multihomed name list is the "main" name
    def getName(self):
        return self.nameList[0]

    def __init__(self, nameList, reposDB, contents, server, serverDir, reposDir,
                 conaryPath, repMap, requireSigs, authCheck=None,
                 entCheck=None, authTimeout = None, readOnlyRepository=False,
                 serverIdx=0, proxies=None, useSSL=False, forceSSL = False,
                 sslCert=None, sslKey=None, closed=False, commitAction = None,
                 deadlockRetry=None, excludeCapsuleContents=False,
                 withCache=False):
        base_server.BaseServer.__init__(self)
        assert(isinstance(contents, ContentStore))
        assert(reposDB is None or isinstance(reposDB, sqlharness.RepositoryDatabase))
        self.contents = contents
        self.reposDB = reposDB
        if isinstance(nameList, str):
            nameList = [nameList]
        self.nameList = nameList
        self.needsPGPKey = True
        self.conaryPath = conaryPath
        self.serverDir = serverDir
        self.server = server
        self.requireSigs = requireSigs
        self.reposDir = reposDir
        self.traceLog = os.path.join(self.reposDir, 'trace.log')
        self.tmpDir = reposDir
        self.authCheck = authCheck
        self.entCheck = entCheck
        self.authTimeout = authTimeout
        self.readOnlyRepository = readOnlyRepository
        self.repMap = repMap
        self.serverIdx = serverIdx
        self.proxies = proxies
        self.useSSL = getattr(self, 'sslOnly', useSSL)
        self.forceSSL = forceSSL
        self.excludeCapsuleContents = excludeCapsuleContents
        self.deadlockRetry = deadlockRetry
        if withCache:
            self.cache = ContentStore(reposDir + '/cscache')
            self.cache.reset()
        else:
            self.cache = None
        if self.useSSL and (sslCert is None or sslKey is None):
            if sslCert is None:
                sslCert = resources.get_archive('ssl-cert.crt')
            if sslKey is None:
                sslKey = resources.get_archive('ssl-cert.key')
        self.sslCert = sslCert
        self.sslKey = sslKey
        self.closed = closed
        self.initPort()
        self.commitAction = commitAction
        self.needsReset = True

    def writeServerConfigFile(self, configFile, **kw):
        conaryrc = kw.pop('conaryrc', None)

        configValues = {
            'contentsDir'           : self.contents.getPath(),
            'baseUri'               : '/conary',
            'repositoryDB'          : self.reposDB.getDriver(),
            'serverName'            : " ".join(self.nameList),
            'traceLog'              : '3 ' + self.traceLog,
            'logFile'               : os.path.join(self.reposDir, 'repos.log'),
            'tmpDir'                : self.reposDir,
        }
        if self.requireSigs:
            configValues['requireSigs'] = True

        if self.authCheck:
            configValues['externalPasswordURL'] = self.authCheck
        if self.entCheck:
            configValues['entitlementCheckURL'] = self.entCheck
        if self.authTimeout is not None:
            configValues['authCacheTimeout'] = self.authTimeout
        if self.authTimeout is not None:
            configValues['authCacheTimeout'] = self.authTimeout
        if self.forceSSL:
            configValues['forceSSL'] = True
        if self.excludeCapsuleContents:
            configValues['excludeCapsuleContents'] = True
        if self.deadlockRetry is not None:
            configValues['deadlockRetry'] = self.deadlockRetry
        if self.readOnlyRepository:
            configValues['readOnlyRepository'] = self.readOnlyRepository
        if self.sslEnabled and self.useSSL and self.sslCert:
            self.writeSSLRepoConfig(configValues)
        if self.closed:
            configValues['closed'] = self.closed
        if self.commitAction:
            configValues['commitAction'] = self.commitAction
        if self.cache:
            configValues['changesetCacheDir'] = self.cache.getPath()

        # Add the command line options
        configValues.update(kw)

        f = open(configFile, "w+")
        for item in configValues.iteritems():
            print >> f, "%s %s" % item
        # Multi-valued, cannot be added to a dictionary
        for repname, reppath in self.repMap.iteritems():
            print >> f, 'repositoryMap %s %s' %(repname, reppath)
        f.close()

        if conaryrc:
            f = open(conaryrc, 'w')
            for serverName in self.nameList:
                f.write('repositoryMap %s http://localhost:%s/conary/\n'
                            % (serverName, configValues['port']) )
            f.write('installLabelPath localhost@rpl:linux\n')

        f.close()

    def writeSSLRepoConfig(self, configValues):
        configValues['useSSL'] = True
        configValues['sslCert'] = self.sslCert
        configValues['sslKey'] = self.sslKey

class ApacheServer(RepositoryServer, apache_server.ApacheServer):
    def __init__(self, *args, **kwargs):
        self.useCache = kwargs.pop('useCache', False)
        RepositoryServer.__init__(self, *args, **kwargs)
        apache_server.ApacheServer.__init__(self, topDir=self.reposDir)
        self.logFileObj = None

    def getServerDir(self):
        return resources.get_path('conary_test', 'server')

    def getPythonHandler(self):
        return 'conary.server.apachehooks'

    def _createAppConfig(self):
        configValues = {
            'tmpDir'    : self.tmpDir,
            'traceLog'  : '3 ' + self.traceLog,
            'authCacheTimeout': self.authTimeout or 0,
        }
        if self.closed:
            configValues['closed'] = self.closed
        if self.useCache:
            configValues['changesetCacheDir'] = \
                    os.path.join(self.serverRoot, 'cscache')
            self.cache = ContentStore(configValues['changesetCacheDir'])
            if self.authTimeout is None:
                configValues['authCacheTimeout'] = 2

        self.writeServerConfigFile(os.path.join(self.serverRoot, 'test.cnr'),
                                   **configValues)

    def reset(self):
        if not self.logFileObj:
            try:
                self.logFileObj = open(self.serverRoot + '/error_log')
            except OSError, err:
                if err.errno != errno.ENOENT:
                    raise
        data = self.logFileObj.read()
        if 'Traceback (most recent call last)' in data:
            sys.stderr.write("Contents of error_log after test:\n" + data)
            sys.stderr.flush()

        # this is a bad hack to clear the cache
        csCachePath = os.path.join(self.serverRoot, 'cscache')
        if os.path.exists(csCachePath):
            shutil.rmtree(csCachePath)
        os.mkdir(csCachePath)

        super(ApacheServer, self).reset()

    def _startAppSpecificTasks(self, resetDir = True):
        if resetDir:
            if os.path.exists(self.reposDir):
                shutil.rmtree(self.reposDir)
            os.mkdir(self.reposDir)
            self.createConfig()
        if self.reposDB:
            self.reposDB.reset()

    def _stopAppSpecificTasks(self):
        if self.reposDB:
            self.reposDB._reset()
            self.reposDB.stop()

    def getServerConfigPath(self):
        return os.path.join(self.serverRoot, 'test.cnr')


class ApacheServerWithCache(ApacheServer):
    def __init__(self, *args, **kwargs):
        kwargs['useCache'] = True
        ApacheServer.__init__(self, *args, **kwargs)

class ApacheSSLServer(ApacheServer, apache_server.ApacheSSLMixin):
    sslEnabled = True
    sslOnly = True

    def initPort(self):
        return apache_server.ApacheSSLMixin.initPort(self)

    def createConfig(self):
        ApacheServer.createConfig(self)
        return apache_server.ApacheSSLMixin.createConfig(self)

    def writeSSLRepoConfig(self, configValues):
        # No need to add anything in the repository's config
        pass

class ApacheSSLServerWithCache(ApacheSSLServer, ApacheServerWithCache):
    def __init__(self, *args, **kwargs):
        kwargs['useCache'] = True
        ApacheSSLServer.__init__(self, *args, **kwargs)

class StandaloneServer(RepositoryServer):
    sslEnabled = True
    def __init__(self, *args, **kwargs):
        RepositoryServer.__init__(self, *args, **kwargs)
        self.serverpid = -1
        if 'SERVER_FILE_PATH' in os.environ:
            self.serverFilePath = os.environ['SERVER_FILE_PATH']
            self.delServerPath = False
        else:
            self.serverFilePath =  None
            self.delServerPath = True
        self.serverLog = self.reposDir + '/server.log'
        util.removeIfExists(self.serverLog)

    def start(self, resetDir = True):
        if self.serverpid != -1:
            return
        if resetDir:
            if os.path.exists(self.reposDir):
                shutil.rmtree(self.reposDir)
            os.mkdir(self.reposDir)
            self.contents.reset()

        if self.reposDB:
            if resetDir:
                self.reposDB.reset()
            self.reposDB.stop()

        sb = os.stat(self.server)
        if not stat.S_ISREG(sb.st_mode) or not os.access(self.server, os.X_OK):
            print "bad server path: %s" % self.server
            sys.exit(1)

        if self.serverFilePath is None:
            self.serverFilePath = testhelp.getTempDir('conarytest-server-file-')

        if getattr(self, 'socket', None):
            self.socket.close()
            self.socket = None
        self.serverpid = os.fork()
        if self.serverpid == 0:
            try:
                log = os.open(self.serverLog, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
                os.dup2(log, 1)
                os.dup2(log, 2)
                os.close(log)
                print "starting server"
                sys.stdout.flush()
                serverrc = os.path.join(self.reposDir, 'serverrc')
                conaryrc = os.path.join(self.reposDir, 'conaryrc')
                self.writeServerConfigFile(serverrc, port=self.port,
                                           conaryrc = conaryrc)
                args = (sys.executable, self.server, '--config-file', serverrc)
                #if self.reposDB:
                #    self.reposDB.stop()
                os_utils.osExec(args)
            except:
                traceback.print_exc()
                os._exit(70)

    def stop(self):
        if self.serverpid != -1:
            signals = [signal.SIGTERM, signal.SIGTERM, signal.SIGKILL]
            if os.environ.get('COVERAGE_DIR', None):
                signals.insert(0, signal.SIGUSR2)
            for signum in signals:
                os.kill(self.serverpid, signum)
                pid, status = os.waitpid(self.serverpid, os.WNOHANG)
                if pid:
                    # Process successfully reaped
                    self.serverpid = -1
                    break
                time.sleep(0.5)
            else:
                # Still not dead 0.5s after a SIGKILL, keep waiting.
                os.waitpid(self.serverpid, 0)
        if self.reposDB:
            self.reposDB.stop()
            self.reposDB = None
        if self.serverFilePath and self.delServerPath:
            util.rmtree(self.serverFilePath)
            self.serverFilePath = None

    def isStarted(self):
        return self.serverpid != -1 or self.reposDB is not None

    def getServerConfigPath(self):
        return os.path.join(self.reposDir, 'serverrc')


class ExistingServer(RepositoryServer):
    def __init__(self, name, port=8000):
        RepositoryServer.__init__(self, [name])
        self.port = port
        self.reposDir = None
        self.name = name
        #self.reset()

    def reset(self):
        repos = netclient.StandaloneServer(self.getMap())
        repos.c[self.name].reset()
        self.needsPGPKey = True

    def start(self):
        pass

    def stop(self):
        pass

class ProxyServerOverrides:

    def writeServerConfigFile(self, configFile, **kw):
        configValues = {
            'proxyContentsDir'      : self.contents.getPath(),
            'changesetCacheDir'     : os.path.join(self.reposDir, 'cscache'),
            'tmpDir'                : self.reposDir,
            'traceLog'              : '3 %s' %
                                      os.path.join(self.reposDir, 'trace.log'),
            'logFile'               : os.path.join(self.reposDir, 'proxy.log')
        }

        if self.capsuleServerUrl:
            configValues['capsuleServerUrl'] = self.capsuleServerUrl
            # We only proxy localhost1
            configValues['injectCapsuleContentServers'] = 'localhost1'

        if self.cacheTimeout is not None:
            configValues['memCacheTimeout'] = "%d" % self.cacheTimeout

        if self.cacheLocation is not None:
            configValues['memCache'] = "%s" % self.cacheLocation

        # discard this for proxies
        kw.pop('conaryrc', False)

        if self.proxies:
            for k, v in self.proxies.items():
                if v.startswith('conary'):
                    configValues["conaryProxy " + k] = "http" + v[6:]
                else:
                    configValues["proxy " + k] = v

        # Add the command line options
        configValues.update(kw)

        if self.sslEnabled and self.useSSL and self.sslCert:
            self.writeSSLRepoConfig(configValues)

        f = open(configFile, "w+")
        for item in configValues.iteritems():
            print >> f, "%s %s" % item
        for ent in self.entitlements:
            print >> f, "entitlement %s %s" % ent
        for user in self.users:
            print >> f, "user %s %s %s" % user

        f.close()

    def updateConfig(self, cfg):
        cfg.configLine("conaryProxy http http://localhost:%s" % self.port)

    def reset(self):
        self.contents.reset()
        csCacheDir = os.path.join(self.reposDir, 'cscache')
        shutil.rmtree(csCacheDir, ignore_errors = True)
        os.mkdir(csCacheDir)

        self.contents.reset()

    def __init__(self, entitlements, users, capsuleServerUrl, cacheTimeout,
                 cacheLocation):
        self.entitlements = entitlements
        self.users = users
        self.capsuleServerUrl = capsuleServerUrl
        self.cacheTimeout = cacheTimeout
        self.cacheLocation = cacheLocation

class StandaloneProxyServer(ProxyServerOverrides, StandaloneServer):

    def __init__(self, *args, **kwargs):
        entitlements = kwargs.pop('entitlements', [])
        users = kwargs.pop('users', [])
        capsuleServerUrl = kwargs.pop('capsuleServerUrl', None)
        cacheTimeout = kwargs.pop('cacheTimeout', None)
        cacheLocation = kwargs.pop('cacheLocation', None)
        ProxyServerOverrides.__init__(self, entitlements, users,
            capsuleServerUrl, cacheTimeout, cacheLocation)
        StandaloneServer.__init__(self, *args, **kwargs)

class StandaloneSSLProxyServer(ProxyServerOverrides, StandaloneServer):

    def __init__(self, *args, **kwargs):
        entitlements = kwargs.pop('entitlements', [])
        users = kwargs.pop('users', [])
        capsuleServerUrl = kwargs.pop('capsuleServerUrl', None)
        ProxyServerOverrides.__init__(self, entitlements, users,
            capsuleServerUrl)
        kwargs['useSSL'] = True
        kwargs['forceSSL'] = True
        StandaloneServer.__init__(self, *args, **kwargs)


class ApacheProxyServer(ProxyServerOverrides, ApacheServer):

    def __init__(self, *args, **kwargs):
        entitlements = kwargs.pop('entitlements', [])
        users = kwargs.pop('users', [])
        capsuleServerUrl = kwargs.pop('capsuleServerUrl', None)
        cacheTimeout = kwargs.pop('cacheTimeout', None)
        cacheLocation = kwargs.pop('cacheLocation', None)
        ProxyServerOverrides.__init__(self, entitlements, users,
            capsuleServerUrl, cacheTimeout, cacheLocation)
        ApacheServer.__init__(self, *args, **kwargs)

class ApacheSSLProxyServer(ProxyServerOverrides, ApacheSSLServer):
    def __init__(self, *args, **kwargs):
        entitlements = kwargs.pop('entitlements', [])
        users = kwargs.pop('users', [])
        capsuleServerUrl = kwargs.pop('capsuleServerUrl', None)
        ProxyServerOverrides.__init__(self, entitlements, users,
            capsuleServerUrl)
        ApacheSSLServer.__init__(self, *args, **kwargs)

class ServerCache:
    serverType = ''

    def __init__(self):
        self.servers = [ None ] * 5

    def stopServer(self, serverIdx):
        if self.servers[serverIdx] is not None:
            server = self.servers[serverIdx]
            self.servers[serverIdx] = None
            server.stop()

    def getCachedServer(self, serverIdx):
        return self.servers[serverIdx]

    def startServer(self, reposDir, conaryPath, SQLserver, serverIdx = 0,
                    requireSigs = False, serverName = None, authCheck = None,
                    entCheck = None, authTimeout = None,
                    readOnlyRepository=False, needsPGPKey=True,
                    useSSL=False, sslCert=None, sslKey=None, forceSSL = False,
                    closed = False, commitAction = None, deadlockRetry = None,
                    resetDir = True, excludeCapsuleContents = False,
                    **kwargs):
        if self.servers[serverIdx] is not None:
            self.servers[serverIdx].resetIfNeeded()
            return self.servers[serverIdx]

        if serverName is None:
            name = 'localhost'
            if serverIdx > 0:
                name += str(serverIdx)
        else:
            name = serverName

        envname = 'CONARY_SERVER'
        if serverIdx > 0:
            reposDir += '-%d' %serverIdx
            envname += str(serverIdx)

        if not os.path.isdir(reposDir):
            os.mkdir(reposDir)

        reposDB = SQLserver.getDB("testdb%d" % serverIdx,
            keepExisting = not resetDir)

        server, serverClass, serverDir, proxyClass, proxy = \
                                        self.getServerClass(envname, useSSL)
        if not serverClass and serverDir:
            return server

        contents = ContentStore(reposDir + '/contents')

        self.servers[serverIdx] = serverClass(name, reposDB, contents, server,
                                              serverDir,
                                              reposDir,
                                              conaryPath,
                                              self.getMap(),
                                              requireSigs,
                                              authCheck = authCheck,
                                              entCheck = entCheck,
                                              authTimeout = authTimeout,
                                              readOnlyRepository = readOnlyRepository,
                                              serverIdx = serverIdx,
                                              useSSL = useSSL,
                                              sslCert = sslCert,
                                              sslKey = sslKey,
                                              forceSSL = forceSSL,
                                              deadlockRetry = deadlockRetry,
                                              closed = closed,
                                              commitAction = commitAction,
                                              excludeCapsuleContents =
                                                    excludeCapsuleContents,
                                              **kwargs)
        self.servers[serverIdx].start(resetDir = resetDir)
        return self.servers[serverIdx]

    def getServerClass(self, envname, useSSL):
        useDefault = False
        server = os.environ.get(envname, None)
        kw = {}
        if server is None:
            useDefault = True
            server = os.environ.get('CONARY_SERVER', None)
        if server is None:
            server = resources.get_path('conary', 'server', 'server.py')

        if useSSL and server.startswith('apache') and 'ssl' not in server:
            server = 'apachessl' + server[6:]

        srvClassMap = [
            ('apache', ApacheServer),
            ('apachecached', ApacheServerWithCache),
            ('apachessl', ApacheSSLServer),
            ('apachesslcached', ApacheSSLServerWithCache),
        ]
        serverDir = serverClass = None
        for srvName, srvClass in srvClassMap:
            if server == srvName:
                serverDir = resources.get_path('conary', 'server')
                serverClass = srvClass
                break
            if server.startswith(srvName + ':'):
                server = serverDir = server.split(":", 1)[1]
                serverClass = srvClass
                break

        if serverDir is None: # Not found in the previous mappings
            # use localhost: only if explicitly set
            if server.startswith("localhost:") and not useDefault:
                server = 'localhost'
                serverDir = None
                serverClass = ExistingServer
            else:
                serverClass = StandaloneServer
                serverDir = os.path.dirname(server)

        conaryProxyType = os.environ.get('CONARY_PROXY', None)
        if conaryProxyType is not None:
            if conaryProxyType == 'standalone':
                proxyClass = StandaloneProxyServer
                #proxyPath = server
                proxyPath = resources.get_path('conary', 'server', 'server.py')
            elif conaryProxyType == 'apache':
                proxyClass = ApacheProxyServer
                proxyPath = server
            else:
                raise ValueError, "Unknown setting for CONARY_PROXY '%s'" % \
                        conaryProxyType
        else:
            proxyClass = None
            proxyPath = None

        return server, serverClass, serverDir, proxyClass, proxyPath

    def resetAllServers(self):
        for i in range(len(self.servers)):
            server = self.servers[i]
            if server is not None:
                server.reset()

    def resetAllServersIfNeeded(self):
        for i in range(len(self.servers)):
            server = self.servers[i]
            if server is not None:
                server.resetIfNeeded()

    def stopAllServers(self, clean=False):
        for i in range(len(self.servers)):
            server = self.servers[i]
            if server is not None:
                self.servers[i] = None
                server.stop()
                if clean and hasattr(server, 'reposDir'):
                    util.rmtree(server.reposDir, ignore_errors=True)


    def getServer(self, serverIdx=0):
        return self.servers[serverIdx]

    def getMap(self):
        servers = {}
        for server in self.servers:
            if server:
                servers.update(server.getMap())
        return servers

    def getServerNames(self):
        for server in self.servers:
            if server:
                yield server.getName()

if 'CONARY_PATH_REPOS' in os.environ:
    env = os.environ.copy()
    env['CONARY_PATH'] = env['CONARY_PATH_REPOS']
    del env['CONARY_PATH_REPOS']
    # Reset PYTHONPATH
    del env['PYTHONPATH']
    cwd = os.environ.get('CONARY_TEST_PATH', None)
    import serverCacheProxy
    _servers = serverCacheProxy.ServerCacheProxy(cwd=cwd, env=env)
else:
    _servers = ServerCache()
_reposDir = None
_proxy = None
_httpProxy = None

def getReposDir(globalvar, testname):
    if globalvar:
        return globalvar
    if _isIndividual():
        globalvar = '/tmp/%s/repos' % testname
        if not os.access(globalvar, os.W_OK):
            globalvar = ('/tmp/%s-%s/repos' % (testname, os_utils.effectiveUser))
    else:
        globalvar = testhelp.getTempDir('%s-repos-' % testname)
        # add /repos to the end of this so that the proxy can live in
        # this temp directory too
        globalvar += '/repos'
        os.mkdir(globalvar)
    return globalvar

class RepositoryHelper(testhelp.TestCase):
    topDir = None
    defLabel = versions.Label("localhost@rpl:linux")

    def __init__(self, *args, **kw):
        testhelp.TestCase.__init__(self, *args, **kw)
        global _servers, _proxy
        self.servers = _servers
        self.proxy = _proxy

    def _getReposDir(self):
        ## Need to be able to override this in base classes
        global _reposDir
        _reposDir = getReposDir(_reposDir, 'conarytest')
        return _reposDir

    def setUp(self):
        if 'CONARY_IDGEN' in os.environ:
            className = "IdGen%s" % os.environ['CONARY_IDGEN']
            cook._IdGen = sys.modules[__name__].__dict__[className]

        if _isIndividual():
            self.topDir = '/tmp/conarytest-%s' % os_utils.effectiveUser

        if self.topDir:
            self.tmpDir = self.topDir
        else:
            self.tmpDir = testhelp.getTempDir('conarytest-')

        self.reposDir = self._getReposDir()
        self.workDir = self.tmpDir + "/work"
        self.buildDir = self.tmpDir + "/build"
        self.rootDir = self.tmpDir + "/root"
        self.cacheDir = self.tmpDir + "/cache"
        self.configDir = self.tmpDir + "/cfg"

        if self.topDir:
            self.reset()

        self.cfg = conarycfg.ConaryConfiguration(False)
        self.cfg.name = 'Test'
        self.cfg.contact = 'http://bugzilla.rpath.com/'
        self.cfg.installLabelPath = conarycfg.CfgLabelList([ self.defLabel ])
        self.cfg.installLabel = self.defLabel
        self.cfg.buildLabel = self.defLabel
        self.cfg.buildPath = self.buildDir
        self.cfg.dbPath = '/var/lib/conarydb'
        self.cfg.debugRecipeExceptions = False
        self.cfg.repositoryMap.clear()
        self.cfg.useDir = None
        self.cfg.quiet = True
        self.cfg.resolveLevel = 1
        self.cfg.user.addServerGlob('*', 'test', 'foo')
        # Keep HTTP retries from blocking the testsuite.
        self.cfg.connectAttempts = 1

        global _proxy
        self.proxy = _proxy

        # set the siteConfigPath to be absolute, only including the
        # files from the current conary that's being tested.
        conaryDir = resources.get_path()
        if conaryDir.startswith('/usr') or '_ROOT_' in conaryDir:
            if '_ROOT_' in conaryDir:
                # we're doing testall - use a different path
                offset = conaryDir.index('_ROOT_') + len('_ROOT_')
            else:
                offset = 0
            siteDir = conaryDir[:offset] + '/etc/conary/site'
            componentDir = conaryDir[:offset] + '/etc/conary/components'
        else:
            siteDir = conaryDir + '/config/site'
            componentDir = conaryDir + '/config/components'
        self.cfg.siteConfigPath = [ cfgtypes.Path(siteDir) ]
        self.cfg.componentDirs = [ cfgtypes.Path(componentDir) ]

        # this causes too much trouble; turn it on only for the
        # test(s) where we care
        self.cfg.configComponent = False

        self.cfg.root = self.rootDir
        self.cfg.lookaside = self.cacheDir
        os.umask(0022)

        # set up the flavor based on the defaults in use
        self.initializeFlavor()
        self._origDir = os.getcwd()
        testhelp.TestCase.setUp(self)

        self.cfg.buildLabel = self.defLabel
        self.cfg.installLabelPath = conarycfg.CfgLabelList([self.defLabel])
        self.cfg.defaultMacros = [
                resources.get_archive('macros'),
                resources.get_archive('macros.d', '*'),
                resources.get_archive('site'),
                ]
        self.cfg.siteConfigPath = [resources.get_archive('site')]
        self.logFilter.clear()
        self.cfg.sourceSearchDir = self.sourceSearchDir = resources.get_archive()
        self.cfg.enforceManagedPolicy = False
        self.cfg.resolveLevel = 2
        self.cfg.updateThreshold = 10
        policyPath = os.environ.get('CONARY_POLICY_PATH',
                                    '/usr/lib/conary/policy').split(':')
        self.cfg.policyDirs = policyPath
        self.cfg.baseClassDir = self.tmpDir + "/baseclasses"

        # set up the keyCache so that it won't prompt for passwords in
        # any of the test suites.
        keyCache = openpgpkey.OpenPGPKeyFileCache()
        openpgpkey.setKeyCache(keyCache)

        # pre-populate private key cache
        keyCache.setPrivatePath(resources.get_archive('secring.gpg'))
        self.prepopulateKeyCache(keyCache)

        # Create dummy keyring
        pubRing = util.joinPaths(self.configDir, 'gpg', 'pubring.gpg')

        # pre-populate public key cache
        self.cfg.pubRing = [ pubRing, resources.get_archive('pubring.gpg') ]
        keyCache.setPublicPath(self.cfg.pubRing)
        keyCache.getPublicKey('')

        self.origTroveVersion = (trove.TROVE_VERSION, trove.TROVE_VERSION_1_1)

        if self.topDir and os.path.isdir(self.tmpDir):
            self.reset()
        else:
            if not os.path.exists(self.tmpDir):
                os.mkdir(self.tmpDir)
            os.mkdir(self.workDir)
            os.mkdir(self.rootDir)
            os.mkdir(self.cacheDir)
            os.mkdir(self.configDir)

        # save the original environment
        self.origEnv = dict(os.environ)
        # default recipes only need to be loaded for certain tests
        loadrecipe._defaultsLoaded = True

        # Reset IP cache
        httputils.IPCache.clear()

    def prepopulateKeyCache(self, keyCache):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        keyCache.getPrivateKey(fingerprint, '111111')
        keyCache.getPrivateKey('', '111111')

    def tearDown(self):
        log.setVerbosity(log.WARNING)
        self.resetFlavors()
        os.chdir(self._origDir)
        testhelp.TestCase.tearDown(self)

        if not _isIndividual():
            self.reset()
            shutil.rmtree(self.tmpDir)
        trove.TROVE_VERSION = self.origTroveVersion[0]
        trove.TROVE_VERSION_1_1 = self.origTroveVersion[1]
        self.logFilter.clear()
        # restore the environment
        for key in os.environ.keys():
            del os.environ[key]
        for key, value in self.origEnv.iteritems():
            os.environ[key] = value
        if hasattr(self, 'cfg'):
            del self.cfg

    def getRepositoryClient(self, user = 'test', password = 'foo',
                            serverIdx = None, repositoryMap = None):
        cfg = copy.copy(self.cfg)
        self.cfg.entitlementDirectory = '/tmp'
        cfg.user = conarycfg.UserInformation()
        if repositoryMap is None:
            cfg.repositoryMap = conarycfg.RepoMap()
            if serverIdx is None:
                for name in self.servers.getServerNames():
                    cfg.user.addServerGlob(name, user, password)
                    cfg.repositoryMap.update(self.servers.getMap())
            else: # we only need to talk to a particular server instance
                server = self.servers.getCachedServer(serverIdx)
                if server is not None:
                    for name in server.nameList:
                        cfg.user.addServerGlob(name, user, password)
                    cfg.repositoryMap.update(server.getMap())
        else:
            cfg.repositoryMap = repositoryMap

        client = conaryclient.ConaryClient(cfg)
        return client.getRepos()

    def addUserAndRole(self, repos, reposLabel, user, pw):
        repos.addRole(reposLabel, user)
        repos.addUser(reposLabel, user, pw)
        repos.updateRoleMembers(reposLabel, user, [user])

    def setupUser(self, repos, reposLabel, user, pw, troves, label):
        self.addUserAndRole(repos, reposLabel, user, pw)
        repos.addAcl(reposLabel, user, troves, label)
        repos.setRoleCanMirror(reposLabel, user, True)

        return self.getRepositoryClient(user = user, password = pw)

    def printRepMap(self):
        rmap = self.servers.getMap()
        for name, url in rmap.items():
            print 'repositoryMap %s %s' %(name, url)

    def openDatabase(self, root=None):
        if root is None:
            root = self.rootDir
        return database.Database(root, self.cfg.dbPath)

    def getConaryClient(self):
        return conaryclient.ConaryClient(self.cfg)

    def openRepository(self, *args, **kwargs):
        for count in range(3):
            try:
                return self.__openRepository(*args, **kwargs)
            except:
                time.sleep(0.5 * (count + 1))
        return self.__openRepository(*args, **kwargs)

    def __openRepository(self, serverIdx=0, requireSigs=False,
                       serverName=None, authCheck=None, entCheck=None,
                       readOnlyRepository=False, proxies=None,
                       useSSL=False, sslCert=None, sslKey=None,
                       authTimeout=None, forceSSL=False,
                       closed=False, commitAction=None,
                       deadlockRetry=None, serverCache=None,
                       resetDir=True, excludeCapsuleContents=False,
                       **kwargs):

        if serverCache is None:
            serverCache = self.servers
        server = serverCache.getCachedServer(serverIdx)
        SQLserver = sqlharness.start(self.topDir)
        newServer = server is None
        reposDir = self.reposDir
        if newServer:
            if serverCache.serverType:
                reposDir += '-%s' % serverCache.serverType
            server = serverCache.startServer(reposDir, resources.get_path(), SQLserver,
                                          serverIdx, requireSigs, serverName,
                                          authCheck = authCheck,
                                          entCheck = entCheck,
                                          readOnlyRepository=readOnlyRepository,
                                          useSSL = useSSL,
                                          sslCert = sslCert,
                                          sslKey = sslKey,
                                          authTimeout = authTimeout,
                                          forceSSL = forceSSL,
                                          closed = closed,
                                          commitAction = commitAction,
                                          deadlockRetry = deadlockRetry,
                                          excludeCapsuleContents =
                                                excludeCapsuleContents,
                                          resetDir = resetDir,
                                          **kwargs)
            # We keep this open to stop others from reusing the port; tell the
            # code which tracks fd leaks so this doesn't reported
            if hasattr(server, 'socket'):
                self._expectedFdLeak(server.socket.fileno())
        else:
            server.setNeedsReset()


        if 'CONARY_HTTP_PROXY' in os.environ:
            httpAddr = os.environ['CONARY_HTTP_PROXY']
            global _httpProxy
            if _httpProxy is None and httpAddr:
                proxyPath = os.path.join(
                                os.path.dirname(self.reposDir), "http-proxy")
                _httpProxy = self.getHTTPProxy(path = proxyPath)

        # make sure map is up to date
        self.cfg.repositoryMap.update(serverCache.getMap())

        serverPath, serverClass, serverDir, proxyClass, proxyPath = \
                            serverCache.getServerClass('CONARY_SERVER', useSSL)
        if newServer and proxyPath:
            # if we're using a proxy, (re)start it with the right server map
            proxyDir = os.path.dirname(self.reposDir) + '/proxy'
            contents = ContentStore(proxyDir + '/contents')
            if self.proxy:
                self.proxy.stop()

            if proxies is None and _httpProxy is not None:
                # No proxies were specified, and we have an HTTP proxy. Use it
                # for the Conary proxy
                d = lambda: 1
                _httpProxy.updateConfig(d)
                pp = d.proxy
            else:
                pp = proxies

            self.proxy = proxyClass('proxy', None,
                                    contents, proxyPath, None,
                                    proxyDir, resources.get_path(),
                                    self.cfg.repositoryMap,
                                    None, proxies = pp)
            self.proxy.start()
            global _proxy
            _proxy = self.proxy

        if self.proxy:
            self.proxy.updateConfig(self.cfg)
        elif _httpProxy:
            _httpProxy.updateConfig(self.cfg)

        count = 0
        client = conaryclient.ConaryClient(self.cfg)
        repos = client.getRepos()

        label = versions.Label("%s@rpl:linux" % server.getName())

        # Try to connect several times with the much safer tryConnect
        # (which consumes one port, not one port per iteration)
        # We wait 60 seconds. Sometimes we're seeing the build bots being
        # overloaded, and spawning a server might take longer than 10 seconds
        # to come up
        try:
            sock_utils.tryConnect('localhost', server.port)
        except socket.error, e:
            msg = 'unable to open networked repository: %s' %str(e)
            print >> sys.stderr, msg
            print >> sys.stderr, 'server log follows:'
            print >> sys.stderr, '-------------------'
            f = open(server.serverLog, 'r')
            print >> sys.stderr, f.read()
            f.close()
            try:
                # We failed to connect. Try to stop the server,
                # hopefully that unblocks the next test
                self.stopRepository(serverIdx)
            except Exception, e:
                print traceback.format_exc()
                print 'failed to stop server: %s: %s' % (e.__class__.__name__, e)
            raise RuntimeError(msg)

        if self.proxy:
            sock_utils.tryConnect('localhost', self.proxy.port)

        # There may be other things that were not fully started yet, like HTTP
        # caches and so on. We'll now try an end-to-end connection.

        ready = False
        while count < 500:
            try:
                try:
                    repos.troveNames(label)
                    ready = True
                    break
                except Exception, e:
                    if closed and closed in str(e):
                        # If in closed mode, stop looping when we get the message
                        # we expect
                        break
                    raise
            except repo_errors.OpenError:
                pass

            time.sleep(0.01)
            count += 1
        if not ready:
            if closed:
                # Well, this is a closed repo, we expect it not to pass some
                # of the tests above
                return repos

            #import epdb, sys
            #epdb.post_mortem(sys.exc_info()[2])

            try:
                self.stopRepository(serverIdx)
            except:
                pass
            raise RuntimeError('unable to open networked repository: %s'
                               %str(e))

        if server.needsPGPKey and not server.readOnlyRepository:
            ascKey = open(resources.get_archive('key.asc'), 'r').read()
            repos.addNewAsciiPGPKey(label, 'test', ascKey)
            server.needsPGPKey = False
        return repos

    def addfile(self, *fileList, **kwArgs):
        cvccmd.sourceCommand(self.cfg, ( "add", ) + fileList, kwArgs )

    def revertSource(self, *fileList, **kwArgs):
        cvccmd.sourceCommand(self.cfg, ( "revert", ) + fileList, kwArgs )

    def setSourceFlag(self, path, text = None, binary = None):
        if text:
            argSet = { 'text' : True }
        elif binary:
            argSet = { 'binary' : True }
        else:
            argSet = {}

        cvccmd.sourceCommand(self.cfg, [ "set", path ], argSet )

    def describe(self, file):
        cvccmd.sourceCommand(self.cfg, [ 'describe', file ], {} )

    def markRemovedCmd(self, trvSpec):
        oldStdin = sys.stdin
        sys.stdin = StringIO.StringIO("Y\n")
        try:
            self.discardOutput(cvccmd.sourceCommand, self.cfg,
                               [ 'markremoved', trvSpec ], {} )
        finally:
            sys.stdin = oldStdin

    def markRemoved(self, trvSpec, repos = None):
        oldStdin = sys.stdin
        sys.stdin = StringIO.StringIO("Y\n")
        if not repos:
            repos = self.openRepository()
        try:
            self.discardOutput(checkin.markRemoved, self.cfg, repos, trvSpec)
        finally:
            sys.stdin = oldStdin

    def mkbranch(self, src, newVer, what=None, shadow = False, binaryOnly=False,
                 sourceOnly=False, ignoreConflicts=True, targetFile = None):
        if isinstance(src, (list, tuple)):
            troveSpecs = src
        else:
            assert(what)
            if type(src) == str and src[0] != "/" and src.find("@") == -1:
                assert(what)
                src = "/" + self.cfg.buildLabel.asString() + "/" + src
            elif isinstance(src, (versions.Version, versions.Label)):
                assert(what)
                src = src.asString()

            troveSpecs = [what + '=' + src]

        if type(newVer) == str and newVer[0] == "@":
            newVer = versions.Label("localhost" + newVer)
        elif isinstance(newVer, str):
            newVer = versions.Label(newVer)

        repos = self.openRepository()

        branch.branch(repos, self.cfg, newVer.asString(),
                      troveSpecs = troveSpecs, makeShadow = shadow,
                      sourceOnly = sourceOnly, binaryOnly = binaryOnly,
                      forceBinary = True, ignoreConflicts = ignoreConflicts,
                      targetFile = targetFile)

    def checkout(self, nameList, versionStr = None, dir = None,
            callback = None):
        dict = {}
        if dir:
            dict = { "dir" : dir }

        if callback is None:
            callback = checkin.CheckinCallback()

        if type(nameList) is str:
            nameList = [ nameList ]

        if versionStr:
            assert(len(nameList) == 1)
            cvccmd.sourceCommand(self.cfg, [ "checkout",
                                          nameList[0] +'='+ versionStr ],
                                 dict, callback=callback)
        else:
            cvccmd.sourceCommand(self.cfg, [ "checkout" ] + nameList, dict,
                                 callback=callback)

    def refresh(self, globs = None, logLevel=log.INFO):

        callback = checkin.CheckinCallback()

        level = log.getVerbosity()
        log.setVerbosity(logLevel)
        cvccmd.sourceCommand(self.cfg, [ "refresh", globs ], None,
                             callback=callback)
        log.setVerbosity(level)

    def commit(self, logLevel=log.INFO, callback=None, message = 'foo',
               cfg=None):
        self.openRepository()
        if not callback:
            callback = checkin.CheckinCallback()
        if not cfg:
            cfg = self.cfg
        level = log.getVerbosity()
        log.setVerbosity(logLevel)
        cvccmd.sourceCommand(cfg, [ "commit" ], { 'message' : message },
                             callback=callback)
        log.setVerbosity(level)

    def context(self, name = None, cfg=None):
        if cfg is None:
            cfg = self.cfg
        cvccmd.sourceCommand(cfg, [ "context", name ], {})

    def diff(self, *args, **kwargs):
        self.openRepository()
        (retVal, str) = self.captureOutput(cvccmd.sourceCommand,
                                           self.cfg, [ "diff" ] + list(args),
                                           {})
        # (working version) Thu Jul  1 09:51:03 2004 (no log message)
        # -> (working version) (no log message)
        str = re.sub(r'\) .* \(', ') (', str, 1)
        if kwargs.get('rc', None) is not None:
            assert(kwargs['rc'] == retVal)
        return str

    def stat(self, *args):
        self.openRepository()
        (rc, str) = self.captureOutput(cvccmd.sourceCommand,
                                       self.cfg, [ "stat" ] + list(args), {})
        return str


    def removeDateFromLogMessage(self, str):
        # 1.0-2 Test (http://buzilla.rpath.com/) Mon Nov 22 12:11:24 2004
        # -> 1.0-2 Test
        str = re.sub(r'\(http://bugzilla.rpath.com/\) .*', '', str)
        return str

    def showLog(self, *args, **argSet):
        self.openRepository()
        (rc, str) = self.captureOutput(cvccmd.sourceCommand,
                                        self.cfg, [ "log" ] + list(args), argSet)
        return self.removeDateFromLogMessage(str)

    def annotate(self, *args):
        self.openRepository()
        (rc, str) = self.captureOutput(cvccmd.sourceCommand,
                                       self.cfg, [ "annotate" ] + list(args),
                                        {})
        # remove date information from string
        # also remove variable space padding
        str = re.sub(r'(\n[^ ]*) *\((.*) .*\):', r'\1 (\2):', str)
        # first line doesn't have an \n
        str = re.sub(r'^([^ ]*) *\((.*) .*\):', r'\1 (\2):', str)
        return str

    def rdiff(self, *args):
        self.openRepository()
        (rc, str) = self.captureOutput(cvccmd.sourceCommand,
                                       self.cfg, [ "rdiff" ] + list(args),
                                        {})
        str = re.sub(r'\(http://bugzilla.rpath.com/\).*', '(http://bugzilla.rpath.com/)', str, 1)
        return str

    def rollbackList(self, root):
        db = database.Database(root, self.cfg.dbPath)
        (rc, str) = self.captureOutput(rollbacks.listRollbacks, db, self.cfg)
        return str

    def rollbackCount(self):
        f = open(self.rootDir + "/var/lib/conarydb/rollbacks/status")
        l = f.readline()
        (min, max) = l.split()
        max = int(max)
        return max

    def rollback(self, root, num = None, replaceFiles = False, tagScript = None,
                 justDatabase = False, showInfoOnly = False,
                 abortOnError = False, capsuleChangesets = []):
        # hack to allow the root as the first parameter
        if num is None and type(root) == int:
            num = root
            root = self.rootDir

        self.cfg.root = root
        client = conaryclient.ConaryClient(self.cfg)
        repos = self.openRepository()
        try:
            ret = client.applyRollback("r.%d" % num,
                                       replaceFiles=replaceFiles,
                                       tagScript = tagScript,
                                       justDatabase = justDatabase,
                                       showInfoOnly = showInfoOnly,
                                       abortOnError = abortOnError,
                                       capsuleChangesets = capsuleChangesets)
        finally:
            client.close()
        return ret

    def newpkg(self, name, factory = None):
        self.openRepository()
        args = {}
        if factory:
            args['factory'] = factory
        cvccmd.sourceCommand(self.cfg, [ "newpkg", name], args)

    def makeSourceTrove(self, name, recipeFile, buildLabel=None,
                        extraFiles=None, factory=None):
        if factory is None:
            factory = (name.startswith('factory-') and 'factory') or None

        oldBuildLabel = self.cfg.buildLabel
        if buildLabel:
            self.cfg.buildLabel = buildLabel
        origDir = os.getcwd()
        try:
            os.chdir(self.workDir)
            self.newpkg(name, factory = factory)
            os.chdir(name)
            if recipeFile is not None:
                self.writeFile(name + '.recipe', recipeFile)
                self.addfile(name + '.recipe')
            if extraFiles:
                for fname in extraFiles:
                    kwargs = dict(binary=False, text=False)
                    if isinstance(fname, tuple):
                        fname, ftype = fname[:2]
                        if ftype == 'binary':
                            kwargs['binary'] = True
                        elif ftype == 'text':
                            kwargs['text'] = True

                    bname = os.path.basename(fname)
                    shutil.copy(fname, bname)
                    self.addfile(bname, **kwargs)
            self.commit()
        finally:
            os.chdir(origDir)
            self.cfg.buildLabel = oldBuildLabel

    def updateSourceTrove(self, name, recipeFile, versionStr=None):
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.checkout(name, versionStr=versionStr)
        os.chdir(name)
        self.writeFile(name + '.recipe', recipeFile)
        self.commit()
        os.chdir(origDir)

    def remove(self, name):
        self.openRepository()
        cvccmd.sourceCommand(self.cfg, [ "remove", name], {})

    def rename(self, oldname, newname):
        self.openRepository()
        cvccmd.sourceCommand(self.cfg, [ "rename", oldname, newname], {})

    def update(self, *args):
        self.openRepository()
        callback = checkin.CheckinCallback()
        cvccmd.sourceCommand(self.cfg, [ "update" ] + list(args), {},
                          callback=callback)

    def resetFlavors(self):
        use.clearFlags()
        use.track(False)

    def resetAllRepositories(self):
        self.servers.resetAllServers()
        if self.proxy:
            self.proxy.reset()

    def merge(self, *args):
        self.openRepository()
        cvccmd.sourceCommand(self.cfg, [ "merge" ] + list(args), {})

    def resetRepository(self, serverIdx=0):
        server = self.servers.getServer(serverIdx)
        if server is not None:
            server.reset()
        self.openRepository(serverIdx=serverIdx)

    def stopRepository(self, serverIdx=0):
        server = self.servers.getServer(serverIdx)
        if server is not None:
            self.servers.stopServer(serverIdx)

    def resetWork(self):
        if os.path.exists(self.workDir):
            util.rmtree(self.workDir)
        if os.path.exists(self.workDir):
            raise IOError, '%s exists but must not!' %self.workDir
        util.mkdirChain(self.workDir)

    def resetRoot(self):
        if os.path.exists(self.rootDir):
            util.rmtree(self.rootDir)
        util.mkdirChain(self.rootDir)

    def resetCache(self):
        if os.path.exists(self.cacheDir):
            util.rmtree(self.cacheDir)
        util.mkdirChain(self.cacheDir)

    def reset(self):
        self.servers.resetAllServersIfNeeded()
        self.resetWork()
        self.resetRoot()
        self.resetCache()

    def _cvtVersion(self, verStr, source=False):
        if isinstance(verStr, versions.Version):
            return verStr
        verStr = str(verStr)
        if verStr[0] == ':':
            buildLabel = self.cfg.buildLabel
            if '/' not in verStr:
                verStr += '/1'
            verStr = '/%s@%s%s' % (buildLabel.getHost(),
                                   buildLabel.getNamespace(),
                                   verStr)
        elif verStr[0] == '@':
            buildLabel = self.cfg.buildLabel
            if '/' not in verStr:
                verStr += '/1'
            verStr = '/%s%s' % (buildLabel.getHost(), verStr)
        elif verStr[0] != '/':
            if '@' in verStr:
                if '/' not in verStr:
                    verStr += '/1'
                verStr = '/%s' % verStr
            else:
                verStr = '/%s/%s' % (self.cfg.buildLabel.asString(), verStr)
        if '-' not in verStr:
            if source:
                verStr += '-1'
            else:
                verStr += '-1-1'

        try:
            v = versions.VersionFromString(verStr).copy()
            v.resetTimeStamps()
            return v
        except errors.ParseError:
            pass
        try:
            newVerStr = verStr
            if '-' not in verStr.rsplit('/')[-1]:
                if source:
                    newVerStr += '-1'
                else:
                    newVerStr += '-1-1'
            v = versions.VersionFromString(newVerStr).copy()
            v.resetTimeStamps()
            return v
        except errors.ParseError:
            pass
        v = versions.ThawVersion(verStr)
        return v

    def addCollection(self, name, version=None, strongList=None,
                      weakRefList=None, calcSize=False, repos=None,
                      defaultFlavor=None, createComps=False,
                      existsOkay=False, redirect=None,
                      changeSetFile = None, buildReqs=None,
                      labelPath=None, sourceName=None,
                      preUpdateScript = None, postInstallScript = None,
                      postUpdateScript = None,
                      preRollbackScript = None, postRollbackScript = None,
                      preInstallScript = None, preEraseScript = None,
                      postEraseScript = None,
                      compatClass = None, flavor = None,
                      loadedReqs=None, metadata=None, imageGroup = None,
                      pathConflicts=None):
        if not repos:
            repos = self.openRepository()

        if isinstance(version, (list, tuple)):
            strongList = version
            version = None
        assert(strongList or redirect)

        if version is None and defaultFlavor is None:
            name, version, defaultFlavor = cmdline.parseTroveSpec(name)
            if not version:
                version = '1.0'

        version = self._cvtVersion(version)
        assert(':' not in name and not name.startswith('fileset'))

        if defaultFlavor is None:
            defaultFlavor = deps.Flavor()
        elif isinstance(defaultFlavor, str):
            defaultFlavor = deps.parseFlavor(defaultFlavor)

        fullList = {}

        if weakRefList is None:
            hasWeakRefs = False
            weakRefList = []
        else:
            hasWeakRefs = True

        idx = 0
        for weakRef, troveList in zip((False, True), (strongList, weakRefList)):
            for info in troveList:
                idx += 1
                trvFlavor = None
                trvVersion = None
                byDefault = True
                if isinstance(info, str):
                    trvName = info
                    if '=' in trvName or '[' in trvName:
                        (trvName, trvVersion, trvFlavor) \
                                        = cmdline.parseTroveSpec(trvName)
                elif isinstance(info, trove.Trove):
                    (trvName, trvVersion, trvFlavor) \
                                        = info.getNameVersionFlavor()
                elif len(info) == 1:
                    (trvName,) = info
                elif len(info) == 2:
                    (trvName, item) = info
                    if isinstance(item, bool):
                        byDefault = item
                    else:
                        trvVersion = item
                elif len(info) == 3:
                    (trvName, trvVersion, trvFlavor) = info
                elif len(info) == 4:
                    (trvName, trvVersion, trvFlavor, byDefault) = info
                else:
                    assert(False)

                if trvVersion is None and trvFlavor is None:
                    (trvName, trvVersion, trvFlavor) = \
                                cmdline.parseTroveSpec(trvName)

                if not trvVersion:
                    trvVersion = version
                else:
                    trvVersion = self._cvtVersion(trvVersion)

                if trvFlavor is None:
                    trvFlavor = defaultFlavor
                elif type(trvFlavor) == str:
                    trvFlavor = deps.parseFlavor(trvFlavor)

                if trvName[0] == ':':
                    trvName = name + trvName

                if createComps and ':' in trvName:
                    self.addComponent(trvName, trvVersion.freeze(),
                                      str(trvFlavor), filePrimer=idx, sourceName=sourceName)

                fullList[(trvName, trvVersion, trvFlavor)] = (byDefault,
                                                              weakRef)
        if flavor:
            flavor = deps.parseFlavor(flavor, raiseError=True)
        else:
            flavor = deps.mergeFlavorList([x[2] for x in fullList],
                                          deps.DEP_MERGE_TYPE_DROP_CONFLICTS)

        if not hasWeakRefs:
            collList = [ x for x in fullList.iteritems() if trove.troveIsCollection(x[0][0])]
            troves = repos.getTroves([x[0] for x in collList], withFiles=False)

            for (troveTup, (byDefault, _)), trv in itertools.izip(collList,
                                                                  troves):
                for childInfo in trv.iterTroveList(strongRefs=True,
                                                   weakRefs=True):
                    childByDefault = (trv.includeTroveByDefault(*childInfo)
                                      and byDefault)
                    currInfo = fullList.get(childInfo, None)
                    if currInfo:
                        childByDefult = currInfo[0] or childByDefault
                        fullList[childInfo] = (childByDefault, currInfo[1])
                    else:
                        fullList[childInfo] = (childByDefault, True)

        if redirect is not None:
            redirectList = []
            for info in redirect:
                if info is None:
                    redirectList.append((None, None, None))
                    continue
                if not isinstance(info, (list, tuple)):
                    info = [info]
                if len(info) == 1:
                    redirName = info[0]
                    redirBranch = version.branch()
                    redirFlavor = flavor
                elif len(info) == 2:
                    redirName, redirBranch = info
                    redirBranch = versions.VersionFromString(redirBranch)
                    redirFlavor = deps.parseFlavor('')
                else:
                    redirName, redirBranch, redirFlavor = info
                    redirBranch = versions.VersionFromString(redirBranch)
                    redirFlavor = deps.parseFlavor(redirFlavor)
                redirectList.append((redirName, redirBranch, redirFlavor))

        # add a pkg diff
        if redirect is not None:
            troveType = trove.TROVE_TYPE_REDIRECT
        else:
            troveType = trove.TROVE_TYPE_NORMAL

        coll = trove.Trove(name, version, flavor, None, type=troveType)
        if existsOkay and repos.hasTrove(name, version, flavor):
            return repos.getTrove(name, version, flavor)

        coll.setIsCollection(True)
        coll.setSize(0)         # None is widely used as a shortcut
        for info, (byDefault, weakRef) in fullList.iteritems():
            coll.addTrove(*info, **dict(byDefault=byDefault, weakRef=weakRef))
        if not sourceName:
            sourceName = name + ':source'
        coll.setSourceName(sourceName)
        coll.setBuildTime(1238075164.694746)

        if redirect:
            for toName, toBranch, toFlavor in redirectList:
                coll.addRedirect(toName, toBranch, toFlavor)
        else:
            coll.setProvides(deps.parseDep('trove: %s' % name))

        if buildReqs is not None:
            buildReqs = self.makeTroveTupleList(buildReqs)
            coll.setBuildRequirements(buildReqs)
        if loadedReqs:
            loadedReqs = self.makeTroveTupleList(loadedReqs)
            coll.setLoadedTroves(loadedReqs)

        if pathConflicts is not None:
            for path in pathConflicts:
                coll.troveInfo.pathConflicts.append(path)

        if labelPath:
            coll.setLabelPath(labelPath)

        if compatClass:
            coll.setCompatibilityClass(compatClass)

        for spec, script in \
            [ (preUpdateScript,    coll.troveInfo.scripts.preUpdate),
              (postInstallScript,  coll.troveInfo.scripts.postInstall),
              (postUpdateScript,   coll.troveInfo.scripts.postUpdate),
              (preRollbackScript,  coll.troveInfo.scripts.preRollback),
              (postRollbackScript, coll.troveInfo.scripts.postRollback),
              (preInstallScript,   coll.troveInfo.scripts.preInstall),
              (preEraseScript,     coll.troveInfo.scripts.preErase),
              (postEraseScript,    coll.troveInfo.scripts.postErase), ]:
            if spec is None: continue
            if type(spec) == str:
                spec = TroveScript(script = spec)

            script.script.set(spec.script)

            if spec.conversions:
                assert(compatClass)
                assert(isinstance(spec, RollbackScript))
                script.conversions.addList(
                        (compatClass, x) for x in spec.conversions)

        if calcSize:
            compList = [x for x in fullList \
                                    if not trove.troveIsCollection(x[0])]
            size = (x.getSize() \
                    for x in repos.getTroves(compList, withFiles=False))
            coll.setSize(sum(x for x in size if x is not None))

        if metadata:
            if not isinstance(metadata, (list, tuple)):
                metadata = [metadata]
            for item in metadata:
                coll.troveInfo.metadata.addItem(item)

        if imageGroup is not None:
            coll.troveInfo.imageGroup.set(imageGroup)

        coll.computeDigests()
        assert(redirect or list(coll.iterTroveList(strongRefs=True)))

        # create an absolute changeset
        cs = changeset.ChangeSet()
        diff = coll.diff(None, absolute = True)[0]
        cs.newTrove(diff)

        if changeSetFile:
            cs.addPrimaryTrove(coll.getName(), coll.getVersion(),
                               coll.getFlavor())
            cs.writeToFile(changeSetFile)
        else:
            repos.commitChangeSet(cs)

        return coll

    addQuickTestCollection = addCollection

    def makeTroveTupleList(self, troveItems, defaultFlavor=None):
        tupleList = []
        for item in troveItems:
            if isinstance(item, trove.Trove):
                tupleList.append(item.getNameVersionFlavor())
            elif isinstance(item, tuple):
                bflv = None
                if len(item) == 1:
                    bver = self._cvtVersion('1')
                    bname = item
                elif len(item) == 2:
                    if isinstance(item[0], trove.Trove):
                        bname, bver = item[0].getName(), item[0].getVersion()
                        bflv = item[1]
                    else:
                        bname, bver = item
                        bver = self._cvtVersion(bver)
                else:
                    bname, bver, bflv = item
                    bver = self._cvtVersion(bver)
                if bflv is None:
                    bflv = defaultFlavor
                elif isinstance(bflv, str):
                    bflv = deps.parseFlavor(bflv)
                tupleList.append((bname, bver, bflv))
            else:
                assert 0, "unknown tuple source %s" % item
        return tupleList

    def addComponent(self, *args, **kw):
        changeSetFile = kw.pop('changeSetFile', None)
        hidden = kw.pop('hidden', False)

        repos = kw.pop('repos', None)
        if not repos:
            repos = self.openRepository()

        kw['repos'] = repos

        t, cs = self.Component(*args, **kw)

        if cs is not None:
            if changeSetFile:
                cs.writeToFile(changeSetFile)
            else:
                repos.commitChangeSet(cs, hidden = hidden)

        return t

    def Component(self, troveName, version=None, flavor=None,
                  fileContents=None,
                  provides=deps.DependencySet(),
                  requires=deps.DependencySet(),
                  filePrimer=0, setConfigFlags=True,
                  repos=None, existsOkay=False, pathIdSalt='',
                  redirect=None, sourceName=None, metadata=None,
                  factory=None, capsule=None,
                  versus=None, buildTime=1238075164.694746):

        if isinstance(flavor, list):
            fileContents = flavor
            flavor = None
        elif isinstance(version, list):
            fileContents = version
            version = None

        if version is None and flavor is None:
            troveName, version, flavor = cmdline.parseTroveSpec(troveName)
            if not version:
                version = '1.0'
        if flavor is None:
            flavor = ''

        troveVersion = self._cvtVersion(version,
                                         source=troveName.endswith(':source'))
        isSource = troveName.endswith(':source')

        assert(':' in troveName or trove.troveIsFileSet(troveName))

        componentDir = self.workDir + "/component"
        if os.path.exists(componentDir):
            shutil.rmtree(componentDir)
        util.mkdirChain(componentDir)
        if isinstance(flavor, list):
            assert(fileContents is None)
            fileContents = flavor
            flavor = ''
        flavor = deps.parseFlavor(flavor)

        # add a pkg diff
        if redirect is not None:
            troveType = trove.TROVE_TYPE_REDIRECT
        else:
            troveType = trove.TROVE_TYPE_NORMAL

        # we create the trove with the wrong flavor here and fix it later
        # (by unioning in the file flavors)
        t = trove.Trove(troveName, troveVersion, flavor, None, type=troveType)

        # set up a file with some contents
        fileList = []
        if redirect is not None:
            redirectList = []
            assert(fileContents is None)
            for info in redirect:
                if info is None:
                    continue

                if not isinstance(info, (list, tuple)):
                    info = [info]
                if len(info) == 1:
                    redirName = info[0]
                    redirBranch = troveVersion.branch()
                    redirFlavor = flavor
                elif len(info) == 2:
                    redirName, redirBranch = info
                    redirBranch = versions.VersionFromString(redirBranch)
                    redirFlavor = None
                else:
                    redirName, redirBranch, redirFlavor = info
                    redirBranch = versions.VersionFromString(redirBranch)
                    if redirFlavor is not None:
                        redirFlavor = deps.parseFlavor(redirFlavor)
                redirectList.append((redirName, redirBranch, redirFlavor))
        else:
            if capsule:
                assert(capsule.endswith('.rpm'))
                f = files.FileFromFilesystem(capsule,
                                             trove.CAPSULE_PATHID)
                hdr = rpmhelper.readHeader(open(capsule))
                t.addRpmCapsule(os.path.basename(capsule),
                                troveVersion,
                                f.fileId(), hdr)
                fileList.append((f, trove.CAPSULE_PATHID,
                                 filecontents.FromFilesystem(capsule)))
            elif fileContents is None:
                path = '/contents%s' % filePrimer
                contents = 'hello, world!\n'

                if not filePrimer:
                    filePrimer = '\0'
                filePrimer = str(filePrimer)
                # Pad it on the left with zeros, up to 16 chars long
                pathId = filePrimer.rjust(16, '\1')
                pathId = pathId[0:16]

                contents = RegularFile(contents = contents, pathId = pathId,
                        config = None)
                fileContents = [(path, contents)]

            index = 0
            for fileInfo in fileContents:
                fileReq = None
                fileProv = None
                fileFlavor = None

                if isinstance(fileInfo, str):
                    fileInfo = [fileInfo, 'foo']

                fileName, contents = fileInfo[0:2]
                if isinstance(contents, filetypes._File):
                    assert(len(fileInfo) == 2)
                else:
                    if len(fileInfo) > 3:
                        if isinstance(fileInfo[3], (list, tuple)):
                            fileReq = fileInfo[3][0]
                            fileProv = fileInfo[3][1]
                        else:
                            fileReq = fileInfo[3]

                    if len(fileInfo) > 2 and fileInfo[2] is not None:
                        fileVersion = self._cvtVersion(fileInfo[2])
                    else:
                        fileVersion = troveVersion

                    contents = RegularFile(requires = fileReq,
                                           provides = fileProv,
                                           contents = contents)
                    contents.version = fileVersion

                cont = componentDir + '/' + fileName
                dir = os.path.dirname(cont)
                if not os.path.exists(dir):
                    util.mkdirChain(dir)

                pathId = contents.pathId
                if pathId is None:
                    pathId = sha1helper.md5String(pathIdSalt + fileName)
                else:
                    pathId += '0' * (16 - len(pathId))
                f = contents.get(pathId)

                f.flags.isSource(isSource)

                if contents.config is not None:
                    f.flags.isConfig(contents.config)
                elif ((setConfigFlags and fileName.startswith('/etc'))
                        or troveName.endswith(':source')):
                    f.flags.isConfig(True)
                index += 1

                if capsule and not (f.flags.isConfig() or 
                                getattr(contents, 'isGhost', None)):
                    # RBL-5684: we force ghost files to not be marked as
                    # payload
                    f.flags.isEncapsulatedContent(True)

                if contents.version:
                    fileVersion = self._cvtVersion(contents.version)
                elif (versus and versus.hasFile(pathId) and
                        versus.getFile(pathId)[1] == f.fileId()):
                    # reuse file version if it hasn't changed
                    fileVersion = versus.getFile(pathId)[2]
                else:
                    fileVersion = troveVersion

                if not troveName.endswith(':source'):
                    if fileName[0] != '/':
                        fileName = '/' + fileName

                assert(len(pathId) == 16)
                t.addFile(pathId, fileName, fileVersion, f.fileId())

                if hasattr(contents, 'contents'):
                    fileList.append((f, pathId, contents.contents))
                else:
                    fileList.append((f, pathId, None))

        # find the flavor for this trove; it depends on the flavors of the
        # files
        for f, pathId, contents in fileList:
            flavor.union(f.flavor())
        t.changeFlavor(flavor)

        # create an absolute changeset
        cs = changeset.ChangeSet()

        if existsOkay and repos.hasTrove(troveName, troveVersion, flavor):
            return repos.getTrove(troveName, troveVersion, flavor), None

        if factory is not None:
            t.setFactory(factory)

        if not redirect:
            if isinstance(requires, str):
                req = deps.parseDep(requires)
            else:
                req = requires.copy()

            if isinstance(provides, str):
                prov = deps.parseDep(provides)
            else:
                prov = provides.copy()

            prov.union(deps.parseDep('trove: %s' % t.getName()))

            for f, pathId, contents in fileList:
                req.union(f.requires())
                prov.union(f.provides())

            t.setRequires(req)
            t.setProvides(prov)

        if not troveName.endswith(':source'):
            if not sourceName:
                sourceName = troveName.split(":")[0] + ":source"
            t.setSourceName(sourceName)

        t.computePathHashes()

        t.setBuildTime(buildTime)

        if redirect:
            for toName, toBranch, toFlavor in redirectList:
                t.addRedirect(toName, toBranch, toFlavor)

        if redirect:
            for toName, toBranch, toFlavor in redirectList:
                t.addRedirect(toName, toBranch, toFlavor)

        size = 0
        # add the file and file contents
        for f, pathId, contents in fileList:
            cs.addFile(None, f.fileId(), f.freeze())
            if f.hasContents and not f.flags.isEncapsulatedContent():
                cs.addFileContents(pathId, f.fileId(),
                                   changeset.ChangedFileTypes.file, contents,
                                   f.flags.isConfig())
                size += f.contents.size()
        if metadata:
            if not isinstance(metadata, (tuple, list)):
                metadata = [metadata]
            for item in metadata:
                t.troveInfo.metadata.addItem(item)
        t.setSize(size)
        t.computeDigests()

        diff = t.diff(None, absolute = True)[0]
        cs.newTrove(diff)
        cs.setPrimaryTroveList([t.getNameVersionFlavor()])

        return t, cs
    addQuickTestComponent = addComponent

    def addDbComponent(self, db, name, version='1', flavor='',
                       provides=deps.DependencySet(),
                       requires=deps.DependencySet()):
        fileList = []

        # create a file
        cont = self.workDir + '/contents'
        f = open(cont, 'w')
        f.write('hello, world!\n')
        f.close()
        pathId = sha1helper.md5FromString('0' * 32)
        f = files.FileFromFilesystem(cont, pathId)
        fileList.append((f, cont, pathId))

        v = self._cvtVersion(version)
        flavor = deps.parseFlavor(flavor)
        t = trove.Trove(name, v, flavor, None)
        for f, name, pathId in fileList:
            t.addFile(pathId, '/' + name, v, f.fileId())
        t.setRequires(requires)
        t.setProvides(provides)
        info = db.addTrove(t)
        db.addTroveDone(info)
        db.commit()
        return t

    addQuickDbTestPkg = addDbComponent

    def addRPMComponent(self, nameSpec, rpmPath, versus = None,
        fileContents=None, requires=deps.DependencySet()):

        if rpmPath[0] != '/':
            rpmPath = resources.get_archive(rpmPath)

        f = open(rpmPath, "r")
        h = rpmhelper.readHeader(f)
        expandDir = self.workDir + '/rpm'
        if os.path.exists(expandDir):
            shutil.rmtree(expandDir)
        os.mkdir(expandDir)

        p = os.popen("cd %s; cpio --quiet -iumd" % (expandDir, ), "w")
        rpmhelper.extractRpmPayload(f, p)
        p.close()
        f.close()

        fl = []
        for path, mode, flags, linksTo, fileColor, rdev in itertools.izip(
                        h[rpmhelper.OLDFILENAMES],
                        h[rpmhelper.FILEMODES], h[rpmhelper.FILEFLAGS],
                        h[rpmhelper.FILELINKTOS],
                        h[rpmhelper.FILECOLORS],
                        h[rpmhelper.FILERDEVS]):
            if stat.S_ISDIR(mode):
                fl.append((path, Directory()))
            elif stat.S_ISBLK(mode) or stat.S_ISCHR(mode):
                minor = rdev & 0xff | (rdev >> 12) & 0xffffff00
                major = (rdev >> 8) & 0xfff
                if stat.S_ISBLK(mode):
                    fl.append((path, BlockDevice(major, minor)))
                elif stat.S_ISCHR(mode):
                    fl.append((path, CharacterDevice(major, minor)))
            else:
                isConfig = ((flags & rpmhelper.RPMFILE_CONFIG) != 0)
                isGhost = ((flags & rpmhelper.RPMFILE_GHOST) != 0)
                # You can't have symlinks that are initialContents
                if stat.S_ISLNK(mode):
                    fobj = Symlink(linksTo)
                else:
                    if isGhost:
                        contents = ''
                        # can't have files which are both initialContents
                        # and config
                        isConfig = False
                    else:
                        contents = open(expandDir + path)

                    if fileColor == 2:
                        req = 'abi: ELF64(SysV x86_64)'
                    elif fileColor == 1:
                        req = 'abi: ELF32(SysV x86)'
                    else:
                        req = None

                    fobj = RegularFile(contents = contents,
                                       config = isConfig,
                                       initialContents = isGhost,
                                       requires = req)
                    if isGhost:
                        # RBL-5684: we force ghost files to not be marked as
                        # payload (see Component)
                        fobj.isGhost = True

                fl.append((path, fobj))
        fl.extend(fileContents or [])

        return self.addComponent(nameSpec, fileContents = fl,
                                 capsule = rpmPath, versus = versus,
                                 requires=requires)

    def addTestPkg(self, num, requires=[], fail=False, content='',
                   flags=[], localflags=[], packageSpecs=[], subPackages=[],
                   version='1.0', branch=None,
                   header='', fileContents='',
                   tag=None, binary=False):
        """ This method is a wrapper around the recipes.py createRecipe
            method.  It creates the recipe with the given characteristics,
            and then commits it to the repository.

            num = recipe name is 'test%(num)s
            requires = other packages added to the buildRequires of
                        this package
            fail - if true, an exit(1) is added
            fileContents - contents of the text file in the package
            content - place to add content to the recipe setup() function
            header - place to add content to the recipe before setup()
            branch - place this source component on a branch
            localFlags - check Flags.foo for this recipe for every foo passed in
            flags - check use.[Arch,Use].foo, for every [Arch,Use].foo passed in
        """
        origDir = os.getcwd()
        os.chdir(self.workDir)
        pkgname = 'test%d' % num
        if not 'packages' in self.__dict__:
            self.packages = {}
        if num in self.packages:
            self.checkout(pkgname, branch)
        else:
            self.newpkg(pkgname)
        os.chdir(pkgname)
        if not isinstance(subPackages, (tuple, list)):
            subPackages = [subPackages]
        if not isinstance(packageSpecs, (tuple, list)):
            packageSpecs = [packageSpecs]
        fileContents = recipes.createRecipe(num, requires, fail, content,
            packageSpecs, subPackages, version=version, flags=flags,
            localflags=localflags, header=header, fileContents=fileContents,
            tag=tag, binary=binary)
        self.writeFile(pkgname + '.recipe', fileContents)
        if num not in self.packages:
            self.addfile(pkgname + '.recipe')
        self.commit()
        os.chdir('..')
        shutil.rmtree(pkgname)
        os.chdir(origDir)
        self.packages[num] = pkgname
        return fileContents

    def cookTestPkg(self, num, logLevel=log.WARNING, macros={}, prep=False):
        stdout = os.dup(sys.stdout.fileno())
        stderr = os.dup(sys.stderr.fileno())
        null = os.open('/dev/null', os.O_WRONLY)
        os.dup2(null, sys.stdout.fileno())
        os.dup2(null, sys.stderr.fileno())
        try: 
            return cook.cookItem(self.repos, self.cfg, 'test%s' % num, macros=macros, prep=prep)
        finally:
            os.dup2(stdout, sys.stdout.fileno())
            os.dup2(stderr, sys.stderr.fileno())
            os.close(null)
            os.close(stdout)
            os.close(stderr)

    def createMetadataItem(self, **kw):
        mi = trove.MetadataItem()
        for key, value in kw.items():
            if isinstance(value, (list, tuple)):
                for val in value:
                    getattr(mi, key).set(val)
            else:
                getattr(mi, key).set(value)
        return mi

    def cookFromRepository(self, troveName, buildLabel = None, ignoreDeps = False, repos = None, logBuild = False, callback = None):
        if buildLabel:
            oldLabel = self.cfg.buildLabel
            self.cfg.buildLabel = buildLabel

        if not repos:
            repos = self.openRepository()

        built = self.discardOutput( cook.cookItem, repos, self.cfg, troveName, 
                                    ignoreDeps = ignoreDeps, logBuild = logBuild,
                                    callback = callback )
        if buildLabel:
            self.cfg.buildLabel = oldLabel

        return built[0]

    def verifyFifo(self, file):
        return stat.S_ISFIFO(os.lstat(file).st_mode)

    def verifyFile(self, path, contents=None, perms=None):
        f = open(path, "r")
        other = f.read()
        if contents is not None:
            if other != contents:
                self.fail("contents incorrect for %s" % path)
            assert(other == contents)
        if perms is not None:
            assert(os.stat(path)[stat.ST_MODE] & 0777 == perms)

    def verifyNoFile(self, file):
        try:
            f = open(file, "r")
        except IOError, err:
            if err.errno == 2:
                return
            else:
                self.fail("verifyNoFile returned unexpected error code: %d" % err.errno)
        else:
            self.fail("file exists: %s" % file)

    def verifySrcDirectory(self, contents, dir = "."):
        self.verifyDirectory(contents + [ "CONARY" ], dir)

    def verifyDirectory(self, contents, dir = "."):
        self.verifyFileList(contents, os.listdir(dir))

    def verifyPackageFileList(self, pkg, ideal):
        list = [ x[1] for x in pkg.iterFileList() ]
        self.verifyFileList(ideal, list)

    def verifyTroves(self, pkg, ideal):
        actual = [ (x[0], x[1].asString(), x[2]) \
                            for x in pkg.iterTroveList(strongRefs=True) ]
        if sorted(actual) != sorted(ideal):
            self.fail("troves don't match expected: got %s expected %s"
                      %(actual, ideal))

    def verifyFileList(self, ideal, actual):
        dict = {}
        for n in ideal: dict[n] = 1

        for n in actual:
            if dict.has_key(n):
                del dict[n]
            else:
                self.fail("unexpected file %s" % n)
        if dict:
            self.fail("files missing %s" % " ".join(dict.keys()))

        assert(not dict)

    def verifyInstalledFileList(self, dir, list):
        paths = {}
        for path in list:
            paths[path] = True
        dirLen = len(dir)

        # this skips all of /var/lib/conarydb/
        for (dirName, dirNameList, pathNameList) in os.walk(dir):
            for path in pathNameList:
                if path[0] == ".": continue
                fullPath = dirName[dirLen:] + "/" + path
                if fullPath == "/var/log/conary": continue

                if fullPath.startswith("/var/lib/conarydb/"): continue
                if paths.has_key(fullPath):
                    del paths[fullPath]
                else:
                    self.fail("unexpected file %s" % fullPath)

        if paths:
            self.fail("files missing %s" % " ".join(paths.keys()))

    def cookItem(self, *args, **kw):
        return self.discardOutput(cook.cookItem, *args, **kw)

    def cookObject(self, loader, prep=False, macros={}, sourceVersion = None,
                   serverIdx = 0, ignoreDeps = False, logBuild = False, 
                   targetLabel = None, repos = None, 
                   groupOptions = None, resume = None):
        theClass = loader.getRecipe()
        if repos is None:
            repos = self.openRepository(serverIdx)
        if sourceVersion is None:
            sourceVersion = cook.guessSourceVersion(repos, theClass.name,
                                                    theClass.version,
                                                    self.cfg.buildLabel,
                                                    searchBuiltTroves=True)[0]
        if not sourceVersion:
            # just make up a sourceCount -- there's no version in
            # the repository to compare against
            sourceVersion = versions.VersionFromString('/%s/%s-1' % (
                                               self.cfg.buildLabel.asString(),
                                               theClass.version))
        use.resetUsed()

        try:
            builtList = self.discardOutput(cook.cookObject, repos, self.cfg, 
                                           [loader],
                                           sourceVersion,
                                           prep=prep, macros=macros,
                                           allowMissingSource=True,
                                           ignoreDeps=ignoreDeps,
                                           logBuild=logBuild,
                                           groupOptions=groupOptions,
                                           resume=resume)
        finally:
            repos.close()

        return builtList

    def cookPackageObject(self, theClass, prep=False, macros={},
                          sourceVersion = None, serverIdx = 0,
                          ignoreDeps = False):
        """ cook a package object, return the buildpackage components
            and package obj
        """
        repos = self.openRepository(serverIdx)
        if sourceVersion is None:
            sourceVersion, _ = cook.guessSourceVersion(repos, theClass.name,
                                                       theClass.version,
                                                       self.cfg.buildLabel,
                                                       searchBuiltTroves=True)
        if not sourceVersion:
            # just make up a sourceCount -- there's no version in
            # the repository to compare against
            sourceVersion = versions.VersionFromString('/%s/%s-1' % (
                                               self.cfg.buildLabel.asString(),
                                               theClass.version))
        use.resetUsed()
        stdout = os.dup(sys.stdout.fileno())
        stderr = os.dup(sys.stderr.fileno())
        null = os.open('/dev/null', os.O_WRONLY)
        os.dup2(null, sys.stdout.fileno())
        os.dup2(null, sys.stderr.fileno())

        try:
            res = cook._cookPackageObject(repos, self.cfg, theClass,
                                                          sourceVersion,
                                                          prep=prep, macros=macros,
                                                          ignoreDeps=ignoreDeps)

        finally:
            os.dup2(stdout, sys.stdout.fileno())
            os.dup2(stderr, sys.stderr.fileno())
            os.close(null)
            os.close(stdout)
            os.close(stderr)
        repos.close()
        if not res:
            return None
        #return bldList, recipeObj
        return res[0:2]

    def getRecipeObjFromRepos(self, name, repos):
        stdout = os.dup(sys.stdout.fileno())
        stderr = os.dup(sys.stderr.fileno())
        null = os.open('/dev/null', os.O_WRONLY)
        os.dup2(null, sys.stdout.fileno())
        os.dup2(null, sys.stderr.fileno())

        try:
            loader, sourceVersion = loadrecipe.recipeLoaderFromSourceComponent(
                                                    name, self.cfg, repos)[0:2]
            recipeObj = cook._cookPackageObject(repos, self.cfg, loader,
                                        sourceVersion, prep=True, requireCleanSources=True)
        finally:
            os.dup2(stdout, sys.stdout.fileno())
            os.dup2(stderr, sys.stderr.fileno())
            os.close(null)
            os.close(stdout)
            os.close(stderr)
        return recipeObj

    def repairTroves(self, pkgList = [], root = None):
        if root is None:
            root = self.rootDir

        repos = self.openRepository()
        db = self.openDatabase(root = root)

        troveList = []
        for item in pkgList:
            name, ver, flv = updatecmd.parseTroveSpec(item)
            troves = db.findTrove(None, (name, ver, flv))
            troveList += troves

        db.repairTroves(repos, troveList)

    def updatePkg(self, root, pkg=[], version = None, tagScript = None,
                  noScripts = False, keepExisting = False, replaceFiles = None,
                  resolve = False, depCheck = True, justDatabase = False,
                  flavor = None, recurse = True, sync = False,
                  info = False, fromFiles = [], checkPathConflicts = True,
                  test = False, migrate = False, keepRequired = None,
                  raiseError = False, callback = None, restartInfo = None,
                  applyCriticalOnly = False, syncChildren = False,
                  keepJournal = False, noRestart=False,
                  exactFlavors = False, replaceManagedFiles = False,
                  replaceModifiedFiles = False, replaceUnmanagedFiles = False,
                  replaceModifiedConfigFiles = False, skipCapsuleOps = False,
                  criticalUpdateInfo = None, modelFile = None):

        if not isinstance(root, str) or not root[0] == '/':
            # hack to allow passing of rootdir as first argument
            # as we used to
            if isinstance(root, list):
                pkg = root
            else:
                pkg = [root]
            root = self.rootDir

        newcfg = self.cfg
        newcfg.root = root

        if callback is None:
            callback = callbacks.UpdateCallback()

        if replaceFiles is not None:
            replaceManagedFiles = replaceFiles
            replaceUnmanagedFiles = replaceFiles
            replaceModifiedFiles = replaceFiles
            replaceModifiedConfigFiles = replaceFiles

        repos = self.openRepository()
        if isinstance(pkg, (str, list)):
            if isinstance(pkg, str):
                if version is not None:
                    if type(version) is not str:
                        version = version.asString()
                    item = "%s=%s" % (pkg, version)
                else:
                    item = pkg

                if flavor is not None:
                    item += '[%s]' % flavor

                pkgl = [ item ]

            else:
                assert(version is None)
                assert(flavor is None)

                pkgl = list(itertools.chain(*(util.braceExpand(x) for x in pkg)))
            # For consistency's sake, if in migrate mode, fake the command
            # line to say migrate
            if migrate:
                newSysArgv = [ 'conary', 'migrate' ]
            else:
                newSysArgv = [ 'conary', 'update' ]
            oldSysArgv = sys.argv

            # Add the packages to handle
            newSysArgv.extend(pkgl)

            newcfg.autoResolve = resolve
            try:
                if keepJournal:
                    k = { 'keepJournal' : True }
                else:
                    k = {}

                try:
                    sys.argv = newSysArgv
                    updatecmd.doUpdate(newcfg, pkgl,
                                       tagScript=tagScript,
                                       keepExisting=keepExisting,
                                       replaceManagedFiles=\
                                                    replaceManagedFiles,
                                       replaceUnmanagedFiles=\
                                                    replaceUnmanagedFiles,
                                       replaceModifiedFiles=\
                                                    replaceModifiedFiles,
                                       replaceModifiedConfigFiles=\
                                                    replaceModifiedConfigFiles,
                                       depCheck=depCheck,
                                       justDatabase=justDatabase,
                                       recurse=recurse, split=True,
                                       sync=sync, info=info,
                                       fromFiles=fromFiles,
                                       checkPathConflicts=checkPathConflicts,
                                       test=test, migrate=migrate,
                                       keepRequired=keepRequired,
                                       callback=callback,
                                       restartInfo=restartInfo,
                                       applyCriticalOnly=applyCriticalOnly,
                                       syncChildren=syncChildren,
                                       forceMigrate=migrate,
                                       noRestart=noRestart,
                                       exactFlavors=exactFlavors,
                                       criticalUpdateInfo=criticalUpdateInfo,
                                       skipCapsuleOps=skipCapsuleOps,
                                       noScripts=noScripts,
                                       systemModelFile=modelFile,
                                       **k)
                finally:
                    sys.argv = oldSysArgv
            except conaryclient.DependencyFailure, msg:
                if raiseError:
                    raise
                print msg
            except errors.InternalConaryError, err:
                raise
            except errors.ConaryError, msg:
                if raiseError:
                    raise
                log.error(msg)
        else:
            # we have a changeset object; mimic what updatecmd does
            assert(not info)
            assert(not fromFiles)
            assert(not test)
            assert(checkPathConflicts)
            cl = conaryclient.ConaryClient(self.cfg)
            cl.setUpdateCallback(callback)
            job = [ (x[0], (None, None), (x[1], x[2]),
                        not keepExisting) for x in
                            pkg.getPrimaryTroveList() ]
            try:
                try:
                    updJob, suggMap = cl.updateChangeSet(job,
                                        keepExisting = keepExisting,
                                        keepRequired = keepRequired,
                                        recurse = recurse, split = True,
                                        sync = sync,
                                        fromChangesets = [ pkg ])
                    if depCheck:
                        assert(not suggMap)
                    if replaceFiles is None:
                        replaceFiles = False
                    # old applyUpdate API doesn't support separate args
                    assert(not replaceManagedFiles)
                    assert(not replaceUnmanagedFiles)
                    assert(not replaceModifiedFiles)
                    assert(not replaceModifiedConfigFiles)
                    cl.applyUpdate(updJob, replaceFiles = replaceFiles,
                                   tagScript = tagScript, justDatabase = justDatabase,
                                   keepJournal = keepJournal)
                finally:
                    updJob.close()
                    cl.close()
            except conaryclient.DependencyFailure, msg:
                if raiseError:
                    raise
                print msg
            except errors.InternalConaryError, err:
                raise
            except errors.ConaryError, err:
                if raiseError:
                    raise
                log.error(err)

    def verifyDatabase(self):
        db = self.openDatabase()
        for info in list(db.iterAllTroves()):
            assert db.getTrove(*info).verifyDigests(), "Update failed"


    def updateAll(self,  **kw):
        updatecmd.updateAll(self.cfg, **kw)

    def localChangeset(self, root, pkg, fileName):
        db = database.Database(root, self.cfg.dbPath)
        newcfg = copy.deepcopy(self.cfg)
        newcfg.root = root
        db = database.Database(root, self.cfg.dbPath)
        newcfg = copy.deepcopy(self.cfg)
        newcfg.root = root

        verify.LocalChangeSetCommand(db, newcfg, pkg, fileName)

        db.close()

    def changeset(self, repos, troveSpecs, fileName, recurse=True):
        cscmd.ChangeSetCommand(self.cfg, troveSpecs, fileName,
                               recurse=recurse)

    def erasePkg(self, root, pkg, version = None, tagScript = None,
                 depCheck = True, justDatabase = False, flavor = None,
                 test = False, recurse=True, callback = None,
                 skipCapsuleOps = False):
        db = database.Database(root, self.cfg.dbPath)

        try:
            if type(pkg) == list:
                sys.argv = [ 'conary', 'erase' ] + pkg
                updatecmd.doUpdate(self.cfg, pkg,
                                   tagScript = tagScript, depCheck = depCheck,
                                   justDatabase = justDatabase,
                                   updateByDefault = False, test = test,
                                   recurse=recurse, callback=callback,
                                   skipCapsuleOps = skipCapsuleOps)
            elif version and flavor:
                item = "%s=%s[%s]" % (pkg, version, flavor)
                sys.argv = [ 'conary', 'erase', item ]
                updatecmd.doUpdate(self.cfg, [ item ],
                                   tagScript = tagScript, depCheck = depCheck,
                                   justDatabase = justDatabase,
                                   updateByDefault = False, test = test,
                                   recurse=recurse, callback=callback,
                                   skipCapsuleOps = skipCapsuleOps)
            elif version:
                item = "%s=%s" % (pkg, version)
                sys.argv = [ 'conary', 'erase', item ]
                updatecmd.doUpdate(self.cfg, [ item ],
                                   tagScript = tagScript, depCheck = depCheck,
                                   justDatabase = justDatabase,
                                   updateByDefault = False, test = test,
                                   recurse=recurse, callback=callback,
                                   skipCapsuleOps = skipCapsuleOps)
            elif flavor:
                item = "%s[%s]" % (pkg, flavor)
                sys.argv = [ 'conary', 'erase', item ]
                updatecmd.doUpdate(self.cfg, [ item ],
                                   tagScript = tagScript, depCheck = depCheck,
                                   justDatabase = justDatabase,
                                   updateByDefault = False, test = test,
                                   recurse=recurse, callback=callback,
                                   skipCapsuleOps = skipCapsuleOps)
            else:
                sys.argv = [ 'conary', 'erase', pkg ]
                updatecmd.doUpdate(self.cfg, [ pkg ],
                                   tagScript = tagScript, depCheck = depCheck,
                                   justDatabase = justDatabase,
                                   updateByDefault = False, test = test,
                                   recurse=recurse, callback=callback,
                                   skipCapsuleOps = skipCapsuleOps)
        except conaryclient.DependencyFailure, msg:
            print msg
        except errors.ClientError, msg:
            log.error(msg)
        db.close()

    def restoreTrove(self, root, *troveList):
        rmv = conarycmd.RestoreCommand()
        cfg = copy.copy(self.cfg)
        cfg.root = root
        return rmv.runCommand(cfg, {}, ( 'conary', 'restore' ) + troveList)

    def removeFile(self, root, *pathList):
        rmv = conarycmd.RemoveCommand()
        cfg = copy.copy(self.cfg)
        cfg.root = root
        return rmv.runCommand(cfg, {}, ( 'conary', 'remove' ) + pathList)

    def build(self, str, name, vars = None, buildDict = None,
              sourceVersion = None, serverIdx = 0, logLevel = log.WARNING,
              returnTrove = None, macros=None, prep = False):
        (built, d) = self.buildRecipe(str, name, d = buildDict,
                                      vars = vars,
                                      sourceVersion = sourceVersion,
                                      logLevel = logLevel, macros = macros,
                                      prep = prep)
        if prep:
            return
        (name, version, flavor) = built[0]

        if returnTrove is None:
            returnTroveList = [ name ]
        else:
            name = name.split(':')[0]

            if not isinstance(returnTrove, (list, tuple)):
                l = ( returnTrove, )
            else:
                l = returnTrove

            returnTroveList = []
            for compName in l:
                if compName[0] == ':':
                    returnTroveList.append(name + compName)
                else:
                    returnTroveList.append(compName)

        version = versions.VersionFromString(version)
        repos = self.openRepository(serverIdx)
        trvList = repos.getTroves(
                    [ (x, version, flavor) for x in returnTroveList ] )

        if isinstance(returnTrove, (list, tuple)):
            return trvList

        return trvList[0]

    def buildRecipe(self, theClass, theName, vars = None, prep=False,
                    macros = None,
                    sourceVersion = None, d = None, serverIdx = 0,
                    logLevel = None, ignoreDeps=False,
                    logBuild=False, repos = None, groupOptions = None,
                    resume = None, branch = None):
        use.setBuildFlagsFromFlavor(theName, self.cfg.buildFlavor)
        if logLevel is None:
            logLevel = log.WARNING

        if vars is None:
            vars = {}

        if macros is None:
            macros = {}

        if branch is None:
            if sourceVersion is not None:
                branch = sourceVersion.branch()
            else:
                branch  = versions.Branch([self.cfg.buildLabel])

        built = []

        if repos is None:
            repos = self.openRepository()


        loader = LoaderFromString(theClass, "/test.recipe", cfg = self.cfg,
                                  repos = repos, objDict = d,
                                  component = theName)

        recipe = loader.getRecipe()

        for name in vars.iterkeys():
            setattr(recipe, name, vars[name])

        level = log.getVerbosity()
        log.setVerbosity(logLevel)
        built = self.cookObject(loader, prep=prep, macros=macros,
                                sourceVersion=sourceVersion,
                                serverIdx = serverIdx,
                                ignoreDeps = ignoreDeps,
                                logBuild = logBuild,
                                repos = repos, groupOptions=groupOptions,
                                resume = resume)
        log.setVerbosity(level)

        recipe = loader.getRecipe()

        # the rest of this is a horrible hack to allow the recipe from this
        # load to be used as a superclass later on
        del recipe.version
        recipe.internalAbstractBaseClass = True

        newD = {}
        if d:
            newD.update(d)
        newD[theName] = recipe

        return (built, newD)

    def overrideBuildFlavor(self, flavorStr):
        flavor = deps.parseFlavor(flavorStr)
        if flavor is None:
            raise RuntimeError, 'Invalid flavor %s' % flavorStr
        buildFlavor = self.cfg.buildFlavor.copy()
        if (deps.DEP_CLASS_IS in flavor.getDepClasses() and
            deps.DEP_CLASS_IS in buildFlavor.getDepClasses()):
            # instruction set deps are overridden completely -- remove
            # any buildFlavor instruction set info
            del buildFlavor.members[deps.DEP_CLASS_IS]
        buildFlavor.union(flavor,
                          mergeType = deps.DEP_MERGE_TYPE_OVERRIDE)
        self.cfg.buildFlavor = buildFlavor

    def pin(self, troveName):
        updatecmd.changePins(self.cfg, [ troveName ], True)

    def unpin(self, troveName):
        updatecmd.changePins(self.cfg, [ troveName ], False)

    def clone(self, targetBranch, *troveSpecs, **kw):
        oldQuiet = self.cfg.quiet
        if kw.pop('verbose', False):
            self.cfg.quiet = False
        else:
            self.cfg.quiet = True

        kw.setdefault('ignoreConflicts', True)
        kw.setdefault('message', 'foo')

        clone.CloneTrove(self.cfg, targetBranch, troveSpecs, **kw)
        cfgmod.quiet = oldQuiet

    def promote(self, *params, **kw):
        oldQuiet = self.cfg.quiet
        if kw.pop('verbose', False):
            self.cfg.quiet = False
        else:
            self.cfg.quiet = True

        kw.setdefault('message', 'foo')
        kw.setdefault('allFlavors', True)
        troveSpecs = []
        labelList = []
        for arg in params:
            if '--' in arg:
                labelList.append(arg.split('--', 1))
            else:
                troveSpecs.append(arg)
        cs = clone.promoteTroves(self.cfg, troveSpecs, labelList, **kw)
        cfgmod.quiet = oldQuiet
        return cs

    def initializeFlavor(self):
        use.clearFlags()
        self.cfg.useDirs = [resources.get_archive('use')]
        self.cfg.archDirs = [resources.get_archive('arch')]
        self.cfg.initializeFlavors()
        use.setBuildFlagsFromFlavor('', self.cfg.buildFlavor, error=False)
        self._origFlavor = use.allFlagsToFlavor('')
        buildIs = use.Arch.getCurrentArch()._toDependency()
        self.buildIs = { 'is' : buildIs }

    def checkConaryLog(self, logText, rootDir = None, skipSections = 0):
        if rootDir is None:
            rootDir = self.cfg.root

        # we split this into a list of lines per command
        sections = []
        sectionNum = None

        for line in open(rootDir + os.path.sep + self.cfg.logFile[0]).xreadlines():
            # strip off the timestamps and any extra whitespace
            line = line[line.index(']') + 1 :].strip()

            if sectionNum is None:
                # first line in a section
                assert(line.startswith('version'))
                # remove the version number
                line = line[line.index(':') + 2:]
                sectionNum = len(sections)
                sections.append([ line ])
            else:
                sections[sectionNum].append(line)
                if "command complete" in line:
                    sectionNum = None

        sections = sections[skipSections:]
        gotLog = "".join("\n".join(x) + "\n" for x in sections)
        assert(gotLog == logText)

    def checkUpdate(self, pkgList, expectedJob, depCheck=True,
                    keepExisting = False, recurse = True,
                    resolve = False, sync = False, replaceFiles = False,
                    exactMatch = True, apply = False, erase = False,
                    fromChangesets = [], syncChildren = False,
                    updateOnly = False, resolveGroupList=[],
                    syncUpdate = False, keepRequired = None,
                    migrate = False, removeNotByDefault = False,
                    oldMigrate = False, checkPrimaryPins = True,
                    client=None, resolveSource=None):
        """ Performs an update as given to doUpdate, and checks the resulting
            job for correctness.  If apply is True, then the job is applied
            as well.

            Parameters:
            pkgList: a list of troveSpecs to attempt to update (foo=3.3[bar])
            expectedJob: a list of changeSpecs that describe the contents
                         of the expected update job.
            apply: actually apply the given job if it passes the check.

            the rest of the parameters are as with doUpdate.

            Acceptable formats for items in the expectedJob list:

            * foo          matches foo being updated or installed
            * foo=--1.0    matches foo 1.0 is installed and nothing
                           is removed
            * foo=1.0--2.0 matches update from 1.0 to 2.0
            * foo=1.0--    matches removal of 1.0

            Flavors are accepted as well.  All portions are specified in
            trove spec format.
        """
        if client:
            assert not resolve, \
                   "Resolve cannot be specified when passing client"
            cl = client
        else:
            if resolve:
                newcfg = copy.deepcopy(self.cfg)
                newcfg.autoResolve = resolve
            else:
                newcfg = self.cfg
            cl = conaryclient.ConaryClient(newcfg)

        repos = self.openRepository()
        if oldMigrate:
            syncUpdate = removeNotByDefault = True

        installMissing = syncUpdate

        areAbsolute = not keepExisting
        if isinstance(pkgList, str):
            pkgList = [pkgList]
        if isinstance(expectedJob, str):
            expectedJob = [expectedJob]

        pkgList = list(itertools.chain(*(util.braceExpand(x) for x in pkgList)))

        applyList = cmdline.parseChangeList(pkgList, keepExisting,
                                            updateByDefault=not erase)
        updJob, suggMap = cl.updateChangeSet(applyList,
                            keepExisting = keepExisting,
                            keepRequired = keepRequired,
                            recurse = recurse, split = True, sync = sync,
                            updateByDefault = not erase,
                            fromChangesets = fromChangesets,
                            resolveDeps=depCheck, syncChildren=syncChildren,
                            updateOnly=updateOnly,
                            resolveGroupList=resolveGroupList,
                            installMissing=installMissing,
                            removeNotByDefault=removeNotByDefault,
                            migrate=migrate,
                            checkPrimaryPins=checkPrimaryPins,
                            resolveSource=resolveSource)
        if depCheck:
            assert(not suggMap or resolve)

        expectedJob = list(itertools.chain(*(util.braceExpand(x) for x in expectedJob)))
        self.checkJobList(updJob.getJobs(), expectedJob, exactMatch)

        if apply:
            cl.applyUpdate(updJob, replaceFiles = replaceFiles)
        updJob.close()

    def checkLocalUpdates(self, expectedJobs, troveNames=None,
                          exactMatch = True, getImplied=False):
        """
            check conary's understanding of the local system changes that
            have been made.
        """
        repos = self.openRepository()

        cl = conaryclient.ConaryClient(self.cfg)

        localUpdates = cl.getPrimaryLocalUpdates(troveNames)
        if getImplied:
            repos = self.openRepository()
            localUpdates += cl.getChildLocalUpdates(repos, localUpdates)

        self.checkJobList([localUpdates], expectedJobs, exactMatch)


    def checkJobList(self, actualJob, expectedJob, exactMatch):
        # check a given set of jobs against a changeList type
        # of jobs.
        erases = trovesource.SimpleTroveSource()
        installs = trovesource.SimpleTroveSource()

        jobList = []
        for job in actualJob:
            for (n, oldInfo, newInfo, isAbs) in job:
                assert(not isAbs)

                if oldInfo[0]:
                    erases.addTrove(n, *oldInfo)
                if newInfo[0]:
                    installs.addTrove(n, *newInfo)

                jobList.append((n, oldInfo, newInfo))

        changeList = cmdline.parseChangeList(expectedJob)
        for (n, oldInfo, newInfo, isAbs), jobStr in zip(changeList, expectedJob):
            if oldInfo != (None, None):
                try:
                    troveList = erases.findTrove(None, (n, oldInfo[0], oldInfo[1]))
                except errors.TroveNotFound:
                    relatedJobs = [x for x in jobList if x[0] == n]
                    if relatedJobs:
                        raise RuntimeError('failed to find erasure for %s.  Jobs involving %s found: %s' % (jobStr, n, relatedJobs))
                    else:
                        raise RuntimeError('failed to find job for %s.  Jobs: %s' % (n, jobList))
                if len(troveList) > 1:
                    raise RuntimeError, (
                        '(%s,%s,%s) matched multiple erased troves' \
                        % (n, oldInfo[0], oldInfo[1]))
                oldVer, oldFla = troveList[0][1:]
            if newInfo != (None, None) or isAbs:
                try:
                    troveList = installs.findTrove(None, (n, newInfo[0],
                                                             newInfo[1]))
                except errors.TroveNotFound:
                    raise RuntimeError('failed to find install for %s.  Jobs involving %s found: %s' % (jobStr, n, [x for x in jobList if x[0] == n]))

                if len(troveList) > 1:
                    raise RuntimeError, (
                        '(%s,%s,%s) matched multiple installed troves' \
                        % (n, newInfo[0], newInfo[1]))
                newVer, newFla = troveList[0][1:]

            if oldInfo == (None, None):
                if isAbs:
                    # we found the trove in the list of installed troves
                    # we didn't specify whether it was a new install or a
                    # install relative to anything, just assume that the
                    # old trove version is correct
                    jobToCheck = [ x for x in jobList \
                                    if x[0] == n and x[2] == (newVer, newFla) ]
                    assert(len(jobToCheck) == 1)
                    jobToCheck = jobToCheck[0]
                else:
                    # this _must_ be a fresh install.
                    jobToCheck = (n, (None, None), (newVer, newFla))
            elif newInfo == (None, None):
                # this _must_ be a fresh install.
                jobToCheck = (n, (oldVer, oldFla), (None, None))
            else:
                jobToCheck = (n, (oldVer, oldFla), (newVer, newFla))

            if jobToCheck in jobList:
                jobList.remove(jobToCheck)
            else:
                raise RuntimeError('expected job %s for job str %s was '
                                   'not included in update' %(jobToCheck,
                                                              jobStr))

        if exactMatch and jobList:
            raise RuntimeError, 'update performed extra jobs %s' % (jobList,)

    def getSearchSource(self):
        return searchsource.NetworkSearchSource(self.openRepository(),
                                                self.cfg.installLabelPath,
                                                self.cfg.flavor)

    def assertRaises(self, exceptionClass, fn, *args, **kwargs):
        text = kwargs.pop('exceptionString', None)

        try:
            fn(*args, **kwargs)
        except exceptionClass, e:
            if text is not None and str(e) != text:
                raise self.failureException(
                        "%s had str value '%s' instead of expected '%s'" %
                        (str(exceptionClass), str(e), text))
        else:
            raise self.failureException("%s not raised" % str(exceptionClass))

        return e

    def checkCall(self, testFn, args, kw, fn, expectedArgs,
                    cfgValues={}, returnVal=None, ignoreKeywords=False,
                    checkCallback=None, **expectedKw):
        methodCalled = [False]
        def _placeHolder(*args, **kw):
            """
            Pretends to be the fn that we are checking the parameters of.
            """
            methodCalled[0] = True

            if checkCallback:
                checkCallback(*args, **kw)

            self.assertEqual(len(args), len(expectedArgs))
            for i, (arg, expectedArg) in enumerate(zip(args, expectedArgs)):
                if isinstance(expectedArg, _NoneArg):
                    assert arg is None, \
                           "%s: argument %d is not None" % (arg, i + 1)
                elif expectedArg is None:
                    pass
                elif (inspect.isclass(expectedArg)
                      and isinstance(arg, expectedArg)):
                    pass
                else:
                    assert arg == expectedArg, \
                           "%s != %s" % (repr(arg), repr(expectedArg))

            for key, expectedVal in expectedKw.iteritems():
                val = kw.pop(key)
                if isinstance(expectedVal, _NoneArg):
                    assert val is None, \
                           "%s: %s s not None" % (key, repr(val))
                elif expectedVal is None:
                    pass
                elif (inspect.isclass(expectedVal)
                      and isinstance(arg, expectedVal)):
                    pass
                else:
                    assert val == expectedVal, \
                           "%s: %s != %s" % (key, repr(val), repr(expectedVal))

            if not ignoreKeywords:
                assert not kw, "%s" % repr(kw)

            if not cfgValues:
                return

            found = False
            for arg in args:
                if isinstance(arg, cfgmod.ConfigFile):
                    found = True
                    for cfgKey, cfgValue in cfgValues.iteritems():
                        assert arg[cfgKey] == cfgValue, \
                               "%s: %s != %s" % (
                                    cfgKey, repr(arg[cfgKey]), repr(cfgValue))
                    break
            assert(found)
        if fn is not None:
            fnModule, fnName = fn.rsplit('.', 1)

            if fnModule in sys.modules:
                origFn = sys.modules[fnModule].__dict__[fnName]
                sys.modules[fnModule].__dict__[fnName] = _placeHolder
                isClassMethod = False
            else:
                fnModule, className = fnModule.rsplit('.', 1)
                isClassMethod = True
                class_ = sys.modules[fnModule].__dict__[className]
                origFn = getattr(class_, fnName)

                setattr(class_,
                        fnName, new.instancemethod(_placeHolder, None,  class_))
        try:
            rv = testFn(*args, **kw)
            if fn is not None:
                assert(methodCalled[0])
            if returnVal is not None:
                assert(rv == returnVal)
            return rv
        finally:
            if fn is not None:
                if isClassMethod:
                    setattr(class_, fnName, origFn)
                else:
                    sys.modules[fnModule].__dict__[fnName] = origFn

    def checkCommand(self, testFn, cmd, fn, expectedArgs,
                     cfgValues={}, returnVal=None, ignoreKeywords=False,
                     checkCallback=None,
                     **expectedKw):
        """Runs testFn with the given command line.

           @param testFn: the function to pass our fake argv to.
           @param cmd: the command line to turn into an argv list.
                       Should start with the program name.
           @param fn: the module and function that we are checking to ensure
                  is called
           @param expectedArgs: the args that should have been passed to fn.
                  If an arg value is None, then no assertion about that
                  argument is made. If the parameter should be None, pass
                  in the rephelp.NoneArg instead.
           @param cfgValues: assert that the given cfg values are set.
                  Can only be used if one of the params passed into
                  fn is a Configuration objec.t
           @param ignoreKeywords: if True, ignore any extra keywords not
                  specified in the expectedKw argument.
           @param checkCallback: if not None, a function to call for
                  customized checks.  Called w/ the original functions'
                  keywords and args.
        """

        level = log.getVerbosity()
        argv = shlex.split(cmd)
        try:
            return self.checkCall(testFn, [argv], {}, fn, expectedArgs,
                    cfgValues=cfgValues, returnVal=returnVal,
                    ignoreKeywords=ignoreKeywords,
                    checkCallback=checkCallback, **expectedKw)
        finally:
            log.setVerbosity(level)

    def findAndGetTroves(self, *troveSpecs, **kw):
        repos = kw.pop('repos', None)
        if not repos:
            repos = self.openRepository()
        troveSpecs = [cmdline.parseTroveSpec(x) for x in troveSpecs ]
        results = repos.findTroves(self.cfg.installLabelPath,
                                   troveSpecs, self.cfg.flavor)
        troveTupList = list(itertools.chain(*results.values()))
        troves = repos.getTroves(troveTupList, **kw)
        troves = dict(itertools.izip(troveTupList, troves))

        finalResult = []
        for troveSpec in troveSpecs:
            assert(len(results[troveSpec]) == 1)
            finalResult.append(troves[results[troveSpec][0]])
        return finalResult

    def findAndGetTrove(self, troveSpec, **kw):
        return self.findAndGetTroves(troveSpec, **kw)[0]

    def setTroveVersion(self, val):
        trove.TROVE_VERSION = val
        trove.TROVE_VERSION_1_1 = val

    def loadRecipe(self, name, flavor, init=False):
        repos = self.openRepository()
        oldBuildFlavor = self.cfg.buildFlavor
        self.overrideBuildFlavor(flavor)
        use.setBuildFlagsFromFlavor(name, self.cfg.buildFlavor)
        use.Arch._getMacro('targetarch')
        loader = loadrecipe.recipeLoaderFromSourceComponent(name, self.cfg,
                                       repos,
                                       buildFlavor=self.cfg.buildFlavor)[0]
        recipeClass = loader.getRecipe()
        if init:
            return recipeClass(self.cfg, None, [])
        self.cfg.buildFlavor = oldBuildFlavor
        return recipeClass

    def getConaryProxy(self, idx = 0, proxies = None,
                       entitlements = [], users = [], useApache = False,
                       withCapsuleContentServer = False,
                       useSSL = False, cacheTimeout = None, cacheLocation = None):
        cProxyDir = os.path.join(self.tmpDir, "conary-proxy")
        if idx:
            cProxyDir += "-%s" % str(idx)
        cProxyContentsDir = os.path.join(cProxyDir, "contents")

        util.rmtree(cProxyDir, ignore_errors = True)

        util.mkdirChain(cProxyContentsDir)
        conaryPath = resources.get_path()
        server = conaryPath + '/conary/server/server.py'

        classMap = {
            (True, True): ApacheSSLProxyServer,
            (True, False) : ApacheProxyServer,
            (False, True) : StandaloneSSLProxyServer,
            (False, False) : StandaloneProxyServer,
        }
        key = (bool(useApache), bool(useSSL))
        ProxyClass = classMap[key]

        if withCapsuleContentServer:
            capsuleServerUrl = "http://localhost:%s/toplevel" % \
                self.capsuleContentServer.port
        else:
            capsuleServerUrl = None

        cProxy = ProxyClass('proxy', None,
            ContentStore(cProxyContentsDir), server, None,
            cProxyDir, conaryPath, None, None, serverIdx = idx,
            proxies = proxies, entitlements = entitlements,
            users = users, capsuleServerUrl = capsuleServerUrl,
            cacheTimeout = cacheTimeout, cacheLocation = cacheLocation)

        return cProxy

    def getHTTPProxy(self, idx = 0, path = None):
        if path:
            cProxyDir = path
        else:
            cProxyDir = os.path.join(self.tmpDir, "http-proxy")
            if idx:
                cProxyDir += "-%s" % str(idx)

        h = HTTPProxy(cProxyDir)
        if h.start():
            return h
        else:
            raise testhelp.SkipTestException("Squid is not installed")

    def failUnlessIn(self, val, target):
        self.failUnless(val in target, "%s not in %s" % (repr(val), repr(target)))

    def failUnlessSubstringIn(self, val, target):
        matches = [val in x for x in target]
        self.failUnless(matches, "%s not found in %s" % (repr(val), repr(target)))

    @staticmethod
    def sleep(length):
        'sleep at least <length> time even if signal causes short sleep'
        start = time.time()
        now = start
        while now < start + length:
            time.sleep(length - (now - start))
            now = time.time()


    @staticmethod
    def trimRecipe(recipe):
        '''
        Strip leading whitespace off a recipe so you can indent
        recipes in a test while still using multi-line string
        literals. Will remove all leading whitespace common to
        non-blank lines.
        '''

        minWhitespace = None
        pattern = re.compile('^(\s*)\S')
        for line in recipe.splitlines():
            match = pattern.match(line)
            if match:
                whitespace = len(match.group(1))
                if minWhitespace is None or whitespace < minWhitespace:
                    minWhitespace = whitespace

        if minWhitespace is None:
            return recipe

        ret = ''
        for line in recipe.splitlines():
            ret += line[minWhitespace:] + '\n'
        return ret

class HTTPProxy(base_server.BaseServer):
    """Dealing with HTTP proxies"""

    proxyBinPath = "/usr/sbin/squid"
    configTemplate = """\
http_port %(port)s
http_port %(authPort)s
cache_dir ufs %(cacheDir)s 100 4 4
access_log %(accessLog)s squid
cache_log %(cacheLog)s
cache_store_log %(storeLog)s
pid_filename %(pidFile)s

auth_param basic program %(libdir)s/squid/ncsa_auth %(passwdFile)s
auth_param basic children 5
auth_param basic realm Squid proxy-caching web server
auth_param basic credentialsttl 2 hours
auth_param basic casesensitive on

acl acl_myport_nonauth myport %(port)s
acl acl_myport_auth myport %(authPort)s

cache_effective_user %(user)s
cache_effective_group %(group)s

visible_hostname localhost.localdomain

%(acls)s
"""

    def __init__(self, topdir):
        self.topdir = topdir
        self.cacheDir = os.path.join(topdir, "cache-dir")
        self.accessLog = os.path.join(topdir, "access.log")
        self.cacheLog = os.path.join(topdir, "cache.log")
        self.storeLog = os.path.join(topdir, "store.log")
        self.pidFile = os.path.join(topdir, "squid.pid")
        self.configFile = os.path.join(topdir, "squid.conf")
        self.authPasswdFile = os.path.join(topdir, "passwd.auth")
        self.stopped = True
        self.pid = None

    def updateConfig(self, cfg, auth = False):
        port = (auth and self.authPort) or self.port
        cfg.proxy = { 'http' : 'http://localhost:%d/' % port,
                      'https' : 'https://localhost:%d/' % port,
                    }

    def start(self):
        if not os.path.exists(self.proxyBinPath):
            return None
        # We will start the proxy on these ports
        self.port, self.authPort = testhelp.findPorts(num = 2)

        self.writeConfigFile()

        stdout = open(os.devnull, "w+")
        stderr = open(os.devnull, "w+")
        # For debugging squid, uncomment the next line
        #stderr = None

        # Kill any existing proxies first
        p = subprocess.Popen(self.getStopCmd(), stdout=stderr, stderr=stderr)
        ret = p.wait()

        # Initialize cache
        p = subprocess.Popen(self.getInitCmd(), stdout=stderr, stderr=stderr)
        ret = p.wait()
        if ret != 0:
            raise Exception("Unable to init squid with config file %s: %s" %
                (self.configFile, ret))

        # Start it
        p = subprocess.Popen(self.getStartCmd(), stdout=stderr, stderr=stderr)
        ret = p.wait()
        if ret != 0:
            raise Exception("Unable to start squid with config file %s: %s" %
                (self.configFile, ret))

        # Wait till we can open a connection
        sock_utils.tryConnect("127.0.0.1", self.port)

        # Save the pid, in case the directory gets removed
        # Loop several times if squid didn't have the chance to write the pid
        # file
        for i in range(10):
            if os.path.exists(self.pidFile):
                break
            time.sleep(0.1)
        else:
            # Give it several more seconds before failing
            time.sleep(2)
        self.pid = int(open(self.pidFile).readline().strip())

        self.stopped = False
        proxyUri = "127.0.0.1:%s" % self.port
        return proxyUri

    def getStartCmd(self):
        return self.getBaseCmd()

    def getInitCmd(self):
        return self.getBaseCmd() + [ '-z' ]

    def getStopCmd(self):
        return self.getBaseCmd() + [ '-k', 'kill' ]

    def getBaseCmd(self):
        return [self.proxyBinPath, '-D', '-f', self.configFile]

    def writeConfigFile(self):
        util.mkdirChain(os.path.dirname(self.configFile))
        acls = self.getAcls()
        # Determine libdir
        from distutils import sysconfig
        libdir = sysconfig.get_config_vars()['LIBDIR']

        opts = dict(port = self.port, authPort = self.authPort,
                    cacheDir = self.cacheDir,
                    accessLog = self.accessLog, storeLog = self.storeLog,
                    cacheLog = self.cacheLog, pidFile = self.pidFile,
                    passwdFile = self.authPasswdFile,
                    acls = "\n".join(acls),
                    libdir = libdir,
                    user = os_utils.effectiveUser,
                    group = os_utils.effectiveGroup)
        open(self.configFile, "w+").write(self.configTemplate % opts)

        # Write password file too
        open(self.authPasswdFile, "w").write("rpath:IOiVc37UsPIV2\n")

    def getAcls(self):
        return [
                "acl acl_proxy_auth proxy_auth REQUIRED",
                "http_access deny acl_myport_auth !acl_proxy_auth",
                "http_access allow all"]

    def stop(self):
        if self.stopped:
            return

        stdout = open(os.devnull, "w+")
        stderr = open(os.devnull, "w+")
        # For debugging squid, uncomment the next line
        #stderr = None

        # Insist on killing squid
        for i in range(5):
            p = subprocess.Popen(self.getStopCmd(), stdout=stdout, stderr=stderr)
            ret = p.wait()
            if ret == 0:
                break
            time.sleep(.1)

        if ret != 0:
            sys.stderr.write("Unable to stop squid with config file %s: %s\n" %
                (self.configFile, ret))
            # Try harder to kill it
            sys.stderr.write("Killing squid process %d\n" % self.pid)
            try:
                os.kill(self.pid, 15)
                time.sleep(1)
                os.kill(self.pid, 9)
            except OSError, e:
                if e.errno != 3: # No such process
                    raise
            if os.path.exists(self.pidFile):
                os.unlink(self.pidFile)

        self.stopped = True

    def getFileSize(self, fname):
        st = os.stat(fname)
        if not st:
            return 0
        return st[stat.ST_SIZE]

    def getAccessLogSize(self):
        return self.getFileSize(self.accessLog)

    def getAccessLogEntry(self, start):
        # The log entry may not be flushed to disk yet
        for i in range(5):
            end = self.getAccessLogSize()
            if end != start:
                break
            time.sleep(.1)

        f = open(self.accessLog)
        f.seek(start)
        line = f.read(end - start)
        f.close()
        return line.split()

    def isStarted(self):
        return not self.stopped


class HTTPServerController(base_server.BaseServer):
    def __init__(self, requestHandler, ssl=False):
        # this is racy :-(
        self.port = testhelp.findPorts(num = 1)[0]
        self.ssl = ssl

        self.childPid = os.fork()
        if self.childPid > 0:
            sock_utils.tryConnect("127.0.0.1", self.port)
            return

        try:
            if ssl:
                klass = SecureHTTPServer
                ctx = SSL.Context("sslv23")
                if isinstance(ssl, tuple):
                    # keypair
                    sslCert, sslKey = ssl
                else:
                    # defaults
                    sslCert, sslKey = 'ssl-cert.crt', 'ssl-cert.key'
                sslCert = os.path.join(resources.get_archive(sslCert))
                sslKey = os.path.join(resources.get_archive(sslKey))
                ctx.load_cert_chain(sslCert, sslKey)
                args = (ctx,)
            else:
                klass = BaseHTTPServer.HTTPServer
                args = ()
            httpServer = klass(("127.0.0.1", self.port), requestHandler, *args)
            httpServer.serve_forever()
        finally:
            os._exit(1)

    def kill(self):
        if not self.childPid:
            self.childPid = None
            return

        if os.environ.get('COVERAGE_DIR', None):
            sendsig = signal.SIGUSR2
        else:
            sendsig = signal.SIGTERM

        start = time.time()
        while True:
            now = time.time()
            if now - start > 15:
                break

            os.kill(self.childPid, sendsig)
            try:
                pid = os.waitpid(self.childPid, os.WNOHANG)[0]
            except OSError, err:
                if err.errno == errno.EINTR:
                    # Interrupted.
                    continue
                elif err.errno == errno.ECHILD:
                    # Process doesn't exist.
                    self.childPid = None
                    return
                else:
                    raise
            else:
                if pid:
                    # Process existed but is now gone.
                    self.childPid = None
                    return
                # Process exists and is still running.
                time.sleep(2)

        # Process still not dead.
        os.kill(self.childPid, signal.SIGKILL)
        os.waitpid(self.childPid, 0)
        self.childPid = None

    stop = close = kill

    def url(self):
        if self.ssl:
            s = 's'
        else:
            s = ''
        return "http%s://localhost:%d/" % (s, self.port)

    def isStarted(self):
        return self.childPid not in (None, 0)


def _cleanUp():
    individual = _isIndividual()
    global _servers
    _servers.stopAllServers(clean=not individual)
    if _proxy:
        _proxy.stop()
        # Clean up the proxy dir too
        if not individual and hasattr(_proxy, 'reposDir'):
            util.rmtree(_proxy.reposDir, ignore_errors = True)
            # If nothing else lives in the parent for reposDir, go ahead and
            # remove that directory too
            try:
                os.rmdir(os.path.dirname(_proxy.reposDir))
            except OSError:
                pass

def getOpenFiles():
    procdir = "/proc/self/fd"
    fdlist = os.listdir(procdir)
    fdlist = ((x, os.path.join(procdir, x)) for x in fdlist)
    fdlist = set((x[0], os.readlink(x[1])) for x in fdlist
                if os.path.exists(x[1]))
    return fdlist

notCleanedUpWarning = True
