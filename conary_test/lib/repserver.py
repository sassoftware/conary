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

import errno
import os
import sys
from conary.lib import util
from testutils.servers.gunicorn_server import GunicornServer
from testutils.servers.uwsgi_server import UwsgiServer
from testutils.servers.nginx_server import NginxServer

from conary_test import resources


class ConaryServer(object):

    cache = None
    reposDB = None

    appServerClasses = {
            'gunicorn': GunicornServer,
            'uwsgi': UwsgiServer,
            }

    def __init__(self, reposDir,
            sslCertAndKey=None,
            withCache=True,
            singleWorker=False,
            configValues=(),
            ):
        self.reposDir = os.path.abspath(reposDir)
        self.reposLog = os.path.join(self.reposDir, 'repos.log')
        self.traceLog = os.path.join(self.reposDir, 'trace.log')
        self.configPath = os.path.join(self.reposDir, 'repository.cnr')
        self.contents = ContentStore(os.path.join(self.reposDir, 'contents'))
        if withCache:
            self.cache = ContentStore(os.path.join(self.reposDir, 'cscache'))
            self.cache.reset()
        self.sslCertAndKey = sslCertAndKey
        if sslCertAndKey is True:
            self.sslCertAndKey = (
                    resources.get_archive('ssl-cert.crt'),
                    resources.get_archive('ssl-cert.key'))
        name = os.environ.get('CONARY_APP_SERVER', 'gunicorn')
        appServerClass = self.appServerClasses[name]
        self.appServer = appServerClass(
                serverDir=os.path.join(self.reposDir, 'appserver'),
                workers=(1 if singleWorker else 2),
                application='conary.server.wsgi_hooks',
                environ={
                    'CONARY_SERVER_CONFIG': self.configPath,
                    },
                )
        self.rpServer = NginxServer(
                serverDir=os.path.join(self.reposDir, 'revproxy'),
                proxyTo=self.appServer.getProxyTo(),
                sslCertAndKey=self.sslCertAndKey,
                )
        self.configValues = configValues
        self.needsReset = True

    def reset(self):
        self.checkForTracebacks()
        for thing in (
                self.appServer,
                self.rpServer,
                self.contents,
                self.reposDB,
                self.cache,
                ):
            if thing:
                thing.reset()
        self.needsReset = False

    def resetIfNeeded(self):
        if self.needsReset:
            self.reset()

    def setNeedsReset(self):
        self.needsReset = True

    def start(self):
        self.needsReset = True
        self.createConfig()
        self.appServer.start()
        self.rpServer.start()

    def stop(self):
        self.checkForTracebacks()
        self.rpServer.stop()
        self.appServer.stop()

    def checkForTracebacks(self):
        try:
            with open(self.appServer.errorLog) as f:
                data = f.read()
            if 'Traceback (most recent call last)' in data:
                print >> sys.stderr, "Contents of error.log after test:"
                print >> sys.stderr, data
                sys.stderr.flush()
        except IOError, err:
            if err.args[0] != errno.ENOENT:
                raise

    def createConfig(self, defaultValues=()):
        configValues = {
            'baseUri'               : '/conary',
            'traceLog'              : '3 ' + self.traceLog,
            'logFile'               : self.reposLog,
            'tmpDir'                : self.reposDir,

        }
        if self.cache:
            configValues['changesetCacheDir'] = self.cache.getPath()
        configValues.update(defaultValues)
        configValues.update(self.configValues)

        util.mkdirChain(os.path.dirname(self.configPath))
        with open(self.configPath, 'w') as f:
            for key, values in configValues.iteritems():
                if values is None:
                    continue
                if not isinstance(values, list):
                    values = [values]
                for value in values:
                    print >> f, key, value

    def getUrl(self, ssl=True):
        return self.rpServer.getUrl(ssl=ssl) + '/conary/'


class RepositoryServer(ConaryServer):

    def __init__(self, reposDir, nameList, reposDB, **kwargs):
        ConaryServer.__init__(self, reposDir, **kwargs)
        if isinstance(nameList, str):
            nameList = [nameList]
        self.nameList = nameList
        self.reposDB = reposDB
        self.needsPGPKey = True

    def getMap(self, ssl=True):
        dest = self.getUrl(ssl=ssl)
        d = dict((name, dest) for name in self.nameList)
        return d

    def getName(self):
        # assume the first entry in a multihomed name list is the "main" name
        return self.nameList[0]

    def reset(self):
        super(RepositoryServer, self).reset()
        self.needsPGPKey = True

    def clearNeedsPGPKey(self):
        self.needsPGPKey = False

    def createConfig(self, defaultValues=()):
        configValues = {
            'contentsDir'           : self.contents.path,
            'repositoryDB'          : self.reposDB.getDriver(),
            'serverName'            : " ".join(self.nameList),
            }
        configValues.update(defaultValues)
        super(RepositoryServer, self).createConfig(defaultValues=configValues)


class ProxyServer(ConaryServer):

    def __init__(self, reposDir, **kwargs):
        kwargs.setdefault('withCache', True)
        ConaryServer.__init__(self, reposDir, **kwargs)

    def createConfig(self, defaultValues=()):
        configValues = {
            'proxyContentsDir'      : self.contents.path,
            }
        configValues.update(defaultValues)
        super(ProxyServer, self).createConfig(defaultValues=configValues)

    def addToConfig(self, configObj):
        configObj.configLine('conaryProxy http ' +
                self.rpServer.getUrl(ssl=False))
        if self.sslCertAndKey:
            configObj.configLine('conaryProxy https ' +
                    self.rpServer.getUrl(ssl=True))
        else:
            configObj.configLine('conaryProxy https ' +
                    self.rpServer.getUrl(ssl=False))


class ContentStore(object):

    def __init__(self, path):
        self.path = os.path.abspath(path)

    def reset(self):
        util.rmtree(self.path, ignore_errors=True)
        util.mkdirChain(self.path)

    def getPath(self):
        return self.path
