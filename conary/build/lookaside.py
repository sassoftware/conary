#
# Copyright (c) 2004-2009 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Provides a cache for storing files locally, including
downloads and unpacking layers of files.
"""
import cookielib
import errno
import os
import socket
import time
import urllib
import urllib2
import re
import copy

from conary.lib import log
from conary.lib import sha1helper
from conary.lib import util
from conary import callbacks
from conary.build.mirror import Mirror
from conary.conaryclient.callbacks import FetchCallback, ChangesetCallback


NETWORK_SCHEMES = ('http', 'https', 'ftp', 'mirror')

NEGATIVE_CACHE_TTL = 60*60 # The TTL for negative cache entries (seconds)

class laUrl(object):
    def __init__(self, urlString, parent=None, extension=None):
        urlString = urllib.unquote(urlString)

        # unfortunately urllib doesn't support unknown schemes so we have them
        # parsed as http
        for x in ('mirror','multiurl','lookaside'):
            x += '://'
            if urlString.startswith(x):
                savedScheme = x
                urlString = urlString.replace(x,'http://',1)
                break
        else:
            savedScheme = None

        (self.scheme, self.user, self.passwd, self.host, self.port,
         self.path, self.params, self.fragment ) = util.urlSplit(urlString)

        if savedScheme:
            self.scheme = savedScheme[:-3]

        self.parent=parent
        assert self.parent is not self
        self.extension=extension

    def asStr(self,noAuth=False,quoted=False):
        if self.parent:
            suffix = self.parent._getCumulativePath()
            path = os.path.normpath(os.sep.join((self.path, suffix)))
        else:
            path = self.path

        if self.extension:
            path += '.' + self.extension

        if quoted:
            path = urllib.quote(path)

        if noAuth:
            return util.urlUnsplit( (self.scheme, None, None,
                                     self.host, self.port, path,
                                     self.params, self.fragment) )
        return util.urlUnsplit( (self.scheme, self.user, self.passwd,
                                 self.host, self.port, path,
                                 self.params, self.fragment) )
    def __str__(self):
        return self.asStr()

    def filePath(self,useParentPath=True):
        suffix = None
        if self.parent and useParentPath:
            suffix = self.parent._getCumulativePath()
        if suffix:
            path = os.path.normpath(os.sep.join((self.path, suffix)))
        else:
            path = self.path
        if self.extension:
            path += '.' + self.extension

        if path[0] == '/':
            return os.path.join('/',self.host,path[1:])
        elif self.host:
            return os.path.join('/',self.host,path)

        return path

    def explicit(self):
        return self.scheme not in [ 'mirror', 'multiurl']

    def _getCumulativePath(self):
        ppath=None
        if self.parent:
            ppath = self.parent._getCumulativePath()

        if self.path:
            if ppath:
                return os.path.normpath(os.sep.join((self.path,ppath)))
            else:
                return self.path
        return ppath

def checkRefreshFilter( refreshFilter, url):
    if not refreshFilter:
        return False
    if refreshFilter( str(url) ):
        return True
    if refreshFilter( os.path.basename(url.path) ):
        return True
    return False

# some recipes reach into Conary internals here, and have references
# to searchAll
def searchAll(cfg, repCache, name, location, srcdirs, autoSource=False,
              httpHeaders={}, localOnly=False):
    return findAll(cfg, repCache, name, location, srcdirs, autoSource,
                   httpHeaders, localOnly, allowNone=True)



# bw compatible findAll method.
def findAll(cfg, repCache, name, location, srcdirs, autoSource=False,
            httpHeaders={}, localOnly=False, guessName=None, suffixes=None,
            allowNone=False, refreshFilter=None, multiurlMap=None,
            unifiedSourcePath = None):
    if guessName:
        name = name + guessName
    ff = FileFinder(recipeName=location, repositoryCache=repCache,
                          localDirs=srcdirs, multiurlMap=multiurlMap,
                          mirrorDirs=cfg.mirrorDirs,
                          cfg=cfg)

    if localOnly:
        if srcdirs:
            searchMethod = ff.SEARCH_LOCAL_ONLY
        else:
            # BW COMPATIBLE HACK - since we know we aren't actually searching
            # srcdirs since they're empty, we take this to mean 
            # repository only.
            searchMethod = ff.SEARCH_REPOSITORY_ONLY
    elif autoSource:
        searchMethod = ff.SEARCH_REPOSITORY_ONLY
    else:
        searchMethod = ff.SEARCH_ALL


    results = ff.fetch(name, suffixes=suffixes, archivePath=unifiedSourcePath,
                       allowNone=allowNone, searchMethod=searchMethod,
                       headers=httpHeaders, refreshFilter=refreshFilter)
    return results[1]

# backwards compatible fetchURL method
def fetchURL(cfg, name, location, httpHeaders={}, guessName=None, mirror=None):
    repCache = RepositoryCache(None, cfg=cfg)
    ff = FileFinder(recipeName=location, repositoryCache=repCache,
                    cfg=cfg)
    try:
        url = laUrl(name)
        return ff.searchNetworkSources(url, headers=httpHeaders)
    except PathFound, pathInfo:
        return pathInfo.path


class FileFinder(object):

    SEARCH_ALL = 0
    SEARCH_REPOSITORY_ONLY = 1
    SEARCH_LOCAL_ONLY = 2

    def __init__(self, recipeName, repositoryCache, localDirs=None,
                 multiurlMap=None, refreshFilter=None, mirrorDirs = None,
                 cfg=None):
        self.cfg = cfg
        self.recipeName = recipeName
        self.repCache = repositoryCache
        if self.repCache:
            self.repCache.setConfig(cfg)
        if localDirs is None:
            localDirs = []
        self.localDirs = localDirs
        self.multiurlMap = multiurlMap
        self.mirrorDirs = mirrorDirs
        self.noproxyFilter = util.noproxyFilter()

    def fetch(self, urlStr, suffixes=None, archivePath=None, headers=None,
              allowNone=False, searchMethod=0, # SEARCH_ALL
              refreshFilter=None):
        urlList = self._getPathsToSearch(urlStr, suffixes)
        for url in urlList:
            try:
                self._fetch(url,
                            archivePath, headers=headers,
                            refreshFilter=refreshFilter,
                            searchMethod=searchMethod)
            except PathFound, pathInfo:
                return pathInfo.isFromRepos, pathInfo.path

        # we didn't find any matching url.
        if not allowNone:
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT),
                          urlStr)
        return None, None

    def _fetch(self, url, archivePath, searchMethod, headers=None,
               refreshFilter=None):
        if isinstance(url,str):
            url = laUrl(url)

        refresh = checkRefreshFilter(refreshFilter,url)
        if searchMethod == self.SEARCH_LOCAL_ONLY:
            self.searchFilesystem(url)
            return
        elif searchMethod == self.SEARCH_REPOSITORY_ONLY:
            if archivePath:
                self.searchArchive(archivePath, url)
            elif refresh:
                self.searchNetworkSources(url, headers)
            self.searchRepository(url)
        else: #SEARCH_ALL
            self.searchFilesystem(url)
            if archivePath:
                self.searchArchive(archivePath, url)
            elif refresh:
                self.searchNetworkSources(url, headers)
            self.searchRepository(url)
            self.searchLocalCache(url)
            self.searchNetworkSources(url,headers)

    def searchRepository(self, url):
        if self.repCache.hasFilePath(url):
            log.info('found %s in repository', url.asStr(noAuth=True) )
            path = self.repCache.cacheFilePath(self.recipeName, url)
            raise PathFound(path, True)

    def searchLocalCache(self,  url):
        # exact match first, then look for cached responses from other servers
        path = self.repCache.getCacheEntry(self.recipeName, url)
        if path: raise PathFound(path, False)

    def searchFilesystem(self, url):
        if url.filePath() == '/':
            return
        path = util.searchFile(url.filePath(), self.localDirs)

        if path:
            raise PathFound(path, False)

    def searchArchive(self, archiveName, url):
        path =  self.repCache.getArchiveCacheEntry(archiveName, url)
        if path:
            raise PathFound(path, True)

    def searchNetworkSources(self, url, headers):
        if url.scheme not in NETWORK_SCHEMES:
            return

        # check for negative cache entries to avoid spamming servers
        negativePath =  self.repCache.checkNegativeCache(self.recipeName, url)
        if negativePath:
            log.warning('not fetching %s (negative cache entry %s exists)',
                        url, negativePath)
            return

        log.info('Trying %s...', str(url))
        if headers is None:
            headers = {}

        inFile = self._fetchUrl(url, headers)
        if inFile is None:
            self.repCache.createNegativeCacheEntry(self.recipeName, url)
        else:
            contentLength = int(inFile.headers.get('Content-Length', 0))
            path = self.repCache.addFileToCache(self.recipeName, url,
                                                inFile, contentLength)
            if path:
                raise PathFound(path, False)
        return

    def _getPathsToSearch(self, urlStr, suffixes):
        url = laUrl(urlStr)

        if url.scheme == 'multiurl':
            multiKey = os.path.dirname(url.filePath())[1:]
            urlObjList = [ laUrl(x,parent=url)
                           for x in self.multiurlMap[multiKey] ]
        else:
            urlObjList = [url]

        newUrlObjList = []
        for ou in urlObjList:
            if ou.scheme == 'mirror':
                for u in Mirror(self.mirrorDirs, ou.host):
                    mu = laUrl(u,parent=ou)
                    newUrlObjList.append(mu)
            else:
                newUrlObjList.append(ou)
        urlObjList = newUrlObjList

        if suffixes is not None:
            newUrlObjList = []
            for url in urlObjList:
                for suffix in suffixes:
                    newurl = copy.copy(url)
                    newurl.extension=suffix
                    newUrlObjList.append(newurl)
            urlObjList = newUrlObjList
        return urlObjList

    class BasicPasswordManager(object):
        # password manager class for urllib2 that handles exactly 1 password
        def __init__(self):
            self.user = ''
            self.passwd = ''

        def add_password(self, user, passwd):
            self.user = user
            self.passwd = passwd

        def find_user_password(self, *args, **kw):
            return self.user, self.passwd

    def _fetchUrl(self, url, headers):
        if isinstance(url,str):
            url = laUrl(url)

        retries = 0
        inFile = None
        while retries < 5:
            try:
                # set up a handler that tracks cookies to handle
                # sites like Colabnet that want to set a session cookie
                cj = cookielib.LWPCookieJar()
                passwdMgr = self.BasicPasswordManager()
                if self.cfg.proxy and \
                        not self.noproxyFilter.bypassProxy(url.host):
                    proxyPasswdMgr = urllib2.HTTPPasswordMgr()
                    opener = urllib2.build_opener(
                        urllib2.HTTPCookieProcessor(cj),
                        urllib2.HTTPBasicAuthHandler(passwdMgr),
                        urllib2.ProxyBasicAuthHandler(proxyPasswdMgr),
                        urllib2.ProxyHandler(self.cfg.proxy)
                        )
                else:
                    proxyPasswdMgr = None
                    opener = urllib2.build_opener(
                        urllib2.HTTPCookieProcessor(cj),
                        urllib2.HTTPBasicAuthHandler(passwdMgr),
                        )

                urlStr = url.asStr(noAuth=True,quoted=True)
                if url.user:
                    url.passwd = url.passwd or ''
                    passwdMgr.add_password(url.user, url.passwd)

                if proxyPasswdMgr:
                    for v in self.cfg.proxy.values():
                        pUrl = laUrl(v[1])
                        if pUrl.user:
                            pUrl.passwd = pUrl.passwd or ''
                            proxyPasswdMgr.add_password(
                                None, pUrl.asStr(noAuth=True,quoted=True),
                                url.user, url.passwd)
                req = urllib2.Request(urlStr, headers=headers)
                inFile = opener.open(req)
                if not urlStr.startswith('ftp://'):
                    content_type = inFile.info()['content-type']
                    if not url.explicit() and 'text/html' in content_type:
                        raise urllib2.URLError('"%s" not found' % urlStr)
                log.info('Downloading %s...', urlStr)
                break
            except urllib2.HTTPError, msg:
                if msg.code == 404:
                    return None
                else:
                    log.error('error downloading %s: %s',
                              urlStr, str(msg))
                    return None
            except urllib2.URLError:
                return None
            except socket.error, err:
                num, msg = err
                if num == errno.ECONNRESET:
                    log.info('Connection Reset by FTP server'
                             'while retrieving %s.'
                             '  Retrying in 10 seconds.', urlStr, msg)
                    time.sleep(10)
                    retries += 1
                else:
                    return None
            except IOError, msg:
                # only retry for server busy.
                ftp_error = msg.args[1]
                if isinstance(ftp_error, EOFError):
                    # server just hung and gave no response
                    return None
                response = msg.args[1].args[0]
                if isinstance(response, str) and response.startswith('421'):
                    log.info('FTP server busy when retrieving %s.'
                             '  Retrying in 10 seconds.', urlStr)
                    time.sleep(10)
                    retries += 1
                else:
                    return None
        return inFile

class RepositoryCache(object):

    def __init__(self, repos, refreshFilter=None, cfg=None):
	self.repos = repos
        self.refreshFilter = refreshFilter
	self.nameMap = {}
        self.cacheMap = {}
        self.quiet = False
        self._basePath = self.downloadRatedLimit = None
        self.setConfig(cfg)

    def setConfig(self, cfg):
        if cfg:
            self.quiet = cfg.quiet
            self._basePath = cfg.lookaside
            self.downloadRateLimit = cfg.downloadRateLimit

    def _getBasePath(self):
        if self._basePath is None:
            raise RuntimeError('Tried to use repository cache with unset'
                               ' basePath')
        return self._basePath

    basePath = property(_getBasePath)

    def addFileHash(self, troveName, troveVersion, pathId, path, fileId,
                    fileVersion, sha1, mode):
	self.nameMap[path] = (troveName, troveVersion, pathId, path, fileId,
                              fileVersion, sha1, mode)

    def hasFilePath(self, url):
        if self.refreshFilter:
            if checkRefreshFilter(self.refreshFilter,url):
                return False
        return url.filePath() in self.nameMap

    def cacheFilePath(self, cachePrefix, url):
	cachePath = self.getCachePath(cachePrefix, url)
        util.mkdirChain(os.path.dirname(cachePath))

        if url.filePath() in self.cacheMap:
            # don't check sha1 twice
            return self.cacheMap[url.filePath()]
	(troveName, troveVersion, pathId, troveFile, fileId,
         troveFileVersion, sha1, mode) = self.nameMap[url.filePath()]
        sha1Cached = None
        cachedMode = None
	if os.path.exists(cachePath):
            sha1Cached = sha1helper.sha1FileBin(cachePath)
        if sha1Cached != sha1:
            if sha1Cached:
                log.info('%s sha1 %s != %s; fetching new...', url.filePath(),
                          sha1helper.sha1ToString(sha1),
                          sha1helper.sha1ToString(sha1Cached))
            else:
                log.info('%s not yet cached, fetching...', url.filePath())

            if self.quiet:
                csCallback = None
            else:
                csCallback = ChangesetCallback()

            f = self.repos.getFileContents(
                [ (fileId, troveFileVersion) ], callback = csCallback)[0].get()
            util.copyfileobj(f, open(cachePath, "w"))
            fileObj = self.repos.getFileVersion(
                pathId, fileId, troveFileVersion)
            fileObj.chmod(cachePath)

        cachedMode = os.stat(cachePath).st_mode & 0777
        if mode != cachedMode:
            os.chmod(cachePath, mode)
        self.cacheMap[url.filePath()] = cachePath
	return cachePath

    def addFileToCache(self, cachePrefix, url, infile, contentLength):
        # cache needs to be hierarchical to avoid collisions, thus we
        # use cachePrefix so that files with the same name and different
        # contents in different packages do not collide
        cachedname = self.getCachePath(cachePrefix, url)
        util.mkdirChain(os.path.dirname(cachedname))
        f = open(cachedname, "w+")

        try:
            BLOCKSIZE = 1024 * 4

            if self.quiet:
                callback = callbacks.FetchCallback()
            else:
                callback = FetchCallback()

            wrapper = callbacks.CallbackRateWrapper(callback, callback.fetch,
                                                    contentLength)
            total = util.copyfileobj(infile, f, bufSize=BLOCKSIZE,
                                     rateLimit = self.downloadRateLimit,
                                     callback = wrapper.callback)

            f.close()
            infile.close()
        except:
            os.unlink(cachedname)
            raise

        # work around FTP bug (msw had a better way?)
        if url.scheme == 'ftp':
            if os.stat(cachedname).st_size == 0:
                os.unlink(cachedname)
                self.createNegativeCacheEntry(cachePrefix, url)
                return None

        return cachedname

    def setRefreshFilter(self, refreshFilter):
        self.refreshFilter = refreshFilter

    def getCacheDir(self, cachePrefix, negative=False):
        if negative:
            cachePrefix = 'NEGATIVE' + os.sep + cachePrefix
        return os.sep.join((self.basePath, cachePrefix))

    def getCachePath(self, cachePrefix, url, negative=False):
        if isinstance(url,str):
            url=laUrl(url)
        cacheDir = self.getCacheDir(cachePrefix, negative=negative)
        cachePath = os.sep.join((cacheDir, url.filePath(not negative)))
        return os.path.normpath(cachePath)

    def clearCacheDir(self, cachePrefix, negative=False):
        negativeCachePath = self.getCacheDir(cachePrefix, negative = negative)
        util.rmtree(os.path.dirname(negativeCachePath), ignore_errors = True)

    def createNegativeCacheEntry(self, cachePrefix, url):
        if isinstance(url,str):
            url=laUrl(url)
        cachePath = self.getCachePath(cachePrefix, url, negative=True)
        util.mkdirChain(os.path.dirname(cachePath))
        open(cachePath, 'w+')

    def findInCache(self, cachePrefix, basename):
        return util.searchPath(basename,
                               os.path.join(self.basePath, cachePrefix))

    def getCacheEntry(self, cachePrefix, url):
        cachePath = self.getCachePath(cachePrefix, url)
        if os.path.exists(cachePath):
            return cachePath

    def checkNegativeCache(self, cachePrefix, url):
        cachePath = self.getCachePath(cachePrefix, url, negative=True)
        if os.path.exists(cachePath):
            if time.time() < (NEGATIVE_CACHE_TTL
                              + os.path.getmtime(cachePath)):
                return cachePath
            else:
                os.remove(cachePath)
        return False

    def getArchiveCachePath(self, archiveName, url=''):
        # CNY-2627 introduced a separate lookaside stack for archive contents
        # this dir tree is parallel to NEGATIVE and trovenames.
        # the name =X_CONTENTS= was chosen because = is an illegal character
        # in a trovename and thus will never conflict with real troves.
        archiveType, trailingPath = archiveName.split('://', 1)
        contentsPrefix = "=%s_CONTENTS=" % archiveType.upper()
        return self.getCachePath(contentsPrefix,
                                 os.path.join(trailingPath, url.filePath()))


    def getArchiveCacheEntry(self, archiveName, url):
        fullPath = self.getArchiveCachePath(archiveName, url)
        if os.path.exists(fullPath):
            return fullPath


class PathFound(Exception):
    def __init__(self, path, isFromRepos):
        self.path = path
        self.isFromRepos = isFromRepos
