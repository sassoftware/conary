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
Provides a cache for storing files locally, including
downloads and unpacking layers of files.
"""

import errno
from conary.lib import log
from conary.lib import sha1helper
from conary.lib import util
from conary import callbacks
import os
import socket
import time
import urllib2

# location is normally the package name
networkPrefixes = ('http://', 'https://', 'ftp://')

class FetchCallback(callbacks.LineOutput, callbacks.FetchCallback):
    def fetch(self, got, need):
        if need == 0:
            self._message("Downloading source (%dKb at %dKb/sec)..." \
                          % (got/1024, self.rate/1024))
        else:
            self._message("Downloading source (%dKb (%d%%) of %dKb at %dKb/sec)..." \
                          % (got/1024, (got*100)/need , need/1024, self.rate/1024))

    def __init__(self, *args, **kw):
        callbacks.LineOutput.__init__(self, *args, **kw)
        callbacks.FetchCallback.__init__(self, *args, **kw)

class ChangesetCallback(callbacks.LineOutput, callbacks.ChangesetCallback):

    def preparingChangeSet(self):
        self.updateMsg("Preparing changeset request")

    def requestingFileContents(self):
        self._message("Requesting file...")

    def downloadingFileContents(self, got, need):
        if need == 0:
            self._message("Downloading file (%dKb at %dKb/sec)..." \
                          % (got/1024, self.rate/1024))
        else:
            self._message("Downloading file (%dKb (%d%%) of %dKb at %dKb/sec)..." \
                          % (got/1024, (got*100)/need , need/1024, self.rate/1024))
    def downloadingChangeSet(self, got, need):
        self._downloading('Downloading', got, self.rate, need)

    def _downloading(self, msg, got, rate, need):
        if got == need:
            self.csText = None
        elif need != 0:
            if self.csHunk[1] < 2 or not self.updateText:
                self.csMsg("%s %dKb (%d%%) of %dKb at %dKb/sec"
                           % (msg, got/1024, (got*100)/need, need/1024, rate/1024))
            else:
                self.csMsg("%s %d of %d: %dKb (%d%%) of %dKb at %dKb/sec"
                           % ((msg,) + self.csHunk + \
                              (got/1024, (got*100)/need, need/1024, rate/1024)))
        else: # no idea how much we need, just keep on counting...
            self.csMsg("%s (got %dKb at %dKb/s so far)" % (msg, got/1024, rate/1024))

        self.update()

    def csMsg(self, text):
        self.csText = text
        self.update()

    def sendingChangeset(self, got, need):
        if need != 0:
            self._message("Committing changeset "
                          "(%dKb (%d%%) of %dKb at %dKb/sec)..."
                          % (got/1024, (got*100)/need, need/1024, self.rate/1024))
        else:
            self._message("Committing changeset "
                          "(%dKb at %dKb/sec)..." % (got/1024, self.rate/1024))


    def update(self):
        t = self.csText
        if t:
            self._message(t)
        else:
            self._message('')

    def done(self):
        self._message('')

    def _message(self, txt, usePrefix=True):
        if txt and usePrefix:
            return callbacks.LineOutput._message(self, self.prefix + txt)
        else:
            return callbacks.LineOutput._message(self, txt)

    def setPrefix(self, txt):
        self.prefix = txt

    def clearPrefix(self):
        self.prefix = ''

    def __init__(self, *args, **kw):
        self.csHunk = (0, 0)
        self.csText = None
        self.prefix = ''
        callbacks.LineOutput.__init__(self, *args, **kw)
        callbacks.ChangesetCallback.__init__(self, *args, **kw)

def _truncateName(name):
    for prefix in networkPrefixes:
        if name.startswith(prefix):
            return name[len(prefix)-2:]
    return name

def createCacheName(cfg, name, location, negative=''):
    name = _truncateName(name)
    cachedname = os.sep.join((cfg.lookaside, negative + location, name))
    util.mkdirChain(os.path.dirname(cachedname))
    return cachedname

def _createCacheEntry(cfg, name, location, infile):
    # cache needs to be hierarchical to avoid collisions, thus we
    # use location so that files with the same name and different
    # contents in different packages do not collide
    cachedname = createCacheName(cfg, name, location)
    f = open(cachedname, "w+")

    try:
        BLOCKSIZE = 1024 * 4

        got = 0
        if infile.info().has_key('content-length'):
            need = int(infile.info()['content-length'])
        else:
            need = 0

        if cfg.quiet:
            callback = callbacks.FetchCallback()
        else:
            callback = FetchCallback()

        wrapper = callbacks.CallbackRateWrapper(callback, callback.fetch,
                                                need)
        total = util.copyfileobj(infile, f, bufSize=BLOCKSIZE,
                                 rateLimit = cfg.downloadRateLimit,
                                 callback = wrapper.callback)

        f.close()
        infile.close()
    except:
        os.unlink(cachedname)
        raise

    # work around FTP bug (msw had a better way?)
    if name.startswith("ftp://"):
	if os.stat(cachedname).st_size == 0:
	    os.unlink(cachedname)
	    _createNegativeCacheEntry(cfg, name[5:], location)
	    return None

    return cachedname

def _createNegativeCacheName(cfg, name, location):
    name = _truncateName(name)
    negativeEntry = createCacheName(cfg, name, location, 'NEGATIVE' + os.sep)
    return negativeEntry

def _createNegativeCacheEntry(cfg, name, location):
    negativeEntry = _createNegativeCacheName(cfg, name, location)
    open(negativeEntry, "w+").close()

def _searchCache(cfg, name, location):
    basename = os.path.basename(name)

    networkSource = False
    for prefix in networkPrefixes:
        if name.startswith(prefix):
            networkSource = True
            break

    if networkSource:
        # check for negative cache entries to avoid spamming servers
        negativeName = _createNegativeCacheName(cfg, name, location)
        if os.path.exists(negativeName):
            if time.time() > 60*60 + os.path.getmtime(negativeName):
                os.remove(negativeName)
            else:
                log.warning('found %s, therefore not fetching %s',
                    negativeName, name)
                return -1

        # exact match first, then look for cached responses from other servers
        positiveName = createCacheName(cfg, name, location)
        if os.path.exists(positiveName):
            return positiveName
        return util.searchPath(basename, os.sep.join((cfg.lookaside,
                                                      location, basename)))
    else:
        return util.searchFile(basename,
                               [os.sep.join((cfg.lookaside, location))])


def _searchRepository(cfg, repCache, name, location):
    """searches repository, and retrieves to cache"""
    if repCache.hasFileName(name):
	log.info('found %s in repository', name)
	return repCache.cacheFile(cfg, name, location, name)
    basename = os.path.basename(name)
    if repCache.hasFileName(basename):
	log.info('found %s in repository', name)
	return repCache.cacheFile(cfg, basename, location, basename)


    return None


def fetchURL(cfg, name, location, httpHeaders={}):
    log.info('Downloading %s...', name)
    retries = 0
    url = None
    while retries < 5:
        try:
            req = urllib2.Request(name, headers=httpHeaders)
            url = urllib2.urlopen(req)
            break
        except urllib2.HTTPError, msg:
            if msg.code == 404:
                _createNegativeCacheEntry(cfg, name, location)
                return None
            else:
                log.error('error downloading %s: %s',
                          name, str(msg))
                return None
        except urllib2.URLError:
            _createNegativeCacheEntry(cfg, name, location)
            return None
        except socket.error, err:
            num, msg = err
            if num == errno.ECONNRESET:
                log.info('Connection Reset by FTP server'
                         'while retrieving %s.'
                         '  Retrying in 10 seconds.', name, msg)
                time.sleep(10)
                retries += 1
            else:
                _createNegativeCacheEntry(cfg, name, location)
                return None
        except IOError, msg:
            # only retry for server busy.
            ftp_error = msg.args[1]
            if isinstance(ftp_error, EOFError):
                # server just hung and gave no response
                _createNegativeCacheEntry(cfg, name, location)
                return None
                
            response = msg.args[1].args[0]
            if isinstance(response, str) and response.startswith('421'):
                log.info('FTP server busy when retrieving %s.'
                         '  Retrying in 10 seconds.', name)
                time.sleep(10)
                retries += 1
            else:
                _createNegativeCacheEntry(cfg, name, location)
                return None
    if url is None:
        return None

    rc = _createCacheEntry(cfg, name, location, url)
    return rc

def searchAll(cfg, repCache, name, location, srcdirs, autoSource=False, 
              localOnly=False, httpHeaders={}):
    """
    searches all locations, including populating the cache if the
    file can't be found in srcdirs, and returns the name of the file.
    autoSource should be True when the file has been pulled from an RPM,
    and so has no path associated but is still auto-added
    """
    if name[0] != '/' and not autoSource:
        # these are files that do not have / in the name and are not
        # indirectly fetched via RPMs, so we look in the local directory
        f = util.searchFile(name, srcdirs)
        if f: return f

    if localOnly:
        return None

    # this needs to come as soon as possible to preserve reproducability
    f = _searchRepository(cfg, repCache, name, location)
    if f: return f

    # OK, now look in the lookaside cache
    # this is for sources that will later be auto-added
    # one way or another
    f = _searchCache(cfg, name, location)
    if f and f != -1: return f

    # negative cache entry
    if f == -1:
        return None

    # Need to fetch a file that will be auto-added to the repository
    # on commit
    for prefix in networkPrefixes:
        if name.startswith(prefix):
            return fetchURL(cfg, name, location, httpHeaders)

    # could not find it anywhere
    return None


def findAll(cfg, repcache, name, location, srcdirs, autoSource=False, httpHeaders={}):
    f = searchAll(cfg, repcache, name, location, srcdirs, autoSource, httpHeaders=httpHeaders)
    if not f:
	raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT), name)
    return f


class RepositoryCache:

    def addFileHash(self, troveName, troveVersion, pathId, path, fileId,
                    fileVersion, sha1):
	self.nameMap[path] = (troveName, troveVersion, pathId, path, fileId,
                              fileVersion, sha1)

    def hasFileName(self, fileName):
	return fileName in self.nameMap

    def cacheFile(self, cfg, fileName, location, basename):
	cachedname = createCacheName(cfg, fileName, location)

        if basename in self.cacheMap:
            # don't check sha1 twice
            return self.cacheMap[basename]
	(troveName, troveVersion, pathId, troveFile, fileId,
                    troveFileVersion, sha1) = self.nameMap[basename]
        sha1Cached = None
	if os.path.exists(cachedname):
            sha1Cached = sha1helper.sha1FileBin(cachedname)
        if sha1Cached != sha1:
            if sha1Cached:
                log.info('%s sha1 %s != %s; fetching new...', basename,
                          sha1helper.sha1ToString(sha1),
                          sha1helper.sha1ToString(sha1Cached))
            else:
                log.info('%s not yet cached, fetching...', basename)

            if cfg.quiet:
                csCallback = None
            else:
                csCallback = ChangesetCallback()

            f = self.repos.getFileContents(
                [ (fileId, troveFileVersion) ], callback = csCallback)[0].get()
            util.copyfileobj(f, open(cachedname, "w"))
            fileObj = self.repos.getFileVersion(
                pathId, fileId, troveFileVersion)
            fileObj.chmod(cachedname)
        self.cacheMap[basename] = cachedname
	return cachedname

    def __init__(self, repos):
	self.repos = repos
	self.nameMap = {}
        self.cacheMap = {}
