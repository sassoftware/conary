#
# Copyright (c) 2004-2006 rPath, Inc.
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
# fully featured callbacks...
from conary.conaryclient.callbacks import FetchCallback, ChangesetCallback
from conary.build.mirror import Mirror
import os
import socket
import time
import urllib2

# location is normally the package name
networkPrefixes = ('http://', 'https://', 'ftp://', 'mirror://')

def _truncateName(name):
    for prefix in networkPrefixes:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name

def createCacheName(cfg, name, location, negative=''):
    name = _truncateName(name)
    cachedname = os.sep.join((cfg.lookaside, negative + location, name))
    return cachedname

def _createCacheEntry(cfg, name, location, infile):
    # cache needs to be hierarchical to avoid collisions, thus we
    # use location so that files with the same name and different
    # contents in different packages do not collide
    cachedname = createCacheName(cfg, name, location)
    util.mkdirChain(os.path.dirname(cachedname))
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
    util.mkdirChain(os.path.dirname(negativeEntry))
    open(negativeEntry, "w+").close()

def _searchCache(cfg, name, location):
    basename = os.path.basename(name)

    networkSource = False
    for prefix in networkPrefixes:
        if name.startswith(prefix):
            networkSource = True
            break

    if networkSource:
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
	log.info('found %s in repository', basename)
	return repCache.cacheFile(cfg, name, location, basename)


    return None


def fetchURL(cfg, name, location, httpHeaders={}, guessName=None, mirror=None):

    retries = 0
    url = None
    mirror = mirror or name

    # check for negative cache entries to avoid spamming servers
    negativeName = _createNegativeCacheName(cfg, name, location)
    if os.path.exists(negativeName):
        if time.time() > 60*60 + os.path.getmtime(negativeName):
            os.remove(negativeName)
        else:
            log.warning('not fetching %s (negative cache entry %s exists)',
                        name, negativeName)
            return None

    log.info('Trying %s...', name)
    while retries < 5:
        try:
            req = urllib2.Request(name, headers=httpHeaders)
            url = urllib2.urlopen(req)
            if not name.startswith('ftp://'):
                content_type = url.info()['content-type']
                if guessName and 'text/html' in content_type:
                    raise urllib2.URLError('"%s" not found' % name)
            log.info('Downloading %s...', name)
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

    rc = _createCacheEntry(cfg, mirror, location, url)
    return rc

# some recipes reach into Conary internals here, and have references
# to searchAll
def searchAll(cfg, repCache, name, location, srcdirs, autoSource=False,
              httpHeaders={}, localOnly=False):
    return findAll(cfg, repCache, name, location, srcdirs, autoSource,
                   httpHeaders, localOnly, allowNone=True)

def findAll(cfg, repCache, name, location, srcdirs, autoSource=False,
            httpHeaders={}, localOnly=False, guessName=None, suffixes=None,
            allowNone=False):

    """
    searches all locations, including populating the cache if the
    file can't be found in srcdirs, and returns the name of the file.
    autoSource should be True when the file has been pulled from an RPM,
    and so has no path associated but is still auto-added
    """

    if not autoSource and not suffixes and not guessName and not name.startswith('/'):
        # these are files that do not start with / and are not
        # indirectly fetched via RPMs, so we look in the local directory
        f = util.searchFile(name, srcdirs)
        if f: return f

    if not guessName:
        if suffixes:
            names = [ "%s.%s" % (name, suffix) for suffix in suffixes ]
        else:
            names = (name, )
    else:
        suffixes = suffixes or ( 'tar.bz2', 'tar.gz', 'tbz2', 'tgz', 'zip' )
        names = [ "%s%s.%s" % (name, guessName, suffix)
                  for suffix in suffixes ]

    # This for r.addArchive('%(name)s-%(version)s.tar.gz', keyid='9BB19A22') case
    if not autoSource and guessName and '/' not in name:
        for sourcename in names:
            f = util.searchFile(sourcename, srcdirs)
            if f: return f

    if localOnly:
        if not allowNone:
            raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT), name)
        else:
            # we do not guess suffixes for local changes
            return None

    for sourcename in names:

        # this needs to come as soon as possible to preserve reproducability
        f = _searchRepository(cfg, repCache, sourcename, location)
        if f: return f

        # OK, now look in the lookaside cache
        # this is for sources that will later be auto-added
        # one way or another
        f = _searchCache(cfg, sourcename, location)
        if f: return f

        # finally, look in srcdirs if appropriate
        if not autoSource and not guessName and '/' not in name:
            f = util.searchFile(sourcename, srcdirs)
            if f: return f


    # Need to fetch a file that will be auto-added to the repository
    # on commit
    if name.startswith('mirror://'):

        urls = []
        # mirror://foo/bar -> mirrorType = "foo", trailingName = "bar"
        mirrorType = name.split('//')[1].split('/', 1)[0]
        mirrorLen = len('mirror://') + len(mirrorType) + 1
        for mirrorBaseURL in Mirror(cfg, mirrorType):
            for name in names:
                trailingName = name[mirrorLen:]
                urls.append(('/'.join((mirrorBaseURL, trailingName)), name))

        names = urls

    for name in names:

        mirror=None
        if type(name) == type(()):
            name, mirror = name

        prefix = name.split('://', 1)[0] + '://'
        if not prefix in networkPrefixes:
            continue

        f = fetchURL(cfg, name, location, httpHeaders, guessName, mirror)
        if f: return f

    if not allowNone:
        raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT), name)


class RepositoryCache:

    def addFileHash(self, troveName, troveVersion, pathId, path, fileId,
                    fileVersion, sha1):
	self.nameMap[path] = (troveName, troveVersion, pathId, path, fileId,
                              fileVersion, sha1)

    def hasFileName(self, fileName):
	return fileName in self.nameMap

    def cacheFile(self, cfg, fileName, location, basename):
	cachedname = createCacheName(cfg, fileName, location)
        util.mkdirChain(os.path.dirname(cachedname))

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
                log.info('%s not yet cached, fetching...', fileName)

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
