#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Provides a cache for storing files locally, including
downloads and unpacking layers of files.
"""

import errno
from lib import log
from lib import sha1helper
from lib import util
import os
import socket
import time
import urllib2

# location is normally the package name

def createCacheName(cfg, name, location, negative=''):
    cachedname = os.sep.join((cfg.lookaside, negative + location, name))
    util.mkdirChain(os.path.dirname(cachedname))
    return cachedname

def _createCacheEntry(cfg, name, location, infile):
    # cache needs to be hierarchical to avoid collisions, thus we
    # use location so that files with the same name and different
    # contents in different packages do not collide
    filename = name[5:]
    cachedname = createCacheName(cfg, filename, location)
    f = open(cachedname, "w+")
    while 1:
        buf = infile.read(1024 * 128)
        if not buf:
            break
        f.write(buf)
    f.close()
    infile.close()

    # work around FTP bug (msw had a better way?)
    if name.startswith("ftp://"):
	if os.stat(cachedname).st_size == 0:
	    os.unlink(cachedname)
	    _createNegativeCacheEntry(cfg, name[5:], location)
	    return None

    return cachedname

def _createNegativeCacheEntry(cfg, name, location):
    negativeEntry = createCacheName(cfg, name, location, 'NEGATIVE' + os.sep)
    open(negativeEntry, "w+").close()

def _searchCache(cfg, name, location):
    basename = os.path.basename(name)

    if name.startswith("http://") or name.startswith("ftp://"):

	# check for negative cache entries to avoid spamming servers
	negativeName = os.sep.join((cfg.lookaside, 'NEGATIVE',
                                    location, name[5:]))
	if os.path.exists(negativeName):
	    if time.time() > 60*60 + os.path.getmtime(negativeName):
		os.remove(negativeName)
            else:
                log.warning('found %s, therefore not fetching %s',
                    negativeName, name)
                return -1

	# exact match first, then look for cached responses from other servers
	positiveName = createCacheName(cfg, name[5:], location)
	if os.path.exists(positiveName):
	    return positiveName
	return util.searchPath(basename, os.sep.join((cfg.lookaside,
                                                      location, basename)))
    else:
	return util.searchFile(basename,
                               [os.sep.join((cfg.lookaside, location))])


def _searchRepository(cfg, repCache, name, location):
    """searches repository, and retrieves to cache"""
    basename = os.path.basename(name)

    if repCache.hasFileName(basename):
	log.debug('found %s in repository', name)
	return repCache.cacheFile(cfg, basename, location)

    return None


def fetchURL(cfg, name, location):
    log.info('Downloading %s...', name)
    retries = 0
    url = None
    while retries < 5:
        try:
            url = urllib2.urlopen(name)
            break
        except urllib2.HTTPError, msg:
            if msg.code == 404:
                _createNegativeCacheEntry(cfg, name[5:], location)
                return None
        except urllib2.URLError:
            _createNegativeCacheEntry(cfg, name[5:], location)
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
                _createNegativeCacheEntry(cfg, name[5:], location)
                return None
        except IOError, msg:
            # only retry for server busy.
            if 'ftp error] 421' in msg:
                log.info('FTP server busy when retrieving %s.'
                         '  Retrying in 10 seconds.', name, msg)
                time.sleep(10)
                retries += 1
            else:
                _createNegativeCacheEntry(cfg, name[5:], location)
                return None
    if url is None:
        return None

    rc = _createCacheEntry(cfg, name, location, url)
    return rc

def searchAll(cfg, repCache, name, location, srcdirs, autoSource=False):
    """
    searches all locations, including populating the cache if the
    file can't be found in srcdirs, and returns the name of the file.
    autoSource should be True when the file has been pulled from an RPM,
    and so has no path associated but is still auto-added
    """
    if '/' not in name and not autoSource:
        # these are files that do not have / in the name and are not
        # indirectly fetched via RPMs, so we look in the local directory
        f = util.searchFile(name, srcdirs)
        if f: return f

    # this needs to come as soon as possible to preserve reproducability
    f = _searchRepository(cfg, repCache, name, location)
    if f: return f

    # OK, now look in the lookaside cache
    # this is for sources that will later be auto-added
    # one way or another
    f = _searchCache(cfg, name, location)
    if f and f != -1: return f

    # Need to fetch a file that will be auto-added to the repository
    # on commit
    if (name.startswith("http://") or name.startswith("ftp://")) and f != -1:
        return fetchURL(cfg, name, location)

    # could not find it anywhere
    return None


def findAll(cfg, repcache, name, location, srcdirs, autoSource=False):
    f = searchAll(cfg, repcache, name, location, srcdirs, autoSource)
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

    def cacheFile(self, cfg, fileName, location):
	cachedname = createCacheName(cfg, fileName, location)
        if fileName in self.cacheMap:
            # don't check sha1 twice
            return self.cacheMap[fileName]
	(troveName, troveVersion, pathId, troveFile, fileId,
                    troveFileVersion, sha1) = self.nameMap[fileName]
        sha1Cached = None
	if os.path.exists(cachedname):
            sha1Cached = sha1helper.sha1FileBin(cachedname)
        if sha1Cached != sha1:
            f = self.repos.getFileContents(
                [ (fileId, troveFileVersion) ])[0].get()
            util.copyfileobj(f, open(cachedname, "w"))
            fileObj = self.repos.getFileVersion(
                pathId, fileId, troveFileVersion)
            fileObj.chmod(cachedname)
        self.cacheMap[fileName] = cachedname
	return cachedname

    def __init__(self, repos):
	self.repos = repos
	self.nameMap = {}
        self.cacheMap = {}
