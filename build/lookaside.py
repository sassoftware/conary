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
import errno
import os
import util
import urllib2
import time
import log

# location is normally the package name

def createCacheName(cfg, name, location, negative=''):
    cachedname = os.sep.join((cfg.lookaside, negative + location, name))
    util.mkdirChain(os.path.dirname(cachedname))
    return cachedname

def createCacheEntry(cfg, name, location, infile):
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
	    createNegativeCacheEntry(cfg, name[5:], location)
	    return None

    return cachedname

def createNegativeCacheEntry(cfg, name, location):
    negativeEntry = createCacheName(cfg, name, location, 'NEGATIVE' + os.sep)
    open(negativeEntry, "w+").close()

def searchCache(cfg, name, location):
    basename = os.path.basename(name)

    if name.startswith("http://") or name.startswith("ftp://"):

	# check for negative cache entries to avoid spamming servers
	negativeName = os.sep.join((cfg.lookaside, 'NEGATIVE',
                                    location, name[5:]))
	if os.path.exists(negativeName):
	    if time.time() > 60*60*24*7 + os.path.getmtime(negativeName):
		os.remove(negativeName)
		return searchCache(cfg, name, location)
	    return -1

	# exact match first, then look for cached responses from other servers
	positiveName = os.sep.join((cfg.lookaside, location, name[5:]))
	if os.path.exists(positiveName):
	    return positiveName
	return util.searchPath(basename, os.sep.join((cfg.lookaside,
                                                      location, basename)))
    else:
	return util.searchFile(basename,
                               [os.sep.join((cfg.lookaside, location))])


def searchRepository(cfg, repCache, name, location):
    """searches repository, and retrieves to cache"""
    basename = os.path.basename(name)

    if repCache.hasFile(basename):
	log.debug('found %s in repository', name)
	return repCache.moveFileToCache(cfg, basename, location)

    return None


def searchAll(cfg, repCache, name, location, srcdirs):
    """searches all locations, including populating the cache if the
    file can't be found in srcdirs, and returns the name of the file"""
    f = util.searchFile(os.path.basename(name), srcdirs)
    if f: return f

    # this needs to come before searching the cache, with the expense
    # of repopulating the cache "unnecessarily", to preserve reproducability
    f = searchRepository(cfg, repCache, name, location)
    if f: return f

    f = searchCache(cfg, name, location)
    if f and f != -1: return f

    if (name.startswith("http://") or name.startswith("ftp://")) and f != -1:
        log.info('Downloading %s...', name)
        retries = 0
        url = None
        while retries < 5:
            try:
                url = urllib2.urlopen(name)
                break
            except urllib2.HTTPError, msg:
                if msg.code == 404:
                    createNegativeCacheEntry(cfg, name[5:], location)
                    return None
            except urllib2.URLError:
                createNegativeCacheEntry(cfg, name[5:], location)
                return None
            except IOError, msg:
                # only retry for server busy.
                if 'ftp error] 421' in msg:
                    log.info('FTP server busy when retrieving %s.  Retrying in 10 seconds.', name, msg)
                    time.sleep(10)
                    retries += 1
                else:
                    createNegativeCacheEntry(cfg, name[5:], location)
                    return None
        if url is None:
            return None

	rc = createCacheEntry(cfg, name, location, url)
	return rc

    return None


def findAll(cfg, repcache, name, location, srcdirs):
    f = searchAll(cfg, repcache, name, location, srcdirs)
    if not f:
	raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT), name)
    return f


class RepositoryCache:

    def addFileHash(self, troveName, troveVersion, troveFlavor, path, 
		    fileVersion):
	self.map[path] = (troveName, troveVersion, troveFlavor, path,
			      fileVersion)

    def hasFile(self, fileName):
	return self.map.has_key(fileName)

    def moveFileToCache(self, cfg, fileName, location):
	cachedname = createCacheName(cfg, fileName, location)
	(troveName, troveVersion, troveFlavor, troveFile,
	 troveFileVersion) = self.map[fileName]
	f = self.repos.getFileContents(troveName, troveVersion, troveFlavor,
                                       troveFile, troveFileVersion).get()
	util.copyfileobj(f, open(cachedname, "w"))

	return cachedname

    def __init__(self, repos):
	self.repos = repos
	self.map = {}
