#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import sys
import errno
import os
import util
import urllib2
import time

# location is normally the package name

def createCacheName(cfg, name, location, negative=''):
    cachedname = '%s/%s%s/%s' %(cfg.lookaside, negative, location, name)
    util.mkdirChain(os.path.dirname(cachedname))
    return cachedname

def createCacheEntry(cfg, name, location, infile):
    # cache needs to be hierarchical to avoid collisions, thus we
    # use location so that files with the same name and different
    # contents in different packages do not collide
    cachedname = createCacheName(cfg, name, location)
    f = open(cachedname, "w+")
    while 1:
        buf = infile.read(1024 * 128)
        if not buf:
            break
        f.write(buf)
    f.close()
    infile.close()
    return cachedname

def createNegativeCacheEntry(cfg, name, location):
    negativeEntry = createCacheName(cfg, name, location, 'NEGATIVE/')
    open(negativeEntry, "w+").close()

def searchCache(cfg, name, location):
    basename = os.path.basename(name)

    if name.startswith("http://") or name.startswith("ftp://"):

	# check for negative cache entries to avoid spamming servers
	negativeName = '%s/NEGATIVE/%s/%s' %(cfg.lookaside, location, name[5:])
	if os.path.exists(negativeName):
	    if time.time() > 60*60*24*7 + os.path.getmtime(negativeName):
		os.remove(negativeName)
		return searchCache(cfg, name, location)
	    return None

	# exact match first, then look for cached responses from other servers
	positiveName = '%s/%s/%s' %(cfg.lookaside, location, name[5:])
	if os.path.exists(positiveName):
	    return positiveName
	return util.searchPath(basename, '%s/%s/%s' %(cfg.lookaside, location,
	                                              basename))
    else:
	return util.searchFile(basename, ['%s/%s' %(cfg.lookaside, location)])


def searchRepository(cfg, repCache, name, location):
    """searches repository, and retrieves to cache"""
    basename = os.path.basename(name)

    if repCache.hasFile(basename):
	print '+ found %s in repository' %name
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
    if f: return f

    if name.startswith("http://") or name.startswith("ftp://"):
	sys.stdout.write("Downloading %s..." % name)
	sys.stdout.flush()
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
            except IOError, msg:
                print 'Error retreiving', name + '.', msg, ' Retrying in 10 seconds.'
                time.sleep(10)
                retries += 1
            except urllib2.URLError:
                createNegativeCacheEntry(cfg, name[5:], location)
                return None
        if url is None:
            return None

	rc = createCacheEntry(cfg, name[5:], location, url)
	print
	return rc

    return None


def findAll(cfg, repcache, name, location, srcdirs):
    f = searchAll(cfg, repcache, name, location, srcdirs)
    if not f:
	raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT))
    return f


class RepositoryCache:

    def addFileHash(self, name, hash):
	self.map[name] = hash

    def hasFile(self, name):
	return self.map.has_key(name)

    def moveFileToCache(self, cfg, name, location):
	cachedname = createCacheName(cfg, name, location)
	dest = open(cachedname, "w+")
	src = self.repos.pullFileContentsObject(self.map[name])
	dest.write(src.read())
	src.close()
	dest.close()
	return cachedname

    def __init__(self, repos):
	self.repos = repos
	self.map = {}
