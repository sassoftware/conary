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

def createCacheEntry(cfg, name, location, infile):
    # cache needs to be hierarchical to avoid collisions, thus we
    # use location so that files with the same name and different
    # contents in different packages do not collide
    cachedname = createCacheName(cfg, name, location)
    f = open(cachedname, "w+")
    f.write(infile.read())
    f.close()
    infile.close()
    return cachedname


def createNegativeCacheEntry(cfg, name, location):
    negativeEntry = createCacheName(name, location, 'NEGATIVE/')
    open(negativeEntry, "w+").close()


def searchCache(cfg, name, location):
    basename = os.path.basename(name)

    if name.startswith("http://") or name.startswith("ftp://"):
	# check for negative cache entries to avoid spamming servers
	negativeName = '%s/NEGATIVE/%s' %(cfg.lookaside, location)
	f = util.searchFile(basename, negativeName)
	if f:
	    if time.time() > 60*60*24*7 + os.path.getmtime(f):
		os.remove(negativeName)
		return searchCache(name, location)
	    return None

    return util.searchFile(basename, '%s/%s' %(cfg.lookaside, location))


def searchRepository(cfg, name, location):
    """searches repository, and retrieves to cache"""
    basename = os.path.basename(name)
    # FIXME: I don't know how to do this yet
    return None


def searchAll(cfg, name, location, srcdirs):
    """searches all locations, including populating the cache if the
    file can't be found in srcdirs, and returns the name of the file"""
    f = util.searchFile(name, srcdirs)
    if f: return f

    f = searchCache(cfg, name, location)
    if f: return f

    f = searchRepository(cfg, name, location)
    if f: return f

    if name.startswith("http://") or name.startswith("ftp://"):
	try:
	    url = urllib2.urlopen(name)
	except urllib2.URLError:
	    createNegativeCacheEntry(cfg, name[5:], location)
	    return None
	return createCacheEntry(cfg, name[5:], location, url)

    return None


def findAll(cfg, name, location, srcdirs):
    f = searchAll(cfg, name, location, srcdirs)
    if not f:
	raise OSError, (errno.ENOENT, os.strerror(errno.ENOENT))
    return f


class LookAside(file):
    def __init__(cfg, name, location, srcdirs, buffered=-1):
	f = findAll(cfg, name, location, srcdirs)
	file.__init__(self, f, "r+", buffered)
