#!/usr/bin/python2.3
# -*- mode: python -*-
#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import os
import posixpath
import sys
import tempfile
import urllib

if len(sys.argv) != 3:
    print "needs path to srs and to the repository"
    sys.exit(1)

sys.path.append(sys.argv[1])

from BaseHTTPServer import HTTPServer
from repository import changeset
from repository import fsrepos
from SimpleHTTPServer import SimpleHTTPRequestHandler
from SimpleXMLRPCServer import SimpleXMLRPCServer
import filecontainer
import select
import xmlshims

class SRSServer(SimpleXMLRPCServer):

    allow_reuse_address = 1

class HttpRequests(SimpleHTTPRequestHandler):
    
    outFiles = {}
    inFiles = {}

    def translate_path(self, path):
        """Translate a /-separated PATH to the local filename syntax.

        Components that mean special things to the local file system
        (e.g. drive or directory names) are ignored.  (XXX They should
        probably be diagnosed.)

        """
        path = posixpath.normpath(urllib.unquote(path))
        words = path.split('/')
        words = filter(None, words)
        path = '/tmp'
        for word in words:
            drive, word = os.path.splitdrive(word)
            head, word = os.path.split(word)
            if word in (os.curdir, os.pardir): continue
            path = os.path.join(path, word)

	if not self.outFiles.has_key(path):
	    # XXX we need to do something smarter here
	    return "/tmp/ "

	del self.outFiles[path]
	self.cleanup = path
        return path

    def do_GET(self):
	self.cleanup = None
	SimpleHTTPRequestHandler.do_GET(self)
	if self.cleanup:
	    os.unlink(self.cleanup)

    def do_PUT(self):
	path = "/tmp/" + os.path.basename(self.path)
	if not self.inFiles.has_key(path):
	    self.send_response(410, "Gone")
	    return

	out = open(path, "w")

	contentLength = int(self.headers['Content-Length'])
	while contentLength:
	    s = self.rfile.read(contentLength)
	    contentLength -= len(s)
	    out.write(s)

	self.send_response(200, 'OK')

	self.inFiles[path] = True

class NetworkRepositoryServer(xmlshims.NetworkConvertors):

    def allTroveNames(self):
	return [ x for x in self.iterAllTroveNames() ]

    def createBranch(self, newBranch, kind, frozenLocation, troveList):
	newBranch = self.toLabel(newBranch)
	if kind == 'v':
	    location = self.toVersion(frozenLocation)
	elif kind == 'l':
	    location = self.toLabel(frozenLocation)
	else:
	    return 0

	self.repos.createBranch(newBranch, location, troveList)
	return 1

    def hasPackage(self, pkgName):
	return self.repos.troveStore.hasTrove(pkgName)

    def hasTrove(self, pkgName, version, flavor):
	return self.repos.troveStore.hasTrove(pkgName, troveVersion = version,
					troveFlavor = flavor)

    def getTroveVersionList(self, troveNameList):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ x for x in
			    self.repos.troveStore.iterTroveVersions(troveName) ]

	return d

    def getFilesInTrove(self, troveName, version, flavor,
                        sortByPath = False, withFiles = False):
        gen = self.repos.troveStore.iterFilesInTrove(troveName,
                                               self.toVersion(version),
                                               self.toFlavor(flavor),
                                               sortByPath, 
                                               withFiles) 
        if withFiles:
            return [ (x[0], x[1], self.fromVersion(x[2]), self.fromFile(x[3]))
                     for x in gen ]
        else:
            return [ (x[0], x[1], self.fromVersion(x[2])) for x in gen ]

    def getFileContents(self, sha1list):
	(fd, path) = tempfile.mkstemp()
	f = os.fdopen(fd, "w")

	fc = filecontainer.FileContainer(f)
	del f
	d = self.repos.getFileContents(sha1list)

	for sha1 in sha1list:
	    fc.addFile(sha1, d[sha1], "", d[sha1].fullSize)
	fc.close()

	HttpRequests.outFiles[path] = True
	fileName = os.path.basename(path)
	return "http://localhost:8001/%s" % fileName

    def getAllTroveLeafs(self, troveNames):
	d = {}
	for troveName in troveNames:
	    d[troveName] = [ x for x in
			    self.repos.troveStore.iterAllTroveLeafs(troveName) ]
	return d

    def getTroveLeavesByLabel(self, troveNameList, labelStr):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ x for x in
			self.repos.troveStore.iterTroveLeafsByLabel(troveName,
								   labelStr) ]

	return d

    def getTroveVersionFlavors(self, troveDict):
	newD = {}
	for (troveName, versionList) in troveDict.iteritems():
	    innerD = {}
	    for versionStr in versionList:
		innerD[versionStr] = [ self.fromFlavor(x) for x in 
		    self.repos.troveStore.iterTroveFlavors(troveName, 
						 self.toVersion(versionStr)) ]
	    newD[troveName] = innerD

	return newD

    def getTroveLatestVersion(self, pkgName, branchStr):
	return self.fromVersion(self.repos.troveStore.troveLatestVersion(pkgName, 
						  self.toBranch(branchStr)))

    def getChangeSet(self, chgSetList, recurse, withFiles):
	l = []
	for (name, flavor, old, new, absolute) in chgSetList:
	    if old == 0:
		l.append((name, self.toFlavor(flavor), None,
			 self.toVersion(new), absolute))
	    else:
		l.append((name, self.toFlavor(flavor), self.toVersion(old),
			 self.toVersion(new), absolute))

	cs = self.repos.createChangeSet(l, recurse = recurse, 
					withFiles = withFiles)
	(fd, path) = tempfile.mkstemp()
	os.close(fd)
	cs.writeToFile(path)
	HttpRequests.outFiles[path] = True
	fileName = os.path.basename(path)
	return "http://localhost:8001/%s" % fileName

    def iterAllTroveNames(self):
	return self.repos.iterAllTroveNames()

    def prepareChangeSet(self):
	(fd, path) = tempfile.mkstemp()
	os.close(fd)
	HttpRequests.inFiles[path] = False
	fileName = os.path.basename(path)
	return "http://localhost:8001/%s" % fileName

    def commitChangeSet(self, url):
	assert(url.startswith("http://localhost:8001/"))
	fileName = url.split("/", 3)[3]
	path = "/tmp/" + fileName
	assert(HttpRequests.inFiles[path])

	try:
	    cs = changeset.ChangeSetFromFile(path)
	finally:
	    pass
	    os.unlink(path)

	self.repos.commitChangeSet(cs)

	return True

    def getFileVersion(self, fileId, version, withContents = 0):
	f = self.repos.troveStore.getFile(fileId, self.toVersion(version))
	return self.fromFile(f)

    def checkVersion(self, clientVersion):
        if clientVersion < 0:
            raise RuntimeError, "client is too old"
        return 0

    def __init__(self, path, mode):
	self.repos = fsrepos.FilesystemRepository(path, mode)

def handler(req):
    req.content_type = "text/xml"
    req.send_http_header()
    data = req.read()
    try:
        params, method = xmlrpclib.loads(data)
    except xmlrpclib.ResponseError, e:
        req.write ( xmlrpclib.dumps(e))
    return apache.OK

if __name__ == '__main__':
    netRepos = NetworkRepositoryServer(sys.argv[2], "c")
    xmlServer = SRSServer(("localhost", 8000))
    xmlServer.register_instance(netRepos)
    xmlServer.register_introspection_functions()

    httpServer = HTTPServer(("localhost", 8001), HttpRequests)

    fds = {}
    fds[xmlServer.fileno()] = xmlServer
    fds[httpServer.fileno()] = httpServer

    p = select.poll()
    for fd in fds.iterkeys():
        p.register(fd, select.POLLIN)

    while True:
        try:
            events = p.poll()
            for (fd, event) in events:
                fds[fd].handle_request()
        except select.error:
            pass
