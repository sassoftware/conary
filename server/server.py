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

from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleHTTPServer import SimpleHTTPRequestHandler
from BaseHTTPServer import HTTPServer
from repository import fsrepos
from repository import changeset
import xmlshims
import select

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

class NetworkRepositoryServer(xmlshims.NetworkConvertors,
			      fsrepos.FilesystemRepository):

    def allTroveNames(self):
	return [ x for x in self.iterAllTroveNames() ]

    def getTroveVersionList(self, troveNameList):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ x for x in
				self.troveStore.iterTroveVersions(troveName) ]

	return d

    def getAllTroveLeafs(self, troveNames):
	d = {}
	for troveName in troveNames:
	    d[troveName] = [ x for x in
				self.troveStore.iterAllTroveLeafs(troveName) ]
	return d

    def getTroveLeavesByLabel(self, troveNameList, labelStr):
	d = {}
	for troveName in troveNameList:
	    d[troveName] = [ x for x in
			     self.troveStore.iterTroveLeafsByLabel(troveName,
								   labelStr) ]

	return d

    def getTroveVersionFlavors(self, troveDict):
	newD = {}
	for (troveName, versionList) in troveDict.iteritems():
	    innerD = {}
	    for versionStr in versionList:
		innerD[versionStr] = [ self.fromFlavor(x) for x in 
		    self.troveStore.iterTroveFlavors(troveName, 
						 self.toVersion(versionStr)) ]
	    newD[troveName] = innerD

	return newD

    def getTroveLatestVersion(self, pkgName, branchStr):
	return self.troveStore.troveLatestVersion(pkgName, 
						  self.fromVersion(branchStr))

    def getChangeSet(self, chgSetList, recurse, withFiles):
	l = []
	for (name, flavor, old, new, absolute) in chgSetList:
	    if old == 0:
		l.append((name, self.toFlavor(flavor), None,
			 self.toVersion(new), absolute))
	    else:
		l.append((name, self.toFlavor(flavor), self.toVersion(old),
			 self.toVersion(new), absolute))

	cs = self.createChangeSet(l, recurse = recurse, withFiles = withFiles)
	(fd, path) = tempfile.mkstemp()
	os.close(fd)
	cs.writeToFile(path)
	HttpRequests.outFiles[path] = True
	fileName = os.path.basename(path)
	return "http://localhost:8001/%s" % fileName

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

	fsrepos.FilesystemRepository.commitChangeSet(self, cs)

	return True

netRepos = NetworkRepositoryServer(sys.argv[2], "r")

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
    events = p.poll()
    for (fd, event) in events:
	fds[fd].handle_request()

