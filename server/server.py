#!/usr/bin/python2.3

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
import xmlshims
import select

class SRSServer(SimpleXMLRPCServer):

    allow_reuse_address = 1

class HttpRequests(SimpleHTTPRequestHandler):
    
    files = {}

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

	if not self.files.has_key(path):
	    # XXX we need to do something smarter here
	    return "/tmp/ "

	del self.files[path]
	self.cleanup = path
        return path

    def do_GET(self):
	self.cleanup = None
	SimpleHTTPRequestHandler.do_GET(self)
	if self.cleanup:
	    os.unlink(self.cleanup)

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

    def pkgLatestVersion(self, pkgName, branchStr):
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
	fileName = os.path.basename(path)
	HttpRequests.files[path] = True
	return "http://localhost:8001/%s" % fileName

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

