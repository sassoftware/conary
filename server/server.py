#!/usr/bin/python2.3

import sys

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
	cs.writeToFile("FOO")
	return "http://localhost:8001/FOO"

netRepos = NetworkRepositoryServer(sys.argv[2], "r")

xmlServer = SRSServer(("localhost", 8000))
xmlServer.register_instance(netRepos)
xmlServer.register_introspection_functions()

httpServer = HTTPServer(("localhost", 8001), SimpleHTTPRequestHandler)

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

