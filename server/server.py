#!/usr/bin/python2.3

import sys

if len(sys.argv) != 3:
    print "needs path to srs and to the repository"
    sys.exit(1)

sys.path.append(sys.argv[1])

from SimpleXMLRPCServer import SimpleXMLRPCServer
from repository import fsrepos
import xmlshims

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

netRepos = NetworkRepositoryServer(sys.argv[2], "r")

server = SRSServer(("localhost", 8000))
server.register_instance(netRepos)
server.register_introspection_functions()
server.serve_forever()

