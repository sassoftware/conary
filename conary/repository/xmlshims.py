#
# Copyright (c) 2004 rPath, Inc.
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

import base64

from conary import deps, files, versions
from conary.lib import sha1helper

class NetworkConvertors(object):

    def freezeVersion(self, v):
	return v.freeze()

    def thawVersion(self, v):
	return versions.ThawVersion(v)

    def fromVersion(self, v):
	return v.asString()

    def toVersion(self, v):
	return versions.VersionFromString(v)

    def fromPathId(self, f):
        assert(len(f) == 16)
	return base64.encodestring(f)

    def toPathId(self, f):
        assert(len(f) == 25)
	return base64.decodestring(f)

    def fromFileId(self, f):
        assert(len(f) == 20)
	return base64.encodestring(f)

    def toFileId(self, f):
        assert(len(f) == 29)
	return base64.decodestring(f)

    def fromPath(self, path):
	return base64.encodestring(path)

    def toPath(self, path):
	return base64.decodestring(path)

    def fromBranch(self, b):
	return b.asString()

    def toBranch(self, b):
	return versions.VersionFromString(b)

    def toFlavor(self, f):
        assert(f is not None)
        if f is 0:
            return None
	return deps.deps.ThawDependencySet(f)

    def fromFlavor(self, f):
        if f is None:
            return 0
	return f.freeze()

    def toFile(self, f):
        pathId = f[:25]
        return files.ThawFile(base64.decodestring(f[25:]), 
			      self.toPathId(pathId))

    def fromFile(self, f):
        s = base64.encodestring(f.freeze())
        return self.fromPathId(f.pathId()) + s

    def fromLabel(self, l):
	return l.asString()

    def toLabel(self, l):
	return versions.Label(l)

    def fromDepSet(self, ds):
        return ds.freeze()

    def toDepSet(self, ds):
        return deps.deps.ThawDependencySet(ds)

    def fromEntitlement(self, ent):
        return base64.encodestring(ent)

    def toEntitlement(self, ent):
        return base64.decodestring(ent)

    def fromTroveTup(self, tuple):
        return (tuple[0], self.fromVersion(tuple[1]), self.fromFlavor(tuple[2]))

    def toTroveTup(self, tuple):
        return (tuple[0], self.toVersion(tuple[1]), self.toFlavor(tuple[2]))
        
