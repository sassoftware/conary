#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

import deps.deps
import versions
import files
import base64

class NetworkConvertors:

    def fromVersion(self, v):
	return v.asString()

    def toVersion(self, v):
	return versions.VersionFromString(v)

    def fromBranch(self, b):
	return b.asString()

    def toBranch(self, b):
	return versions.VersionFromString(b)

    def toFlavor(self, f):
	if f == 0:
	    return None

	return deps.deps.ThawDependencySet(f)

    def fromFlavor(self, f):
	if f is None:
	    return 0

	return f.freeze()

    def toFile(self, f):
        fileId = f[:40]
        return files.ThawFile(base64.decodestring(f[40:]), fileId)

    def fromFile(self, f):
        s = base64.encodestring(f.freeze())
        return f.id() + s

    def fromLabel(self, l):
	return str(l)

    def toLabel(self, l):
	return versions.BranchName(l)
