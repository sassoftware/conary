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

import deps.deps
import versions
import files
import base64
from lib import sha1helper

class NetworkConvertors(object):

    def freezeVersion(self, v):
	return v.freeze()

    def thawVersion(self, v):
	return versions.ThawVersion(v)

    def fromVersion(self, v):
	return v.asString()

    def toVersion(self, v):
	return versions.VersionFromString(v)

    def fromFileId(self, f):
	return sha1helper.sha1ToString(f)

    def toFileId(self, f):
	return sha1helper.sha1FromString(f)

    def fromBranch(self, b):
	return b.asString()

    def toBranch(self, b):
	return versions.VersionFromString(b)

    def toFlavor(self, f):
	if f == 0 or f == "none" or f is None:
	    return None

	return deps.deps.ThawDependencySet(f)

    def fromFlavor(self, f):
	if f is None:
	    return 0

	return f.freeze()

    def toFile(self, f):
        fileId = f[:40]
        return files.ThawFile(base64.decodestring(f[40:]), 
			      self.toFileId(fileId))

    def fromFile(self, f):
        s = base64.encodestring(f.freeze())
        return self.fromFileId(f.id()) + s

    def fromLabel(self, l):
	return l.asString()

    def toLabel(self, l):
	return versions.BranchName(l)
