#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import base64

from conary import deps, files, versions
from conary.lib import compat


class NetworkConvertors(object):

    def freezeVersion(self, v):
        return v.freeze()

    def thawVersion(self, v):
        return versions.ThawVersion(v)

    def fromVersion(self, v):
        return v.asString()

    def toVersion(self, v):
        return versions.VersionFromString(v)

    def versionStringToFrozen(self, verStr, timeStamps):
        if isinstance(timeStamps, basestring):
            timeStamps = [float(x) for x in timeStamps.split(':')]
        timeStamps = ['%.3f' % x for x in timeStamps]
        return versions.strToFrozen(verStr, timeStamps)

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
        return deps.deps.ThawFlavor(f)

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

    def toFileAsStream(self, f, rawPathId = False):
        pathId, stream = f[:25], f[25:]
        if not rawPathId:
            pathId = self.toPathId(pathId)

        return pathId, base64.decodestring(stream)

    def fromFileAsStream(self, pathId, stream, rawPathId = False):
        s = base64.encodestring(stream)
        if not rawPathId:
            pathId = self.fromPathId(pathId)

        return pathId + s

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

    def fromTroveTup(self, tuple, withTime=False):
        if withTime:
            return (tuple[0], self.freezeVersion(tuple[1]),
                    self.fromFlavor(tuple[2]))
        else:
            return (tuple[0], self.fromVersion(tuple[1]),
                    self.fromFlavor(tuple[2]))

    def toTroveTup(self, tuple, withTime=False):
        if withTime:
            return (tuple[0], self.thawVersion(tuple[1]),
                    self.toFlavor(tuple[2]))
        else:
            return (tuple[0], self.toVersion(tuple[1]), self.toFlavor(tuple[2]))


class RequestArgs(compat.namedtuple('RequestArgs',
        'version args kwargs')):

    def toWire(self):
        if self.version < 51:
            assert not self.kwargs
            return (self.version,) + tuple(self.args)
        else:
            return (self.version, self.args, self.kwargs)

    @classmethod
    def fromWire(cls, argList):
        version = argList[0]
        if version < 51:
            args = argList[1:]
            kwargs = {}
        else:
            args, kwargs = argList[1:]
        return cls(version, tuple(args), dict(kwargs))


class ResponseArgs(compat.namedtuple('ResponseArgs',
        'isException result excName excArgs excKwargs')):

    @classmethod
    def newResult(cls, result):
        return cls(False, result, None, None, None)

    @classmethod
    def newException(cls, excName, excArgs=(), excKwargs=()):
        return cls(True, None,
                excName, tuple(excArgs), dict(excKwargs))

    def toWire(self, version):
        """Returns a 2-tuple (response, headers)"""
        if self.isException:
            if version < 60:
                assert not self.excKwargs
                result = (self.excName,) + tuple(self.excArgs)
            else:
                result = (self.excName, self.excArgs, self.excKwargs)
        else:
            result = self.result

        headers = {}
        if 60 <= version <= 70:
            # These versions suffer from an incredibly silly mistake where the
            # isException flag got passed through the X-Conary-UsedAnonymous
            # header.
            if self.isException:
                headers['X-Conary-Usedanonymous'] = '1'
            response = (result,)
        else:
            # Versions <= 59 and >= 71 make more sense.
            response = (self.isException, result)
        return response, headers

    @classmethod
    def fromWire(cls, version, response, headers):
        if 60 <= version <= 70:
            # See comment in toWire()
            isException = 'X-Conary-Usedanonymous' in headers
            result, = response
        else:
            isException, result = response

        if isException:
            if version < 60:
                excName = result[0]
                excArgs = result[1:]
                excKwargs = {}
            else:
                excName, excArgs, excKwargs = result
            result = None
        else:
            excName = excArgs = excKwargs = None

        return cls(isException, result, excName, excArgs, excKwargs)
