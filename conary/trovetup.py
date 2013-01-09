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


from conary.deps import deps
from conary import errors
from conary import versions
from conary.lib.compat import namedtuple as _namedtuple


class TroveSpec(_namedtuple('TroveSpec', 'name version flavor')):
    """
    A trove spec is a partial trove specification. It contains an optionally
    optional name, an optional version specification, and an optional flavor.
    The version specification may be a full version, a branch, a label,
    a revision or partial revision, or a label plus a revision or partial
    revision.
    """
    __slots__ = ()

    def __new__(cls, name, version=None, flavor=None,
                allowEmptyName=True, withFrozenFlavor=False):
        """
        @param name: the input string or tuple
        @type name: string or tuple

        @param version: optional version, if version not included in name
        @type version: string

        @param flavor: optional version, if version not included in name
        @type flavor: string, or frozen flavor if C{withFrozenFlavor} is True.

        @param allowEmptyName: if set, will accept an empty string and some
        other variations.
        @type allowEmptyName: bool

        @param withFrozenFlavor: if set, will accept a frozen flavor
        @type withFrozenFlavor: bool

        @raise errors.TroveSpecError: Raised if the input string is not
        a valid TroveSpec
        """
        if isinstance(name, (tuple, list)):
            # TroveSpec(sometuple)
            name, version, flavor = name
        elif version is None and flavor is None:
            # TroveSpec('a=b[c]')
            return cls.fromString(name, allowEmptyName=allowEmptyName,
                withFrozenFlavor=withFrozenFlavor)

        # TroveSpec(name, version, flavor)
        if isinstance(flavor, basestring):
            flavor = cls._thawFlavor(flavor, withFrozenFlavor)
        return tuple.__new__(cls, (name, version, flavor))

    def __repr__(self):
        return 'TroveSpec(%r)' % (self.asString(True),)

    def asString(self, withTimestamp=False):
        if self.version is not None:
            version = '=' + self.version
        else:
            version = ''
        if self.flavor is not None:
            flavor = '[' + str(self.flavor) + ']'
        else:
            flavor = ''
        return ''.join((self.name, version, flavor))
    __str__ = asString

    @staticmethod
    def _thawFlavor(flavor, withFrozenFlavor):
        if withFrozenFlavor:
            return deps.ThawFlavor(flavor)
        return deps.parseFlavor(flavor)

    @classmethod
    def fromString(cls, specStr, allowEmptyName=True, withFrozenFlavor=False):
        origSpecStr = specStr
        # CNY-3219: strip leading and trailing whitespaces around job
        # specification
        specStr = specStr.strip()
        if specStr.find('[') > 0 and specStr[-1] == ']':
            specStr = specStr[:-1]
            l = specStr.split('[')
            if len(l) != 2:
                raise errors.TroveSpecError(origSpecStr, "bad flavor spec")
            specStr, flavorSpec = l
            flavor = cls._thawFlavor(flavorSpec, withFrozenFlavor)
            if flavor is None:
                raise errors.TroveSpecError(origSpecStr, "bad flavor spec")
        else:
            flavor = None

        if specStr.find("=") >= 0:
            l = specStr.split("=")
            if len(l) != 2:
                raise errors.TroveSpecError(origSpecStr, "Too many ='s")
            name, versionSpec = l
        else:
            name = specStr
            versionSpec = None
        if not name and not allowEmptyName:
            raise errors.TroveSpecError(origSpecStr, 'Trove name is required')

        return tuple.__new__(cls, (name, versionSpec, flavor))



class TroveTuple(_namedtuple('TroveTuple', 'name version flavor')):
    """
    A trove tuple is a (name, version, flavor) tuple that uniquely identifies a
    single trove. It is always an exact reference.

    For a partial specification, see L{TroveSpec}.
    """
    # NOTE to future developers: if a version of TroveTuple with timestampless
    # versions becomes useful, subclass it instead of kludging this one to
    # support both. You should really never be in a situation where you don't
    # know whether your version has timestamps!
    __slots__ = ()
    hasTimestamp = True
    _thawVerFunc = staticmethod(versions.ThawVersion)
    _thawFlavFunc = staticmethod(deps.parseFlavor)

    def __new__(cls, name, version=None, flavor=None):
        if isinstance(name, (tuple, list)):
            # TroveTuple(sometuple)
            name, version, flavor = name
        elif version is None and flavor is None:
            # TroveTuple('a=b[c]')
            return cls.fromString(name)

        # TroveTuple(name, version, flavor)
        if isinstance(version, basestring):
            version = cls._thawVerFunc(version)
        if isinstance(flavor, basestring):
            flavor = cls._thawFlavFunc(flavor)
        return tuple.__new__(cls, (name, version, flavor))

    def __repr__(self):
        return 'TroveTuple(%r)' % (self.asString(True),)

    def asString(self, withTimestamp=False):
        if withTimestamp:
            ver = self.version.freeze()
        else:
            ver = self.version.asString()
        return '%s=%s[%s]' % (self.name, ver, self.flavor)
    __str__ = asString

    @classmethod
    def fromString(cls, ttstr, withFrozenFlavor=False):
        try:
            ttstr = _cast(ttstr)
        except UnicodeEncodeError:
            raise errors.ParseError("Trove tuple must be ASCII safe")

        equals = ttstr.count('=')
        left = ttstr.count('[')
        right = ttstr.count(']')
        if equals != 1 or left not in (0, 1) or right != left:
            raise errors.ParseError("Not a valid trove tuple")

        equals = ttstr.find('=')
        left = ttstr.find('[')
        right = ttstr.find(']')

        name = ttstr[:equals]
        if left < 0:
            # No flavor.
            assert right < 0
            left = right = len(ttstr)
        elif right != len(ttstr) - 1:
            raise errors.ParseError("Not a valid trove tuple")
        version = ttstr[equals + 1 : left]
        flavor = ttstr[left + 1 : right]
        if not version:
            raise errors.ParseError("Not a valid trove tuple")
        return cls(name, version, flavor)


class JobSpec(_namedtuple('JobSpec', 'name old new')):
    """
    A job spec holds a single update request, including a name, optional old
    version and flavor, and optional new version and flavor.
    """
    __slots__ = ()
    # TODO: Parsers, stringifiers, etc.


class JobTuple(_namedtuple('JobTuple', 'name old new absolute')):
    """
    A job tuple represents a single trove job, consisting of a name, old
    version and flavor, new version and flavor, and a flag indicating whether
    the job is absolute.
    """
    __slots__ = ()
    # TODO: Parsers, stringifiers, etc.


def _cast(val):
    "Return C{val.encode('ascii')} if it is a unicode, or C{val} otherwise."
    if isinstance(val, unicode):
        val = val.encode('ascii')
    return val
