#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#


from conary.deps import deps
from conary import errors
from conary import versions
from conary.lib.compat import namedtuple as _namedtuple


class TroveSpec(_namedtuple('TroveSpec', 'name version flavor')):
    """
    A trove spec is a partial trove specification. It contains a name, an
    optional version specification, and an optional flavor. The version
    specification may be a full version, a branch, a label, a revision or
    partial revision, or a label plus a revision or partial revision.
    """
    __slots__ = ()
    # TODO: Parsers, stringifiers, etc.


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
