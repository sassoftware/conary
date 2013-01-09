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


"""
Basic error types for all things conary.

The base of the conary error hierarchy is defined here.
Other errors hook into these base error classes, but are
defined in places closer to where they are used.

The cvc error hierarchy is defined in the cvc build dir.
"""

__developer_api__ = True

class InternalConaryError(Exception):
    """Base class for conary errors that should never make it to
       the user.  If this error is raised anywhere it means neither bad
       input nor bad environment but a logic error in conary.

       Can be used instead of asserts, e.g., when there is a > normal
       chance of it being hit.

       Also reasonable to use as a mix-in, so, that an exception can be in
       its correct place in the hierarchy, while still being internal.
    """


class ConaryError(Exception):
    """Base class for all exposed conary errors"""
    pass


class CvcError(ConaryError):
    """Base class for errors that are cvc-specific."""
    pass


class ParseError(ConaryError):
    """Base class for errors parsing input"""
    pass

class TroveSpecError(ParseError):
    """Error parsing a trove spec (parseTroveSpec or TroveSpec)"""
    # Part of the conaryclient.cmdline.parseTroveSpec API
    def __init__(self, spec, error):
        self.spec = spec
        ParseError.__init__(self, 'Error with spec "%s": %s' % (spec, error))

class VersionStringError(ConaryError):
    """Base class for other version string specific error"""
    pass

class DatabaseError(ConaryError):
    """ Base class for errors communicating with the local database. """
    pass


class ClientError(ConaryError):
    """Base class for errors in the conaryclient library."""
    pass

class RepositoryError(ConaryError):
    """
        Base class for errors communicating to the repository, though not
        necessarily with the returned values.
    """

    def marshall(self, marshaller):
        return (str(self),), {}

    @staticmethod
    def demarshall(marshaller, tup, kwArgs):
        return (tup[0],), {}

class WebError(ConaryError):
    """ Base class for errors with the web client """
    pass

class FilesystemError(ConaryError):
    """Base class for errors that are filesystem-specific"""
    def __init__(self, errorCode, path, errorString, *args, **kwargs):
        self.errorCode = errorCode
        self.path = path
        self.errorString = errorString
        ConaryError.__init__(self, *args, **kwargs)

class TroveNotFound(ConaryError):
    """
    No trove was found or the match parameters were incorrectly specified.
    """

class TroveSpecsNotFound(ConaryError):
    """
    Just like TroveNotFound, but takes TroveSpecs instead of arbitrary
    strings.
    """
    def __init__(self, specList):
        self.specList = specList

    def __str__(self):
        return ' '.join([ "No troves found matching:" ] + [
                          item.asString() for item in self.specList ])

    __repr__ = __str__


class LatestRequired(TroveNotFound):
    """Returned from findTrove when flavor filtering results in an old trove"""
    def __init__(self, requireData):
        self.requireData = requireData
        self.message = None

    def _genMessage(self):
        message = ""
        for troveNVF, flavors, newVersion in self.requireData:
            message += "%s=%s[%s] was found, but newer troves exist:\n" % \
                    troveNVF
            for flv in flavors:
                message += "%s=%s[%s]\n" % (troveNVF[0], newVersion,
                                          flv.difference(troveNVF[2]))
            message += '\n'
        message += "This error indicates that conary selected older versions of the troves mentioned above due to flavor preference. You should probably select one of the current flavors listed. If you meant to select an older trove, you can pass requireLatest=False as a parameter to r.add, r.replace or related calls. To disable requireLatest checking entirely, declare requireLatest=False as a recipe attribute."
        self.message = message

    def __str__(self):
        if not self.message:
            self._genMessage()
        return self.message

class LabelPathNeeded(TroveNotFound):
    """Returned from findTrove when a label path is required but wasn't given"""

class DatabasePathConflicts(DatabaseError):
    """Occurs when multiple paths conflict inside of a job. This should
       always be handled internally."""

    def getConflicts(self):
        return self.l

    def __init__(self, l):
        self.l = l

class DatabaseLockedError(DatabaseError):
    """
    Occurs when the local database is locked
    """
    def __str__(self):
        return ("The local database is locked.  It is possible that a "
                "database journal file exists that needs to be rolled back, "
                "but you don't have write permission to the database.")

class ShadowRedirect(ConaryError):
    """User attempted to create a shadow (or branch, but branches aren't
       really supported anymore) or a redirect"""

    def __str__(self):
        return "cannot create a shadow of %s=%s[%s] because it is a redirect" \
                    % self.info

    def __init__(self, n, v, f):
        self.info = (n, v, f)

class MissingTrovesError(ConaryError):

    def __str__(self):
        l = []
        if self.missing:
            l.append(
                "The following troves are missing from the repository and " \
                 "cannot be installed: %s" % \
                 ", ".join([ "%s=%s[%s]" % x for x in self.missing ]))
        if self.removed:
            l.append(
                "The following troves no longer exist in the repository and " \
                 "cannot be installed: %s" % \
                 ", ".join([ "%s=%s[%s]" % x for x in self.removed ]))
        return '\n'.join(l)

    def __init__(self, missing=[], removed=[]):
        self.missing = missing
        self.removed = removed

class InvalidRegex(ParseError):
    """User attempted to input an invalid regular expression"""

    def __str__(self):
        return "%s is not a valid regular expression" % self.expr

    def __init__(self, expr):
        self.expr = expr

class ReexecRequired(ConaryError):
    """
       Conary needs to reexec itself with the same command again.
       Can occur due to critical component updates.
    """
    def __init__(self, msg, params=None, data=None):
        self.execParams = params
        self.data = data
        ConaryError.__init__(self, msg)

class DecodingError(ConaryError):
    """
    An error occurred while loading the frozen representation of a data
    structure
    """

class CancelOperationException(Exception):
    """Inherit from this class and throw exceptions of this type if you
    want a callback to stop an update at the end of the job"""
    cancelOperation = True

class MissingRollbackCapsule(ConaryError):
    """
    An error occurred while loading the frozen representation of a data
    structure
    """

UncatchableExceptionClasses = ( SystemExit, KeyboardInterrupt )

def exceptionIsUncatchable(e):
    if isinstance(e, UncatchableExceptionClasses):
        return True
    if hasattr(e, "errorIsUncatchable") and e.errorIsUncatchable:
        return True
    return False
