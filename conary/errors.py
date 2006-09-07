# -*- mode: python -*-
#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Basic error types for all things conary.

The base of the conary error hierarchy is defined here.
Other errors hook into these base error classes, but are 
defined in places closer to where they are used.

The cvc error hierarchy is defined in the cvc build dir.
"""

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


class CvcError(Exception):
    """Base class for errors that are cvc-specific."""
    pass


class ParseError(ConaryError):
    """Base class for errors parsing input"""
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
    pass

class WebError(ConaryError):
    """ Base class for errors with the web client """
    pass

class TroveNotFound(ConaryError):
    """Returned from findTrove when no trove is matched"""

class DatabasePathConflicts(DatabaseError):
    """Occurs when multiple paths conflict inside of a job. This should
       always be handled internally."""

    def getConflicts(self):
        return self.l

    def __init__(self, l):
        self.l = l

class ShadowRedirect(ConaryError):
    """User attempted to create a shadow (or branch, but branch's aren't
       really supported anymore) or a redirect"""

    def __str__(self):
        return "cannot create a shadow of %s=%s[%s] because it is a redirect" \
                    % self.info

    def __init__(self, n, v, f):
        self.info = (n, v, f)

class RemovedTrovesError(ConaryError):

    def __str__(self):
        return "The following troves no longer exist in the repository and " \
               "cannot be installed: %s" % \
               ", ".join([ "%s=%s[%s]" % x for x in self.l ])

    def __init__(self, troveList):
        self.l = troveList

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

UncatchableExceptionClasses = ( SystemExit, KeyboardInterrupt )

def exceptionIsUncatchable(e):
    return isinstance(e, UncatchableExceptionClasses)
