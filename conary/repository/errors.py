#
# Copyright (c) 2005 rPath, Inc.
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
from conary import versions

class RepositoryMismatch(Exception):
    pass

class InsufficientPermission(Exception):

    def __str__(self):
        if self.server:
            return "Insufficient permission to access server %s" % self.server

        return "Insufficient permission"
        
    def __init__(self, server = None):
        self.server = server

class RepositoryError(Exception):
    """Base class for exceptions from the system repository"""

class MethodNotSupported(RepositoryError):
    """Attempt to call a server method which does not exist"""

class TroveNotFound(RepositoryError):
    """Raised when findTrove failes"""

class RepositoryLocked(RepositoryError):
    def __str__(self):
        return 'The repository is currently busy.  Try again in a few moments.'

class OpenError(RepositoryError):
    """Error occured opening the repository"""

class CommitError(RepositoryError):
    """Error occured commiting a trove"""

class DuplicateBranch(RepositoryError):
    """Error occured commiting a trove"""

class TroveMissing(RepositoryError):
    troveType = "trove"
    def __str__(self):
        if type(self.version) == list:
            return ('%s %s does not exist for any of '
                    'the following labels:\n    %s' %
                    (self.troveType, self.troveName,
                     "\n    ".join([x.asString() for x in self.version])))
        elif self.version:
            if isinstance(self.version, versions.Branch):
                return ("%s %s does not exist on branch %s" % \
                    (self.troveType, self.troveName, self.version.asString()))

            return "version %s of %s %s does not exist" % \
                (self.version.asString(), self.troveType, self.troveName)
	else:
	    return "%s %s does not exist" % (self.troveType, self.troveName)

    def __init__(self, troveName, version = None):
	"""
	Initializes a TroveMissing exception.

	@param troveName: trove which could not be found
	@type troveName: str
	@param version: version of the trove which does not exist
	@type version: versions.Version
	"""
	self.troveName = troveName
	self.version = version
        if troveName.startswith('group-'):
            self.type = 'group'
        elif troveName.startswith('fileset-'):
            self.type = 'fileset'
        elif troveName.find(':') != -1:
            self.type = 'component'
        else:
            self.type = 'package'

class UnknownException(RepositoryError):
    def __str__(self):
	return "UnknownException: %s %s" % (self.eName, self.eArgs)

    def __init__(self, eName, eArgs):
	self.eName = eName
	self.eArgs = eArgs

class UserAlreadyExists(RepositoryError):
    pass

class GroupAlreadyExists(RepositoryError):
    pass

class PermissionAlreadyExists(RepositoryError):
    pass

class UserNotFound(Exception):
    def __init__(self, user = "user"):
        self.user = user

    def __str__(self):
        return "UserNotFound: %s" % self.user

class InvalidServerVersion(RepositoryError):
    pass

class GetFileContentsError(RepositoryError):
    def __init__(self, val):
        Exception.__init__(self)
        self.val = val

class FileContentsNotFound(GetFileContentsError):
    def __init__(self, val):
        GetFileContentsError.__init__(self, val)

class FileStreamNotFound(GetFileContentsError):
    def __init__(self, val):
        GetFileContentsError.__init__(self, val)

class InvalidClientVersion(RepositoryError):
    pass

class AlreadySignedError(RepositoryError):
    def __str__(self):
        return self.error
    def __init__(self, error = "Already signed"):
        self.error=error

class DigitalSignatureError(RepositoryError):
    def __str__(self):
        return self.error
    def __init__(self, error = "Trove can't be signed"):
        self.error=error

from conary.trove import DigitalSignatureVerificationError, TroveIntegrityError
from conary.lib.openpgpfile import KeyNotFound, BadSelfSignature, IncompatibleKey

# This is a list of simple exception classes and the text string
# that should be used to marshall an exception instance of that
# class back to the client.  The str() value of the exception will
# be returned as the exception argument.
simpleExceptions = (
    (AlreadySignedError,         'AlreadySignedError'),
    (BadSelfSignature,           'BadSelfSignature'),
    (DigitalSignatureVerificationError, 'DigitalSignatureVerificationError'),
    (GroupAlreadyExists,         'GroupAlreadyExists'),
    (IncompatibleKey,            'IncompatibleKey'),
    (InvalidClientVersion,       'InvalidClientVersion'),
    (KeyNotFound,                'KeyNotFound'),
    (UserAlreadyExists,          'UserAlreadyExists'),
    (UserNotFound,               'UserNotFound'),
    (CommitError,                'CommitError'),
    (DuplicateBranch,            'DuplicateBranch'),
    (TroveIntegrityError,        'TroveIntegrityError'),
    )
