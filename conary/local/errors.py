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
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary.deps import deps

class UpdateError:

    pass

class DirectoryInWayError(UpdateError):

    def __str__(self):
        return "non-empty directory %s is in the way of a newly created " \
                       "file in %s=%s[%s]" % \
                         (self.path, self.name, str(self.version),
                          deps.formatFlavor(self.flavor))

    def __init__(self, path, name, version, flavor):
        self.path = path
        self.name = name
        self.version = version
        self.flavor = flavor

class FileInWayError(UpdateError):

    def __str__(self):
        return "%s is in the way of a newly created file in %s=%s[%s]" % \
                         (self.path, self.name, str(self.version),
                          deps.formatFlavor(self.flavor))

    def __init__(self, path, name, version, flavor):
        self.path = path
        self.name = name
        self.version = version
        self.flavor = flavor

class DatabasePathConflictError(UpdateError):

    def __str__(self):
        return "%s conflicts with a file owned by %s=%s[%s]" % \
                         (self.path, self.name, str(self.version),
                          deps.formatFlavor(self.flavor))

    def __init__(self, path, name, version, flavor):
        self.path = path
        self.name = name
        self.version = version
        self.flavor = flavor

class PathConflictError(UpdateError):

    def __str__(self):
        return "path conflict for %s (%s on head)" % (self.fsPath, headPath)

    def __init__(self, fsPath, headPath):
        self.fsPath = fsPath
        self.headPath = headPath

class DirectoryToSymLinkError(UpdateError):

    def __str__(self):
        return '%s changed from a directory to a symbolic link.  To apply ' \
               'this changeset, first manually move %s to %s, then run ' \
               '"ln -s %s %s".' % \
                    (self.finalPath, self.finalPath, self.newLocation,
                     self.headPath, self.finalPath)

    def __init__(self, finalPath, newLocation, headPath):
        self.finalPath = finalPath
        self.newLocation = newLocation
        self.headPath = headPath

class DirectoryToNonDirectoryError(UpdateError):

    def __str__(self):
        return "%s changed from a directory to a non-directory" % self.path

    def __init__(self, path):
        self.path = path

class FileTypeChangedError(UpdateError):

    def __str__(self):
        return "file type of %s changed" % self.path

    def __init__(self, path):
        self.path = path

class FileAttributesConflictError(UpdateError):

    def __str__(self):
        return "file attributes conflict for %s" % self.path

    def __init__(self, path):
        self.path = path

class FileContentsConflictError(UpdateError):

    def __str__(self):
        return "file contents conflict for %s" % self.path

    def __init__(self, path):
        self.path = path

