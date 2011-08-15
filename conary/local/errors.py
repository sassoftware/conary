#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


from conary.deps import deps
from conary.errors import ConaryError

class UpdateError(ConaryError):

    pass

class DirectoryInWayError(UpdateError):

    def __str__(self):
        return "directory %s is in the way of a newly created " \
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

class DuplicatePath(UpdateError):

    def __str__(self):
        return "path %s added both locally and in repository" % self.path

    def __init__(self, path):
        self.path = path
