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
