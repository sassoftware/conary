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
This module defines helper classes designed to define filestream objects.
This allows for creation of filestreams without the need to create files
on the filesystem.
"""

import os
import time

from conary import errors, files
from conary.deps import deps
from conary.lib import digestlib, util
from conary.repository import filecontents

class ParameterError(errors.ClientError):
    """There were errors with the parameters passed into a filestream helper"""

class ConflictingFlags(errors.ClientError):
    """Config, initialContents and transient are mutually exclusive."""

class _File(object):
    """
    Abstract base class which all for other filestream helper classes.

    This class is the parent of all filestream helper classes.  It should
    not be directly instantiated.
    """
    needSha1 = False
    kwargs = {'perms': 0644,
            'tags': None,
            'requires': None,
            'provides': None,
            'flavor': None,
            'owner': 'root',
            'group': 'root',
            'mtime': None}
            #'config': False,
            #'initialContents': False,
            #'transient': False,
    aliasedArgs = {'mode': 'perms'}

    def __init__(self, **kwargs):
        assert type(self) != _File
        for arg in kwargs:
            aliasedArg = self.aliasedArgs.get(arg, arg)
            if aliasedArg not in self.kwargs:
                raise ParameterError("'%s' is not allowed for this class" % arg)

        for key, val in self.aliasedArgs.iteritems():
            if key in kwargs and val in kwargs:
                raise ParameterError( \
                        "'%s' and '%s' cannot be specified together" % \
                        (key, val))
            elif key in kwargs:
                kwargs[val] = kwargs[key]

        for key, val in self.__class__.kwargs.iteritems():
            setattr(self, key, kwargs.get(key, val))

        self.mtime = int(self.mtime or time.time())

        if type(self.requires) == str:
            self.requires = deps.parseDep(self.requires)
        elif self.requires is None:
            self.requires = deps.DependencySet()

        if isinstance(self.provides, str):
            self.provides = deps.parseDep(self.provides)
        elif self.provides is None:
            self.provides = deps.DependencySet()

        if type(self.flavor) == str:
            self.flavor = deps.parseFlavor(self.flavor)
        if self.flavor is None:
            self.flavor = deps.Flavor()

        if self.tags is None:
            self.tags = []

    def getContents(self):
        return None

    def _touchupFileStream(self, fileStream):
        pass

    def get(self, pathId):
        f = self.fileClass(pathId)
        f.inode = files.InodeStream(self.perms & 07777, self.mtime,
                self.owner, self.group)
        self._touchupFileStream(f)
        if self.needSha1:
            sha1 = digestlib.sha1()
            contents = self.contents.get()
            devnull = open(os.devnull, 'w')
            util.copyfileobj(contents, devnull, digest = sha1)
            devnull.close()

            f.contents = files.RegularFileStream()
            f.contents.size.set(contents.tell())
            f.contents.sha1.set(sha1.digest())
        f.provides.set(self.provides)
        f.requires.set(self.requires)
        f.flavor.set(self.flavor)
        for tag in self.tags:
            f.tags.set(tag)
        return f

class Symlink(_File):
    """
    NAME
    ====

    B{C{Symlink}} - Define a symlink filestream helper.

    SYNOPSIS
    ========

    c{Symlink([I{target}] || [I{requires}, I{provides}, I{flavor}, I{owner}, I{group}, I{config}, I{tags}])}

    DESCRIPTION
    ===========

    The C{Symlink} class defines a symlink filestream helper.  I{target} is a
    mandatory argument for this class.

    PARAMETERS
    ==========
    The following parameters apply to the Symlink class.

    B{target}: The target of the symlink.  This parameter is a string.

    B{requires}: (None) Marks this file with the specified requirements.
    This parameter is a deps.Dependency object.

    B{provides}: (None) Marks files as providing certain features or
    characteristics.  This parameter is a deps.Dependency object.

    B{flavor}: (None) Marks this file with the specified flavor.  File
    flavors are aggregated to determine trove flavors.  This parameter
    is a deps.Flavor object.

    B{owner}: ('root') Marks this file as owned by I{owner}.  This parameter
    is a string

    B{group}: ('root') Marks this file as belonging to I{group}.  This
    parameter is a string

    B{tags} : (None) Tags associated with this file.  When a file with a
    tag is installed, removed, or changed, the listed tag handler is executed.
    See documentation on tag handlers for more information.  This parameter
    is a list of strings.

    USER COMMANDS
    =============
    The following user commands are applicable to C{Symlink}:

        - C{get(I{pathId})} : Returns a filestream with the settings
          represented by this class.

    EXAMPLES
    ========

    C{sym = Symlink('/foo/bar')}
    C{sym.get()}

    Creates a symlink filestream helper that points to /foo/bar, and
    retrieves the filestream associated with it.
    """
    fileClass = files.SymbolicLink
    kwargs = _File.kwargs.copy()
    del kwargs['perms']

    def __init__(self, target, **kwargs):
        _File.__init__(self, **kwargs)
        self.target = target
        self.perms = 0777

    def _touchupFileStream(self, f):
        f.target.set(self.target)

class RegularFile(_File):
    """
    NAME
    ====

    B{C{RegularFile}} - Define a regular file filestream helper.

    SYNOPSIS
    ========

    C{RegularFile([I{contents}, I{requires}, I{provides}, I{flavor}, I{mode}, I{owner}, I{group}, I{config}, I{initialContents}, I{transient}, I{tags}])}

    DESCRIPTION
    ===========

    The C{RegularFile} class defines a regular file filestream helper.  This
    class takes I{contents} as an optional argument.

    PARAMETERS
    ==========
    The following parameters apply to the RegularFile class.

    B{contents}: (None) defines the contents of the file.  This parameter can
    be a string or a file-like object.

    B{requires}: (None) Marks this file with the specified requirements.
    This parameter is a deps.Dependency object.

    B{provides}: (None) Marks files as providing certain features or
    characteristics.  This parameter is a deps.Dependency object.

    B{flavor}: (None) Marks this file with the specified flavor.  File
    flavors are aggregated to determine trove flavors.  This parameter
    is a deps.Flavor object.

    B{owner}: ('root') Marks this file as owned by I{owner}.  This parameter
    is a string

    B{group}: ('root') Marks this file as belonging to I{group}.  This
    parameter is a string

    B{mode}: (0644) Defines the access permissons of the file.  This
    parameter is an integer.

    B{config}: (False) Marks this file as a config file.  This is the same
    behavior as the Config Policy. A file marked as a config file cannot
    also be marked as a transient file or an initialContents file.  Conary
    enforces this requirement.

    B{initialContents}: (False) Marks this file as a initialContents file.

    B{transient}: (False) Marks this file as a transient file.

    B{tags} : (None) Tags associated with this File.  When a file with a
    tag is installed, removed, or changed, the listed tag handler is executed.
    See documentation on tag handlers for more information.  This parameter
    is a list of strings.

    USER COMMANDS
    =============
    The following user commands are applicable to C{RegularFile}:

        - C{get(I{pathId})} : Returns a filestream with the settings
          represented by this class.

        - C{getContents()} : Returns a seekable file object reflecting the
          contents associated with this filestream.

    EXAMPLES
    ========

    C{fileObj = RegularFile(contents = 'foo')}
    C{fileObj.get()}

    Creates a filestream helper with contents "foo", and retrieves the
    filestream associated with it.
    """
    fileClass = files.RegularFile
    needSha1 = True
    kwargs = _File.kwargs.copy()
    kwargs.update({'contents': '',
            'linkGroup': None,
            'config': False,
            'transient': False,
            'initialContents': False})

    def __init__(self, **kwargs):
        _File.__init__(self, **kwargs)
        if isinstance(self.contents, str):
            self.contents = filecontents.FromString(self.contents)
        else:
            self.contents = filecontents.FromFile(self.contents)

        self._enforceMutuallyExclusiveFlags()

    def getContents(self):
        return self.contents.get()

    def _touchupFileStream(self, f):
        self._enforceMutuallyExclusiveFlags()
        f.linkGroup.set(self.linkGroup)
        f.flags.isInitialContents(set = self.initialContents)
        f.flags.isConfig(set = self.config)
        f.flags.isTransient(set = self.transient)

    def _enforceMutuallyExclusiveFlags(self):
        flagCount = 0
        for flag in (self.config, self.transient, self.initialContents):
            flagCount += int(bool(flag))
        if flagCount > 1:
            raise ConflictingFlags("Config, transient and initialContents "
                    "are mutually exclusive")


class _Device(_File):
    """
    NAME
    ====

    B{C{_Device}} - Define a _device filestream helper.

    SYNOPSIS
    ========

    c{_Device([I{major}, I{minor}] || [I{contents}, I{requires}, I{provides}, I{flavor}, I{mode}, I{owner}, I{group}, I{tags}])}

    DESCRIPTION
    ===========

    The C{_Device} class defines a _device filestream helper.
    This class takes I{major} and I{minor} as mandatory arguments.

    PARAMETERS
    ==========
    The following parameters apply to the _Device class.

    B{major}: Sets the _device major number.  This parameter
    is an integer.

    B{minor}: Sets the _device minor number.  This parameter
    is an integer.

    B{requires}: (None) Marks this device with the specified requirements.
    This parameter is a deps.Dependency object.

    B{provides}: (None) Marks this device as providing certain features or
    characteristics.  This parameter is a deps.Dependency object.

    B{flavor}: (None) Marks this device with the specified flavor.  File
    flavors are aggregated to determine trove flavors.  This parameter
    is a deps.Flavor object.

    B{owner}: ('root') Marks this device as owned by I{owner}.  This parameter
    is a string

    B{group}: ('root') Marks this device as belonging to I{group}.  This
    parameter is a string

    B{mode}: (0644) Defines the access permissons of the device.  This
    parameter is an integer.

    B{tags} : (None) Tags associated with this device.  When a file with a
    tag is installed, removed, or changed, the listed tag handler is executed.
    See documentation on tag handlers for more information.  This parameter
    is a list of strings.

    USER COMMANDS
    =============
    The following user commands are applicable to C{_Device}:

        - C{get(I{pathId})} : Returns a filestream with the settings
          represented by this class.

    EXAMPLES
    ========

    C{dev = _Device(8, 1)}
    C{dev.get()}

    Creates a _device helper with major number 8 and minor number 1,
    and retrieves the filestream associated with it.
    """
    def __init__(self, major, minor, **kwargs):
        assert type(self) != _Device
        _File.__init__(self, **kwargs)
        self.major = major & 0xff
        self.minor = minor & 0xff

    def _touchupFileStream(self, f):
        f.devt.major.set(self.major)
        f.devt.minor.set(self.minor)

class BlockDevice(_Device):
    __doc__ = _Device.__doc__.replace('_Device', 'BlockDevice')
    __doc__ = __doc__.replace('_device', 'block device')
    fileClass = files.BlockDevice

class CharacterDevice(_Device):
    __doc__ = _Device.__doc__.replace('_Device', 'CharacterDevice')
    __doc__ = __doc__.replace('_device', 'character device')
    fileClass = files.CharacterDevice

class Directory(_File):
    """
    NAME
    ====

    B{C{Directory}} - Define a directory filestream helper.

    SYNOPSIS
    ========

    C{Directory([I{requires}, I{provides}, I{flavor}, I{mode}, I{owner}, I{group}, I{tags}])}

    DESCRIPTION
    ===========

    The C{Directory} class defines a directory filestream helper.

    PARAMETERS
    ==========
    The following parameters apply to the Directory class.

    B{requires}: (None) Marks this directory with the specified requirements.
    This parameter is a deps.Dependency object.

    B{provides}: (None) Marks this directory as providing certain features or
    characteristics.  This parameter is a deps.Dependency object.

    B{flavor}: (None) Marks this directory with the specified flavor.  File
    flavors are aggregated to determine trove flavors.  This parameter
    is a deps.Flavor object.

    B{owner}: ('root') Marks this directory as owned by I{owner}.
    This parameter is a string

    B{group}: ('root') Marks this directory as belonging to I{group}.  This
    parameter is a string

    B{mode}: (0644) Defines the access permissons of this directory.  This
    parameter is an integer.

    B{tags} : (None) Tags associated with this directory.  When a file with a
    tag is installed, removed, or changed, the listed tag handler is executed.
    See documentation on tag handlers for more information.  This parameter
    is a list of strings.

    USER COMMANDS
    =============
    The following user commands are applicable to C{Directory}:

        - C{get(I{pathId})} : Returns a filestream with the settings
          represented by this class.

    EXAMPLES
    ========

    C{dir = Directory(contents = 'foo')}
    C{dir.get()}

    Creates a directory filestream helper, and retrieves the filestream
    associated with it.
    """
    fileClass = files.Directory
    kwargs = _File.kwargs.copy()
    kwargs.update({'perms': 0755})

    def __init__(self, **kwargs):
        _File.__init__(self, **kwargs)

class Socket(_File):
    """
    NAME
    ====

    B{C{Socket}} - Define a unix domain socket filestream helper.

    SYNOPSIS
    ========

    C{Socket([I{requires}, I{provides}, I{flavor}, I{mode}, I{owner}, I{group}, I{tags}])}

    DESCRIPTION
    ===========

    The C{Socket} class defines a socket filestream helper.

    PARAMETERS
    ==========
    The following parameters apply to the Socket class.

    B{requires}: (None) Marks this socket with the specified requirements.
    This parameter is a deps.Dependency object.

    B{provides}: (None) Marks this socket as providing certain features or
    characteristics.  This parameter is a deps.Dependency object.

    B{flavor}: (None) Marks this socket with the specified flavor.  File
    flavors are aggregated to determine trove flavors.  This parameter
    is a deps.Flavor object.

    B{owner}: ('root') Marks this socket as owned by I{owner}.
    This parameter is a string

    B{group}: ('root') Marks this socket as belonging to I{group}.  This
    parameter is a string

    B{mode}: (0644) Defines the access permissons of this socket.  This
    parameter is an integer.

    B{tags} : (None) Tags associated with this socket.  When a file with a
    tag is installed, removed, or changed, the listed tag handler is executed.
    See documentation on tag handlers for more information.  This parameter
    is a list of strings.

    USER COMMANDS
    =============
    The following user commands are applicable to C{Socket}:

        - C{get(I{pathId})} : Returns a filestream with the settings
          represented by this class.

    EXAMPLES
    ========

    C{dir = Socket(contents = 'foo')}
    C{dir.get()}

    Creates a socket filestream helper, and retrieves the filestream
    associated with it.
    """
    fileClass = files.Socket
    kwargs = _File.kwargs.copy()
    kwargs.update({'perms': 0755})

    def __init__(self, **kwargs):
        _File.__init__(self, **kwargs)


class NamedPipe(_File):
    """
    NAME
    ====

    B{C{NamedPipe}} - Define a named pipe filestream helper.

    SYNOPSIS
    ========

    C{NamedPipe([I{requires}, I{provides}, I{flavor}, I{mode}, I{owner}, I{group}, I{tags}])}

    DESCRIPTION
    ===========

    The C{NamedPipe} class defines a named pipe filestream helper.

    PARAMETERS
    ==========
    The following parameters apply to the NamedPipe class.

    B{requires}: (None) Marks this named pipe with the specified requirements.
    This parameter is a deps.Dependency object.

    B{provides}: (None) Marks this named pipe as providing certain features or
    characteristics.  This parameter is a deps.Dependency object.

    B{flavor}: (None) Marks this named pipe with the specified flavor.  File
    flavors are aggregated to determine trove flavors.  This parameter
    is a deps.Flavor object.

    B{owner}: ('root') Marks this named pipe as owned by I{owner}.
    This parameter is a string

    B{group}: ('root') Marks this named pipe as belonging to I{group}.  This
    parameter is a string

    B{mode}: (0644) Defines the access permissons of this named pipe.  This
    parameter is an integer.

    B{tags} : (None) Tags associated with this named pipe.  When a file with a
    tag is installed, removed, or changed, the listed tag handler is executed.
    See documentation on tag handlers for more information.  This parameter
    is a list of strings.

    USER COMMANDS
    =============
    The following user commands are applicable to C{NamedPipe}:

        - C{get(I{pathId})} : Returns a filestream with the settings
          represented by this class.

    EXAMPLES
    ========

    C{dir = NamedPipe(contents = 'foo')}
    C{dir.get()}

    Creates a socket filestream helper, and retrieves the filestream
    associated with it.
    """
    fileClass = files.NamedPipe
    kwargs = _File.kwargs.copy()
    kwargs.update({'perms': 0755})

    def __init__(self, **kwargs):
        _File.__init__(self, **kwargs)
