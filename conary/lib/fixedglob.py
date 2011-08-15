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


"""Filename globbing utility."""

import os
import fnmatch
import re
import errno

__all__ = ["glob"]

def glob(pathname):
    """Return a list of paths matching a pathname pattern.

    The pattern may contain simple shell-style wildcards a la fnmatch.

    """
    if not has_magic(pathname):
        return [pathname]
    dirname, basename = os.path.split(pathname)
    if not dirname:
        return glob1(os.curdir, basename)
    elif has_magic(dirname):
        list = glob(dirname)
    else:
        list = [dirname]
    if not has_magic(basename):
        result = []
        for dirname in list:
            if basename or os.path.isdir(dirname):
                name = os.path.join(dirname, basename)
                try:
                    os.lstat(name)
                except OSError, err:
                    # if the file does not exist, or if an element of the
                    # path to the file is not a directory, ignore the error
                    if err.errno != errno.ENOENT and err.errno != errno.ENOTDIR:
                        raise
                else:
                    result.append(name)
    else:
        result = []
        for dirname in list:
            sublist = glob1(dirname, basename)
            for name in sublist:
                result.append(os.path.join(dirname, name))
    return result

def glob1(dirname, pattern):
    if not dirname: dirname = os.curdir
    try:
        names = os.listdir(dirname)
    except os.error:
        return []
    if pattern[0]!='.':
        names=filter(lambda x: x[0]!='.',names)
    return fnmatch.filter(names,pattern)


magic_check = re.compile('[*?[]')

def has_magic(s):
    return magic_check.search(s) is not None
