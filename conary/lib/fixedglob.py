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
