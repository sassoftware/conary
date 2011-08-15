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


"""
Filters are slightly extended regular expressions with mode bit masking.
The extended regular expressions are applied to filenames, and the
mode bit masks (positive and negative) are applied to the mode of the
file as it appears in the filesystem.
"""

import os
import re
import stat

from conary.lib import log

class Filter:
    """
    Determine whether a path meets a set of constraints.  FileFilter
    acts like a regular expression, except that besides matching
    the name, it can also test against file metadata.
    """
    def __init__(self, regex, macros, setmode=None, unsetmode=None, name=None, rootdir=None):
        """
        Provide information to match against.
        @param regex: regular expression(s) to match against pathnames
        @type regex: string, list of strings, or compiled regular expression;
        strings or lists of strings will have macros interpolated.
        @param macros: current recipe macros
        @param setmode: bitmask containing bits that must be set
        for a match
        @type setmode: integer
        @param unsetmode: bitmask containing bits that must be unset
        for a match
        @type unsetmode: integer
        @param name: name of package or component
        @type name: string

        The setmode and unsetmode masks should be constructed from
        C{stat.S_IFDIR}, C{stat.S_IFCHR}, C{stat.S_IFBLK}, C{stat.S_IFREG},
        C{stat.S_IFIFO}, C{stat.S_IFLNK}, and C{stat.S_IFSOCK}
        Note that these are not simple bitfields.  To specify
        ``no symlinks'' in unsetmask you need to provide
        C{stat.S_IFLNK^stat.S_IFREG}.
        To specify only character devices in setmask, you need
        C{stat.S_IFCHR^stat.SBLK}.
        Here are the binary bitmasks for the flags::
            S_IFDIR  = 0100000000000000
            S_IFCHR  = 0010000000000000
            S_IFBLK  = 0110000000000000
            S_IFREG  = 1000000000000000
            S_IFIFO  = 0001000000000000
            S_IFLNK  = 1010000000000000
            S_IFSOCK = 1100000000000000
        """
        if name:
            self.name = name
        if rootdir is None:
            self.rootdir = macros['destdir']
        else:
            self.rootdir = rootdir
        self.setmode = setmode
        self.unsetmode = unsetmode
        tmplist = []
        if callable(regex):
            regex = regex()
        if type(regex) is str:
            try:
                self.regexp = self._anchor(regex %macros)
            except ValueError, msg:
                log.error('invalid macro substitution in "%s", missing "s"?' %regex)
                raise
            self.re = re.compile(self.regexp)
        elif type(regex) in (tuple, list):
            for subre in regex:
                try:
                    subre = self._anchor(subre %macros)
                except ValueError, msg:
                    log.error('invalid macro substitution in "%s", missing "s"?' %subre)
                    raise
                tmplist.append('(?:' + subre + ')')
            self.regexp = '|'.join(tmplist)
            self.re = re.compile(self.regexp)
        else:
            self.re = regex

    def _anchor(self, regex):
        """
        Make regular expressions be anchored "naturally" for pathnames.
        paths starting in / are anchored at the beginning of the string;
        paths ending in anything other than / are anchored at the end.
        Use .* to override this: .*/ at the beginning, or foo.* at the
        end.
        """
        if regex[:1] == '/':
            regex = '^' + regex
        if regex[-1:] != '/' and regex[-1:] != '$':
            regex = regex + '$'
        return regex

    def match(self, path, mode=None):
        """
        Compare a path to the constraints
        @param path: The string that should match the regex
        @param mode: optional parameter used when the path is not on
        disk (e.g., if a device node is being created virtually)
        """
        # search instead of match in order to not automatically
        # front-anchor searches
        match = self.re.search(path)
        if match:
            if self.setmode or self.unsetmode:
                if not mode:
                    if path[0] == '/':
                        mode = os.lstat(self.rootdir + path)[stat.ST_MODE]
                    else:
                        mode = os.lstat(os.path.join(self.rootdir,path))[stat.ST_MODE]
                if self.setmode is not None:
                    # if some bit in setmode is not set in mode, no match
                    if (self.setmode & mode) != self.setmode:
                        return 0
                if self.unsetmode is not None:
                    # if some bit in unsetmode is set in mode, no match
                    if self.unsetmode & mode:
                        return 0
            return 1

        return 0


class PathSet(object):
    '''
    This class implements an interface sufficiently similar to
    a regular expression object to use for filters, but looks up
    strings in a set of matches rather than compiling a regular
    expression.  This is used when the matches are generated from
    specific files in the filesystem, and is not made directly
    available in recipes.  It will be generally faster than using
    a regular expression, and some versions of python are built
    with limitations on the complexity of regular expressions that
    raise OverflowError for regular expressions of complexity seen
    in real packages.
    '''
    slots = [ 'name', '_set' ]
    def __init__(self, *args, **kwargs):
        name = kwargs.pop('name', None)
        self.name = name
        self._set = set(*args)
    def match(self, string, mode=None):
        return string in self._set
    search = match
    def __call__(self):
        return self
