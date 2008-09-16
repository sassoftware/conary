#
# Copyright (c) 2008 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Contains functions to assist in dealing with deb files.
"""

class ControlFileParser(object):
    "Parse a Debian control file"
    __slots__ = [ '_prevVal', ]

    def __init__(self):
        self._prevVal = None

    def parse(self, iterable):
        """
        Yields tuples (key, valueLines)
        """
        for row in iterable:
            row = row.rstrip('\n')
            if row.startswith(' '):
                # Part of a multi-line value.
                # If no previous value was found, ignore this line
                if self._prevVal is None:
                    print "Ignoring"
                    continue
                if row == ' .':
                    # New paragraph
                    self._prevVal[1].append('')
                else:
                    self._prevVal[1].append(row[1:])
                continue
            if self._prevVal is not None:
                key, val = self._prevVal
                self._prevVal = None
                yield (key, val)
            arr = row.split(':', 1)
            if len(arr) != 2:
                # Malformed line (no :)
                continue
            key, val = arr
            val = val.lstrip(' ')
            self._prevVal = (key, [ val ])
        if self._prevVal is not None:
            yield self._prevVal
            self._prevVal = None

