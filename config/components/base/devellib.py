#
# Copyright (c) 2004-2006 rPath, Inc.
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

# This specification is one of the few that cares about file types;
# thus the use of stat.S_IFLNK to find symbolic links
# Like lib, devellib is architecture-specific; devel is architecture-neutral

import stat
filters = ('devellib', ((r'\.so', stat.S_IFLNK),
                        r'\.a',
                        '(%(libdir)s|%(datadir)s)/pkgconfig/'))
follows = ('perl', 'python')
precedes = ('lib', 'devel', 'data')
