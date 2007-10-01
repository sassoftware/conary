#
# Copyright (c) 2007 rPath, Inc.
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

# Note that cil components have %(lib)s in them; we depend on the
# NonMultilibComponent policy to ensure that cil is multilib-safe.

filters = ('cil', ('%(prefix)s/(%(lib)s|lib)/(mono|[^/]*-sharp-[^/]*)/', ))
precedes = ('devellib', 'lib', 'devel')
