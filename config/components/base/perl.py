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

# Note that perl components have %(lib)s in them; we depend on the
# NonMultilibComponent policy to ensure that perl is multilib-safe.

filters = ('perl', ('/usr/(%(lib)s|lib)/perl.*/(vendor|site)_perl/',))
precedes = ('devellib', 'lib', 'devel')
