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

# generic, unstructured "supplemental documentation" (thus "supdoc")
# has tended to be about an order of magnitude larger than structured
# documentation as included in the "doc" component, but is less
# widely useful (CNY-883)

filters = ('supdoc', ('%(datadir)s/doc/',))
follows = ('develdocs',)
precedes = ('data',)