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

# The locale component is for additional locale data that is not required
# to run a program in its default locale, but is required to get translations
# for alternate locales.

filters = ('locale', ('%(datadir)s/locale/',
                      '%(datadir)s/gnome/help/.*/'))
follows = ('gnomehelpmenu',)
precedes = ('data',)
