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

# GNOME programs tend to need their menu help to function without apparant
# error, so it needs to be either in 'runtime' or 'data'.  It would otherwise
# show up in locale.  Most other systems include the default locale within
# the application, and need locale data only for translations.  This is
# therefore encoded as an exception to the general rule.

filters = ('runtime', ('%(datadir)s/gnome/help/.*/C/',))
follows = ('doc', 'supdoc')
precedes = ('locale', 'data')
