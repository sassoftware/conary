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


# GNOME programs tend to need their menu help to function without apparant
# error, so it needs to be either in 'runtime' or 'data'.  It would otherwise
# show up in locale.  Most other systems include the default locale within
# the application, and need locale data only for translations.  This is
# therefore encoded as an exception to the general rule.

filters = ('runtime', ('%(datadir)s/gnome/help/.*/C/',))
follows = ('doc', 'supdoc')
precedes = ('locale', 'data')
