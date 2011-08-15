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


# The lib component is only for architecture-specific files.  It should
# have no architecture-neutral files, in order to enable multilib
# support.

import stat
filters = ('lib', ((r'.*/(%(lib)s|lib)/', None, stat.S_IFDIR),))
follows = ('python',
           'perl',
           'data',
           'devellib',
           'devel',)
