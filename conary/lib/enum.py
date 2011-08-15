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


class EnumeratedType(dict):

    def __getattr__(self, item):
        if self.has_key(item):
            return self[item]
        raise AttributeError, "'EnumeratedType' object has no " \
                    "attribute '%s'" % item

    def __init__(self, name, *vals):
        for item in vals:
            self[item] = "%s-%s" % (name, item)
