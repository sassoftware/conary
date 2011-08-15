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


# implement decorators for tagging api calls in conary code

def apiDecorator(api_type):
    def _apiDecorator(func):
        lines = ['']
        marked = False
        if not func.__doc__:
            func.__doc__ = func.__name__
        lines = func.__doc__.split('\n')
        for idx, line in enumerate(lines):
            if '@' in line:
                l = line.replace('\t', '        ')
                l2 = line.lstrip()
                indent = len(l) - len(l2)
                lines.insert(idx, (' ' * indent) + '(%s)' % api_type)
                marked = True
                break

        if not marked:
            lines[0] = lines[0] + ' (%s)' % api_type

        try:
            func.__doc__ = '\n'.join(lines)
        except TypeError:
            # maybe a C function.
            pass
        return func
    return _apiDecorator

publicApi = apiDecorator('PUBLIC API')
developerApi = apiDecorator('DEVELOPER API')
