# Copyright (c) 2008 rPath, Inc.
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

# implement decorators for tagging api calls in conary code

def publicApi(func):
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
            lines.insert(idx, (' ' * indent) + '(PUBLIC API)')
            marked = True
            break

    if not marked:
        lines[0] = lines[0] + ' (PUBLIC API)'

    func.__doc__ = '\n'.join(lines)
    return func

