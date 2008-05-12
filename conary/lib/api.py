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
    msg = ""
    if func.__doc__:
        msg = "\n\n" + func.__doc__
    if func.func_name == '__init__':
        # XXX: should decorating a class' init() method mean anything?
        pass
    func.__doc__ = 'PUBLIC API' + msg
    return func

