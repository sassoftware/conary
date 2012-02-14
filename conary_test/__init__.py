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


from testrunner import testhelp


def rpm(func):
    # mark the context as rpm
    testhelp.context('rpm')(func)

    def run(*args, **kwargs):
        try:
            __import__('rpm')
        except ImportError:
            raise testhelp.SkipTestException('RPM module not present')
        else:
            return func(*args, **kwargs)

    run.func_name = func.func_name
    run._contexts = func._contexts

    return run
