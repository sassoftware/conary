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

try:
    import keyutils as _keyutils
    _keyring = _keyutils.KEY_SPEC_SESSION_KEYRING
except ImportError:
    _keyutils = _keyring = None


def getPassword(keyDesc):
    if _keyutils:
        keyId = _keyutils.request_key(keyDesc, _keyring)
        if keyId is not None:
            return _keyutils.read_key(keyId)
    return None


def setPassword(keyDesc, passwd):
    if _keyutils:
        _keyutils.add_key(keyDesc, passwd, _keyring)
    return passwd
