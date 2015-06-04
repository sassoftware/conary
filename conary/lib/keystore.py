#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


try:
    import keyutils as _keyutils
    _keyring = _keyutils.KEY_SPEC_USER_KEYRING
except ImportError:
    _keyutils = None


def _setupSession():
    """
    Link the user keyring into the session keyring. pam_keyinit.so normally
    does this by default, but do it again in case someone made a new session or
    the session was revoked.

    We want to add keys to the user keyring so that they are reachable across
    sessions, and continue to work after the session is revoked due to e.g.
    detaching from screen and closing SSH. But keys are created with
    permissions that require "possession" to read them, where possession means
    the key is reachable via the session keyring. If the session keyring is
    empty, adding a key to any other keyring means that key is not under
    possession and thus not readable even though we own it.

    The fix is to ensure the user keyring is always linked under the session
    keyring so that keys in the user keyring are reachable via the session
    keyring.
    """
    global _keyring
    try:
        _keyutils.link(_keyutils.KEY_SPEC_USER_KEYRING,
                _keyutils.KEY_SPEC_SESSION_KEYRING)
    except AttributeError:
        # Old keyutils. Downgrade to just using the session keyring directly,
        # because there's no way to look in the user keyring.
        _keyring = _keyutils.KEY_SPEC_SESSION_KEYRING
        return
    except _keyutils.Error as err:
        if err.args[0] != _keyutils.EKEYREVOKED:
            raise
        # Session key was revoked. Make a new session and try again.
        _keyutils.join_session_keyring()
        _keyutils.link(_keyutils.KEY_SPEC_USER_KEYRING,
                _keyutils.KEY_SPEC_SESSION_KEYRING)


def getPassword(keyDesc):
    if not _keyutils:
        return None
    _setupSession()
    try:
        keyId = _keyutils.request_key(keyDesc, _keyring)
    except _keyutils.Error as err:
        if err.args[0] != _keyutils.EKEYREVOKED:
            raise
        # This happens if using old keyutils if the session was revoked (which
        # normally _setupSession would fix), or using new keyutils if the key
        # itself was revoked.
        return None
    if keyId is not None:
        return _keyutils.read_key(keyId)
    return None


def setPassword(keyDesc, passwd):
    if not _keyutils:
        return passwd
    _setupSession()
    try:
        _keyutils.add_key(keyDesc, passwd, _keyring)
    except _keyutils.Error as err:
        if err.args[0] != _keyutils.EKEYREVOKED:
            raise
        # This should only happen if using old keyutils.
    return passwd


def invalidatePassword(keyDesc):
    if not _keyutils:
        return
    try:
        keyId = _keyutils.search(_keyring, keyDesc)
        _keyutils.revoke(keyId)
    except AttributeError:
        # Old keyutils, oh well
        return
    except _keyutils.Error as err:
        if err.args[0] != _keyutils.EKEYREVOKED:
            raise
        # Close enough
