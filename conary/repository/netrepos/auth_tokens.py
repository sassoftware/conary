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

from conary.lib import util


class ValidPasswordTokenType(object):
    """
    Type of L{ValidPasswordToken}, a token used in lieu of a password in
    authToken to represent a user that has been authorized by other
    means (e.g. a one-time token).

    For example, a script that needs to perform some operation from a
    particular user's viewpoint, but has direct access to the database
    via a shim client, may use L{ValidPasswordToken} instead of a
    password in authToken to bypass password checks while still adhering
    to the user's own capabilities and limitations.

    This type should be instantiated exactly once (as
    L{ValidPasswordToken}).
    """
    __slots__ = ()

    def __str__(self):
        return '<Valid Password>'

    def __repr__(self):
        return 'ValidPasswordToken'
ValidPasswordToken = ValidPasswordTokenType()


class ValidUser(object):
    """
    Object used in lieu of a username in authToken to represent an imaginary
    user with a given set of roles.

    For example, a script that needs to perform a repository operation with a
    particular set of permissions, but has direct access to the database via
    a shim client, may use an instance of L{ValidUser} instead of a username
    in authToken to bypass username and password checks while still adhering
    to the limitations of the specified set of roles.

    The set of roles is given as a list containing role names, or integer
    roleIds. Mixing of names and IDs is allowed. Additionally, a role of '*'
    will entitle the user to all roles in the repository; if no arguments are
    given this is the default.
    """
    __slots__ = ('roles', 'username')

    def __init__(self, *roles, **kwargs):
        if not roles:
            roles = ['*']
        if isinstance(roles[0], (list, tuple)):
            roles = roles[0]
        self.roles = frozenset(roles)
        self.username = kwargs.pop('username', None)
        if kwargs:
            raise TypeError("Unexpected keyword argument %s" %
                    (kwargs.popitem()[0]))

    def __str__(self):
        if self.username:
            user_fmt = '%r ' % (self.username,)
        else:
            user_fmt = ''
        if '*' in self.roles:
            return '<User %swith all roles>' % (user_fmt,)
        else:
            return '<User %swith roles %s>' % (user_fmt,
                    ', '.join(unicode(x) for x in self.roles))

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, sorted(self.roles))

    def __reduce__(self):
        # Be pickleable, but don't actually pickle the object as it could
        # then cross a RPC boundary and become a security vulnerability. Plus,
        # it would confuse logcat.
        if self.username:
            return str, (str(self.username),)
        else:
            return str, (str(self),)


class _Accessor(object):

    def __init__(self, index, filter=None):
        self.index = index
        self.filter = filter.__name__ if filter else None

    def __get__(self, ownself, owncls):
        if ownself is None:
            return self
        return ownself[self.index]

    def __set__(self, ownself, value):
        if self.filter:
            value = getattr(ownself, self.filter)(value)
        ownself[self.index] = value


class AuthToken(list):
    __slots__ = ('flags',)
    _user, _password, _entitlements, _remote_ip, _forwarded_for = range(5)

    def __init__(self, user='anonymous', password='anonymous', entitlements=(),
            remote_ip=None, forwarded_for=None):
        list.__init__(self, [None] * 5)
        self.user = user
        self.password = password
        self.entitlements = list(entitlements)
        self.remote_ip = remote_ip
        self.forwarded_for = list(forwarded_for) if forwarded_for else []
        self.flags = None

    def _filter_password(self, password):
        if self.user == password == 'anonymous':
            return password
        elif password is ValidPasswordToken:
            return password
        else:
            return util.ProtectedString(password)

    user = _Accessor(_user)
    password = _Accessor(_password, _filter_password)
    entitlements = _Accessor(_entitlements)
    remote_ip = _Accessor(_remote_ip)
    forwarded_for = _Accessor(_forwarded_for)

    def __repr__(self):
        out = '<AuthToken'
        if self.user != 'anonymous' or not self.entitlements:
            out += ' user=%s' % (self.user,)
        if self.entitlements:
            ents = []
            for ent in self.entitlements:
                if isinstance(ent, (tuple, list)):
                    # Remove entitlement class
                    ent = ent[1]
                ents.append('%s...' % ent[:6])
            out += ' entitlements=[%s]' % (', '.join(ents))
        if self.remote_ip:
            out += ' remote_ip=%s' % self.remote_ip
        if self.forwarded_for:
            out += ' forwarded_for=%s' % (','.join(self.forwarded_for))
        return out + '>'

    def getAllIps(self):
        return set([self.remote_ip] + self.forwarded_for)
