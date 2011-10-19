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

"""
NOTE: DO NOT USE unless you need an implementation of sha256 which has an
implementation error when the length of the string being hashed is 55 % 64.
See http://pycrypto.cvs.sourceforge.net/viewvc/pycrypto/crypto/src/SHA256.c?r1=1.3&r2=1.4
for fix to this bug.  In this case, we want to keep the bw compatible bug for
creating older signatures.

On top of that, this module is provided only for backwards compatibility with
existing code (rAPA specifically) that not only relied on the broken digest but
also the particular interface by which Conary provided it.
"""

from conary.lib.ext.sha256_nonstandard import digest as _digest


class SHA256_nonstandard(object):
    name = 'sha256_nonstandard'
    digest_size = digestsize = 32
    block_size = 64

    def __init__(self, msg=''):
        self.msg = msg

    def update(self, msg):
        self.msg += msg

    def digest(self):
        return _digest(self.msg)

    def hexdigest(self):
        return _digest(self.msg).encode('hex')

    def copy(self):
        return type(self)(self.msg)


new = SHA256_nonstandard
blocksize = new.block_size
digest_size = new.digest_size
