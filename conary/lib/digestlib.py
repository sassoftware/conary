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
# pyflakes=ignore-file


"Compatibility module for python 2.4 - 2.6"

try:
    import hashlib
    sha1 = hashlib.sha1
    md5 = hashlib.md5
    sha256 = hashlib.sha256
except ImportError:
    import sha
    import md5
    from Crypto.Hash import SHA256
    sha1 = sha.new
    md5 = md5.new
    sha256 = SHA256.new




# Bug check - a version of sha256 we used to distribute miscalculated sha256
# for strings of length 55 (mod 64).  If we're using such a version, then
# we don't have a standard sha256 implementation.
isStandardSha256 = sha256('\0' * 55).hexdigest() == '02779466cdec163811d078815c633f21901413081449002f24aa3e80f0b88ef7'
if not isStandardSha256:
    sha256 = None
del isStandardSha256

# import backwards compatible version of sha256 with this calculation bug.
from conary.lib.ext.sha256_nonstandard import digest as sha256_nonstandard
