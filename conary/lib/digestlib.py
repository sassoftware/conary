#
# Copyright (c) 2004-2009 rPath, Inc.
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
from conary.lib import SHA256_nonstandard
sha256_nonstandard = SHA256_nonstandard.new
