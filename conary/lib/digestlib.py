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


"Compatibility module for python 2.4 - 2.6"

try:
    import hashlib
    sha1 = hashlib.sha1
    md5 = hashlib.md5
    sha224 = hashlib.sha224
    sha256 = hashlib.sha256
    sha384 = hashlib.sha384
    sha512 = hashlib.sha512
except ImportError:
    import sha
    import md5
    from Crypto.Hash import SHA256
    sha1 = sha.new
    md5 = md5.new
    sha256 = SHA256.new
    sha224 = sha384 = sha512 = None




# Bug check - a version of sha256 we used to distribute miscalculated sha256
# for strings of length 55 (mod 64).  If we're using such a version, then
# we don't have a standard sha256 implementation.
isStandardSha256 = sha256('\0' * 55).hexdigest() == '02779466cdec163811d078815c633f21901413081449002f24aa3e80f0b88ef7'
if not isStandardSha256:
    sha256 = None
del isStandardSha256

# import backwards compatible version of sha256 with this calculation bug.
from conary.lib.ext.sha256_nonstandard import digest as sha256_nonstandard
