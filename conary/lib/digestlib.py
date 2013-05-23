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
# pyflakes=ignore-file

"""
Formerly backwards compatibility for Python 2.4, now Python 2.6 is required so
this is just here to avoid breaking imports from other projects.
"""

from hashlib import sha1, md5, sha224, sha256, sha384, sha512

# import backwards compatible version of sha256 with calculation bug.
from conary.lib.ext.sha256_nonstandard import digest as sha256_nonstandard
