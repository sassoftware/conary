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


# This specification is one of the few that cares about file types;
# thus the use of stat.S_IFLNK to find symbolic links
# Like lib, devellib is architecture-specific; devel is architecture-neutral

import stat
filters = ('devellib', ((r'\.so', stat.S_IFLNK),
                        r'\.a',
                        r'\.la', # CNY-3143
                        '(%(libdir)s|%(datadir)s)/pkgconfig/'))
follows = ('perl', 'python')
precedes = ('lib', 'devel', 'data')
