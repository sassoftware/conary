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


# The lib component is only for architecture-specific files.  It should
# have no architecture-neutral files, in order to enable multilib
# support.

import stat
filters = ('lib', ((r'.*/(%(lib)s|lib)/', None, stat.S_IFDIR),))
follows = ('python',
           'perl',
           'data',
           'devellib',
           'devel',)
