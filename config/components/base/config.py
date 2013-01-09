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


# Implements the default :config component (CNY-172)

# :config component only if no executable bits set (CNY-1260)
filters = ('config', (('%(sysconfdir)s/', None, 0111),))
# taghandler and tagdescription must not be in :config (CNY-2256)
follows = ('taghandler',)
