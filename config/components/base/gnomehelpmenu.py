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


# GNOME programs tend to need their menu help to function without apparant
# error, so it needs to be either in 'runtime' or 'data'.  It would otherwise
# show up in locale.  Most other systems include the default locale within
# the application, and need locale data only for translations.  This is
# therefore encoded as an exception to the general rule.

filters = ('runtime', ('%(datadir)s/gnome/help/.*/C/',))
follows = ('doc', 'supdoc')
precedes = ('locale', 'data')
