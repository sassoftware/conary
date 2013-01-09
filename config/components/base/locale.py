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


# The locale component is for additional locale data that is not required
# to run a program in its default locale, but is required to get translations
# for alternate locales.

filters = ('locale', ('%(datadir)s/locale/',
                      '%(datadir)s/gnome/help/.*/'))
follows = ('gnomehelpmenu',)
precedes = ('data',)
