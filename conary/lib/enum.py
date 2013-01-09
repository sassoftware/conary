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


class EnumeratedType(dict):

    def __getattr__(self, item):
        if self.has_key(item):
            return self[item]
        raise AttributeError, "'EnumeratedType' object has no " \
                    "attribute '%s'" % item

    def __init__(self, name, *vals):
        for item in vals:
            self[item] = "%s-%s" % (name, item)
