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


from conary.lib import xmlparser

class SmartFormFieldParser(xmlparser.XmlParser):

    def __init__(self, s):
        self.name = None
        self.default = None
        xmlparser.XmlParser.__init__(self, s)
        self.parse(s)
        assert(self.name)

    def finishElement(self, parent, key, name, attrs, data):
        if name == 'name':
            self.name = data
        elif name == 'default':
            self.default = data
