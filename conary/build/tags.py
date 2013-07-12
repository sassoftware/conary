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


"""
Module implementing tag file handling
"""
import os

from conary.build import filter
from conary.lib.cfg import CfgEnum, CfgList, CfgString, ConfigFile, ParseError
from conary.lib.cfg import directive

EXCLUDE, INCLUDE = range(2)

class CfgImplementsItem(CfgEnum):
    validValueDict = {'files':   ('update', 'preremove', 'remove',
                                  'preupdate'),
                      'handler': ('update', 'preremove'),
                      'description':  ('update', 'preremove')}

    def __init__(self):
        validValues = []
        for fileType, actionList in self.validValueDict.iteritems():
            validValues.extend(' '.join((fileType, x)) for x in actionList)
        self.validValues = validValues
        CfgEnum.__init__(self)

    def checkEntry(self, val):
        if val.find(" ") < 0:
            raise ParseError, \
                'missing type/action in "implements %s"' %val
        CfgEnum.checkEntry(self, val)
        # XXX missing check for description here

CfgImplements = CfgList(CfgImplementsItem)


class CfgDataSource(CfgEnum):
    validValues = ['args', 'stdin', 'multitag' ]


class TagFile(ConfigFile):
    file              = CfgString
    name              = CfgString
    description       = CfgString
    datasource        = (CfgDataSource, 'args')
    implements        = CfgImplements

    def __init__(self, filename, macros = {}, warn=False):
        ConfigFile.__init__(self)

        self.tag = os.path.basename(filename)
        self.tagFile = filename
        self.macros = macros
        self.filterlist = []
        self.read(filename, exception=True)
        if 'implements' in self.__dict__:
            for item in self.__dict__['implements']:
                if item.find(" ") < 0:
                    raise ParseError, \
                        'missing type/action in "implements %s"' %item
                key, val = item.split(" ")
                # deal with self->handler protocol change
                if key == 'description':
                    if warn:
                        # at cook time
                        raise ParseError, \
                            'change "implements %s" to "implements handler" in %s' % (key, filename)
                    # throw this away
                    continue

    @directive
    def include(self, val):
        if not self.macros:
            return
        self.filterlist.append((INCLUDE, filter.Filter(val, self.macros)))

    @directive
    def exclude(self, val):
        if not self.macros:
            return
        self.filterlist.append((EXCLUDE, filter.Filter(val, self.macros)))

    def match(self, filename):
        for keytype, filter in self.filterlist:
            if filter.match(filename):
                if keytype == EXCLUDE:
                    return False
                else:
                    return True
        return False

def loadTagDict(dirPath):
    d = {}
    try:
        files = os.listdir(dirPath)
    except OSError:
        return {}

    for path in files:
        # ignore hidden files
        if path.startswith('.'):
            continue
        c = TagFile(os.path.join(dirPath, path))
        d[c.tag] = c

    return d
