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


senseMap = {1: '', 2: '~', 3: '~!', 4: '!'}


def _escapeName(name):
    return name.replace(':', '::')


def _escapeFlags(name):
    return name.replace(':', '\:')


def depSetFreeze(members):
    words = []
    for tag, depClass in sorted(members.items()):
        for dep in depClass.getDeps():
            words.append('%d#' % tag)
            words.extend(depFreeze(dep))
            words.append('|')
    if words:
        # Pop trailing pipe character
        words.pop()
    return ''.join(words)


def depFreeze(dep):
    words = []
    words.append(_escapeName(dep.name))
    for flag, sense in sorted(dep.flags.items()):
        words.append(':%s%s' % (senseMap[sense], _escapeFlags(flag)))
    return words


def depSetSplit(offset, data):
    data = data[offset:]
    end = data.find('|')
    if end < 0:
        end = len(data)
    data = data[:end]
    tag = data.find('#')
    if tag < 0:
        raise ValueError("invalid frozen dependency")
    tag, frozen = data[:tag], data[tag + 1:]
    next = offset + end + 1
    return next, int(tag), frozen


def depSplit(frozen):
    frozen = frozen.replace('::', '\1').replace('\\:', '\1')
    a = frozen.find(':')
    if a < 0:
        a = len(frozen)
    name, flags = frozen[:a], frozen[a+1:]
    name = name.replace('\1', ':')
    if flags:
        flagList = flags.split(':')
        flagList = [x.replace('\1', ':').replace('\\', '') for x in flagList]
    else:
        flagList = []
    return name, flagList
