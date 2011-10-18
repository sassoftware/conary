#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
