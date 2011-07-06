#
# Copyright (c) 2011 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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
    words.pop()
    return ''.join(words)


def depFreeze(dep):
    words = []
    words.append(_escapeName(dep.name))
    for flag, sense in sorted(dep.flags.items()):
        words.append(':%s%s' % (_escapeFlags(flag), senseMap[sense]))
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
