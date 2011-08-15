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


import struct

class _javaSymbolTable:
    def __init__(self):
        self.stringList = {}
        self.classRef = {}
        self.typeRef = {}

def _isValidTLD(refString):
    return "." in refString

def _parseSymbolTable(contents):
    if len(contents) <= 4 or contents[0:4] != "\xCA\xFE\xBA\xBE":
        raise ValueError, 'no java magic'
    poolSize = struct.unpack('>H', contents[8:10])[0]
    if not poolSize:
        raise ValueError, 'bad java file: no string pool'

    i = 10
    c = 1
    symbolTable = _javaSymbolTable()
    while c < poolSize:
        tag = struct.unpack('B', contents[i])[0]
        i += 1
        if tag == 1:
            # string
            length = struct.unpack('>H', contents[i:i+2])[0]
            i += 2
            symbolTable.stringList[c] = contents[i:i+length]
            i += length
        elif tag == 7:
            symbolTable.classRef[c] = struct.unpack('>H', contents[i:i+2])[0]
            i += 2
        elif tag == 12:
            symbolTable.typeRef[c] = struct.unpack('>H', contents[i+2:i+4])[0]
            i += 4
        elif tag == 8:
            i += 2
        elif tag == 3 or tag == 4 or tag == 9 or tag == 10 or tag == 11:
            i += 4
        elif tag == 5 or tag == 6:
            i += 8
            # double counts as two
            c += 1
        else:
            raise ValueError, 'unknown tag %d' %tag
        c += 1

    # get the className
    i += 2
    t = struct.unpack('>H', contents[i:i+2])[0]
    classID = symbolTable.classRef[t]
    className = symbolTable.stringList[classID]

    return symbolTable, className, i+2


def _parseRefs(refStr):
    rest = refStr
    s = set()
    while rest and 'L' in rest and ';' in rest:
        this, rest = rest.split('L', 1)[1].split(';', 1)
        if this:
            this = '.'.join(this.split('/'))
            s.add(this)
    return s


def getDeps(contents):
    try:
        symbolTable, className, offset = _parseSymbolTable(contents)
    except ValueError:
        return None, None

    reqSet = set()

    for referencedClassID in symbolTable.classRef.values():
        if referencedClassID not in symbolTable.stringList:
            continue
        refString = symbolTable.stringList[referencedClassID]
        if refString.startswith('['):
            if 'L' in refString:
                refString = refString[refString.index('L'):]
                # pull out all the references in this array
                reqSet.update((x for x in _parseRefs(refString)
                               if _isValidTLD(x)))
            # else ignore the array, nothing here for us to record
        else:
            parsedRef = '.'.join(refString.split('/'))
            if _isValidTLD(parsedRef):
                reqSet.add(parsedRef)

    for referencedTypeID in symbolTable.typeRef.values():
        if referencedTypeID in symbolTable.stringList:
            reqSet.update((x for x in
                           _parseRefs(symbolTable.stringList[referencedTypeID])
                           if _isValidTLD(x)))

    return '.'.join(className.split('/')), reqSet
