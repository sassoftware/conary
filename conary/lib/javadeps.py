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

# The Java class file format is described in JSR-202: https://www.jcp.org/en/jsr/detail?id=202

import struct
from collections import namedtuple

InnerClass = namedtuple("InnerClass", "innerClassName outerClassName innerName innerClassAccessFlags")

GenericInfo = namedtuple("GenericInfo", "accessFlags name descriptor attributes")

class ACCESS(object):
    PUBLIC = 0x0001
    PRIVATE = 0x0002
    PROTECTED = 0x0004

class _javaSymbolTable:
    def __init__(self):
        self.accessFlags = 0
        self.classNameIndex = None
        self.superClassNameIndex = None
        self.floatList = {}
        self.stringList = {}
        self.classRef = {}
        self.typeRef = {}
        self.fieldRef = {}
        self.methodRef = {}
        self.interfaceMethodRef = {}
        self.intList = {}
        self.longList = {}
        self.doubleList = {}
        self.stringRef = {}
        self._fields = []
        self._methods = []
        self._interfaceMethods = []

    @classmethod
    def _asUtf8(cls, contents, i):
        length = struct.unpack('>H', contents[i:i+2])[0]
        data = contents[i+2:i+2+length]
        # If we cared, we'd uncomment the next line
        #return data.decode('utf-8'), i+2+length
        return data, i+2+length

    @classmethod
    def _asClassRef(cls, contents, i):
        return struct.unpack('>H', contents[i:i+2])[0], i+2

    @classmethod
    def _asTypeRef(cls, contents, i):
        return struct.unpack('>HH', contents[i:i+4]), i+4

    @classmethod
    def _asCommonInfo(cls, contents, i):
        classIndex, nameAndType = struct.unpack('>HH', contents[i:i+4])
        return (classIndex, nameAndType), i+4

    @classmethod
    def _asInt(cls, contents, i):
        return struct.unpack('>I', contents[i:i+4]), i+4

    @classmethod
    def _asFloat(cls, contents, i):
        return struct.unpack('>f', contents[i:i+4]), i+4

    @classmethod
    def _asDouble(cls, contents, i):
        return struct.unpack('>d', contents[i:i+8]), i+8

    @classmethod
    def _asLong(cls, contents, i):
        return struct.unpack('>Q', contents[i:i+8]), i+8

    _tagToField = {
            1 : ('stringList', '_asUtf8', 1),
            3 : ('intList', '_asInt', 1),
            4 : ('floatList', '_asFloat', 1),
            5 : ('longList', '_asLong', 2),
            6 : ('doubleList', '_asDouble', 2),
            7 : ('classRef', '_asClassRef', 1),
            8 : ('stringRef', '_asClassRef', 1),
            9 : ('fieldRef', '_asCommonInfo', 1),
            10 : ('methodRef', '_asCommonInfo', 1),
            11 : ('interfaceMethodRef', '_asCommonInfo', 1),
            12 : ('typeRef', '_asTypeRef', 1),
            }

    def process(self, tag, contents, poolPos, i):
        if tag not in self._tagToField:
            raise ValueError, 'unknown tag %d' %tag
        storage, methodName, incr = self._tagToField[tag]
        field = getattr(self, storage)
        method = getattr(self, methodName)
        data, i = method(contents, i)
        field[poolPos] = data
        poolPos += incr
        return poolPos, i

    @property
    def className(self):
        return self.stringList[self.classRef[self.classNameIndex]]

    @property
    def superClassName(self):
        return self.stringList[self.classRef[self.superClassNameIndex]]

    @property
    def innerClasses(self):
        for attrName, attrVal in self.iterAttributes():
            if attrName == 'InnerClasses':
                return attrVal
        return None

    def iterAttributes(self):
        return self._attribIterator(self._attributes)

    def iterFields(self):
        return self._dataInfoIterator(self._fields)

    def iterMethods(self):
        return self._dataInfoIterator(self._methods)

    def _readAttribute(self, contents, i):
        attrNameIndex, attrLength = struct.unpack('>HI', contents[i:i+6])
        i += 6
        attrName = self.stringList[attrNameIndex]
        if attrName == 'Code':
            # Code can be long, and we don't need it
            attrInfo = ''
        else:
            attrInfo = contents[i:i+attrLength]
        i += attrLength
        return (attrNameIndex, attrInfo), i

    def _attribIterator(self, attribList):
        for attrNameIndex, attrInfo in attribList:
            yield self._attrib(attrNameIndex, attrInfo)

    def _attrib(self, attrNameIndex, attrInfo):
        attrName = self.stringList[attrNameIndex]
        if attrName == 'InnerClasses':
            innerClasses = []
            classCount = struct.unpack('>H', attrInfo[:2])[0]
            for j in range(classCount):
                idx = 2 + j * 8
                tup = struct.unpack('>HHHH', attrInfo[idx:idx+8])
                iClsInfoIdx, oClsInfoIdx, iNameIdx, iAccessFlags = tup
                if iClsInfoIdx != 0:
                    iClsName = self.stringList[self.classRef[iClsInfoIdx]]
                else:
                    iClsName = None
                if oClsInfoIdx != 0:
                    oClsName = self.stringList[self.classRef[oClsInfoIdx]]
                else:
                    oClsName = None
                if iNameIdx != 0:
                    iName = self.stringList[iNameIdx]
                else:
                    iName = None
                innerClasses.append(InnerClass(iClsName, oClsName, iName,
                        iAccessFlags))

            attrInfo = innerClasses
        elif attrName == 'SourceFile':
            attrInfo = self.stringList[struct.unpack('>H', attrInfo)[0]]
        return attrName, attrInfo

    def _readArrayOfAttributes(self, contents, i):
        attribs = []
        count = struct.unpack('>H', contents[i:i+2])[0]
        i += 2
        for _ in range(count):
            (attrNameIndex, attrInfo), i = self._readAttribute(contents, i)
            attribs.append((attrNameIndex, attrInfo))
        return attribs, i

    def _dataInfoIterator(self, objList):
        for accessFlags, nameIndex, descriptorIndex, attribs in objList:
            yield GenericInfo(accessFlags,
                    self.stringList[nameIndex],
                    self.stringList[descriptorIndex],
                    list(self._attribIterator(attribs)))

    def _readArrayOfDataInfo(self, contents, i):
        ret = []
        count = struct.unpack('>H', contents[i:i+2])[0]
        i += 2
        for _ in range(count):
            accessFlags, nameIndex, descriptorIndex = struct.unpack('>HHH',
                    contents[i:i+6])
            i += 6
            attribs, i = self._readArrayOfAttributes(contents, i)
            ret.append((accessFlags, nameIndex, descriptorIndex, attribs))
        return ret, i

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
        c, i = symbolTable.process(tag, contents, c, i)

    symbolTable.accessFlags = struct.unpack('>H', contents[i:i+2])[0]
    i += 2

    symbolTable.classNameIndex = struct.unpack('>H', contents[i:i+2])[0]
    i += 2

    symbolTable.superClassNameIndex = struct.unpack('>H', contents[i:i+2])[0]
    i += 2

    interfacesCount = struct.unpack('>H', contents[i:i+2])[0]
    i += 2
    # Skip over interfaces
    i += 2 * interfacesCount

    symbolTable._fields, i = symbolTable._readArrayOfDataInfo(contents, i)
    symbolTable._methods, i = symbolTable._readArrayOfDataInfo(contents, i)
    symbolTable._attributes, i = symbolTable._readArrayOfAttributes(contents, i)

    #print "Class Name", symbolTable.className
    #print "Superclass Name", symbolTable.superClassName
    #print "Fields", list(symbolTable.iterFields())
    #print "Methods", list(symbolTable.iterMethods())
    #print "Attributes", list(symbolTable.iterAttributes())
    #print "Access Flags", symbolTable.accessFlags
    return symbolTable, i


def _parseRefs(refStr):
    rest = refStr
    s = set()
    while rest and 'L' in rest and ';' in rest:
        this, rest = rest.split('L', 1)[1].split(';', 1)
        if this:
            s.add(this.replace('/', '.'))
    return s

def _isAnonymousInnerClass(className):
    # This also catches $1MethodScopedInner
    parts = iter(className.split('$'))
    parts.next()
    for part in parts:
        if part[0].isdigit():
            return True
    return False

def getDeps(contents):
    try:
        symbolTable, offset = _parseSymbolTable(contents)
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
            parsedRef = refString.replace('/', '.')
            if _isValidTLD(parsedRef):
                reqSet.add(parsedRef)

    for nameID, referencedTypeID in symbolTable.typeRef.values():
        if referencedTypeID in symbolTable.stringList:
            reqSet.update((x for x in
                           _parseRefs(symbolTable.stringList[referencedTypeID])
                           if _isValidTLD(x)))

    reqSet = set(x for x in reqSet if not _isAnonymousInnerClass(x))

    innerClassesMap = dict(
            ((x.outerClassName.replace('/', '.'), x.innerName), x)
            for x in (symbolTable.innerClasses or [])
            if x.innerName is not None
                and x.outerClassName is not None)
    privateInnerClasses = set()
    for (outerClassName, innerName), obj in sorted(innerClassesMap.items()):
        className = "%s$%s" % (outerClassName, innerName)
        # If the parent class is private, this one is too
        if (outerClassName in privateInnerClasses or
                (obj.innerClassAccessFlags & ACCESS.PRIVATE) == ACCESS.PRIVATE):
            privateInnerClasses.add(className)

    className = symbolTable.className.replace('/', '.')
    parts = className.split('$')
    partsIter = iter(parts)
    outerClassName = partsIter.next()
    for innerName in partsIter:
        if innerName[0].isdigit():
            # Anonymous inner class
            className = None
            break
        outerClassName = "%s$%s" % (outerClassName, innerName)
        if outerClassName in privateInnerClasses:
            className = None
            break
    return className, reqSet - privateInnerClasses

if __name__ == '__main__':
    import sys
    print getDeps(file(sys.argv[1]).read())
