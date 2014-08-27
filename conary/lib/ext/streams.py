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


import struct
from conary.lib.compat import namedtuple


SMALL               = 0
LARGE               = 1
DYNAMIC             = 2

FAIL_UNKNOWN        = 0
SKIP_UNKNOWN        = 1
PRESERVE_UNKNOWN    = 2


class _BaseStream(object):
    pass


class _ValueStream(_BaseStream):
    _valueDefault = None

    def __init__(self, value=None):
        if value is None:
            self._value = self._valueDefault
        elif type(value) is str:
            # This catches StringStream values, but conveniently its frozen
            # form is identical to its value.
            self.thaw(value)
        else:
            self.set(value)

    def __call__(self):
        return self._value

    def __eq__(self, other):
        return self._value == other._value
    def __ne__(self, other):
        return self._value != other._value
    def __cmp__(self, other):
        if type(self) != type(other):
            raise TypeError("invalid type")
        return cmp(self._value, other._value)

    def __hash__(self):
        return hash(self._value)

    def set(self, value):
        raise NotImplementedError

    def freeze(self, skipSet=None):
        return self._value

    def thaw(self, frozen):
        self.set(frozen)

    def diff(self, other):
        if other is None:
            return None
        elif type(self) != type(other):
            raise TypeError("invalid type")
        elif self._value == other._value:
            # same as other, no diff
            return None
        else:
            # different from other, diff is the entire value
            return self.freeze()

    def twm(self, diff, other):
        if type(self) != type(other):
            raise TypeError("invalid type")
        if self._value == other._value:
            # same as other, keep the diff
            self.thaw(diff)
            return False
        elif self._value == diff:
            # same as diff, keep self
            return False
        else:
            # three different values, conflict
            return True


class StringStream(_ValueStream):
    _valueDefault = ''

    def set(self, value):
        if value is None:
            # Technically this isn't compatible because cstreams would return
            # None on get later, but when freezing None is treated the same as
            # empty string by containers.
            value = ''
        elif not isinstance(value, str):
            raise TypeError("invalid type '%s' for string stream" %
                    type(value).__name__)
        self._value = value


class _NumericStream(_ValueStream):
    _valueDefault = None
    _intFormat = None

    def set(self, value):
        if isinstance(value, float):
            value = int(value)
        elif not isinstance(value, (int, long)) and value is not None:
            raise TypeError("invalid type '%s' for numeric stream" %
                    type(value).__name__)
        self._value = value


    def freeze(self, skipSet=None):
        if self._value is None:
            return ''
        else:
            return struct.pack(self._intFormat, self._value)

    def thaw(self, frozen):
        if frozen:
            value = struct.unpack(self._intFormat, frozen)[0]
        else:
            value = None
        self.set(value)


class ByteStream(_NumericStream):
    _intFormat = '>B'


class ShortStream(_NumericStream):
    _intFormat = '>h'


class IntStream(_NumericStream):
    _intFormat = '>i'


class LongLongStream(_NumericStream):
    _intFormat = '>Q'


_tagInfo = namedtuple('_tagInfo', 'tag sizeType type name')


class StreamSet(_BaseStream):
    streamDict = None
    ignoreUnknown = FAIL_UNKNOWN

    def __init__(self, data=None, offset=0):
        self._unknownTags = []
        for tag in self._getTags():
            setattr(self, tag.name, tag.type())
        if data is not None:
            self.thaw(data[offset:])

    @classmethod
    def _getTags(cls):
        # Cache stream set definition in a class variable, but look only
        # exactly in the current class, never a parent class.
        tags = cls.__dict__.get('_streamTags', None)
        if not tags:
            if not cls.streamDict:
                raise ValueError(
                        "%s class is missing a streamDict class variable" %
                        cls.__name__)
            tags = sorted(_tagInfo(tag, sizeType, type_, name)
                    for (tag, (sizeType, type_, name))
                    in cls.streamDict.items())
            cls._streamTags = tags
        return tags

    def __eq__(self, other, skipSet=None):
        if type(self) != type(other):
            return False
        for tag in self._getTags():
            if skipSet and tag.name in skipSet:
                continue
            if getattr(self, tag.name) != getattr(other, tag.name):
                return False
        return True

    def __ne__(self, other, skipSet=None):
        return not self.__eq__(other, skipSet=skipSet)

    def __hash__(self):
        return hash(self.freeze())

    def __deepcopy__(self, memo):
        raise NotImplementedError

    @staticmethod
    def _pack(values, includeEmpty):
        # equivalent to concatStrings from streamset.c
        values.sort()
        words = []
        for tag, substream in values:
            if substream is None:
                continue
            if not substream and not includeEmpty:
                continue
            sizeType = tag.sizeType
            size = len(substream)
            if sizeType == DYNAMIC:
                if size < 0x8000:
                    sizeType = SMALL
                else:
                    sizeType = LARGE
            if sizeType == SMALL:
                if size >= 0x8000:
                    raise ValueError("short int overflow")
                fmt = '>BH'
            elif sizeType == LARGE:
                if size >= 0x80000000:
                    raise ValueError("long int overflow")
                size |= 0x80000000
                fmt = '>BI'
            else:
                raise TypeError("Invalid tag size")
            words.append(struct.pack(fmt, tag.tag, size))
            words.append(substream)
        if includeEmpty and not words:
            return None
        return ''.join(words)

    def freeze(self, skipSet=None, freezeKnown=True, freezeUnknown=True):
        out = []
        if freezeKnown:
            for tag in self._getTags():
                if skipSet and tag.name in skipSet:
                    continue
                value = getattr(self, tag.name)
                if isinstance(value, StreamSet):
                    substream = value.freeze(skipSet, freezeKnown,
                            freezeUnknown)
                else:
                    substream = value.freeze(skipSet)
                out.append((tag, substream))
        if freezeUnknown:
            for tag, substream in self._unknownTags:
                if skipSet and tag.tag in skipSet:
                    continue
                out.append((tag, substream))
        return self._pack(out, includeEmpty=False)

    @staticmethod
    def _readTag(frozen):
        tagNum = ord(frozen[0])
        if ord(frozen[1]) & 0x80:
            # 31 bit size
            size = struct.unpack('>I', frozen[1:5])[0] & 0x7fffffff
            sizeType = LARGE
            frozen = frozen[5:]
        else:
            # 15 bit size
            size = struct.unpack('>H', frozen[1:3])[0]
            sizeType = SMALL
            frozen = frozen[3:]
        if len(frozen) < size:
            raise ValueError("not enough data thawing stream set")
        substream, frozen = frozen[:size], frozen[size:]
        return tagNum, sizeType, substream, frozen

    def thaw(self, frozen):
        tagMap = dict((x.tag, x) for x in self._getTags())
        self._unknownTags = []
        while frozen:
            tagNum, sizeType, substream, frozen = self._readTag(frozen)
            # Find the matching stream from our stream definition
            tag = tagMap.get(tagNum)
            if not tag:
                if self.ignoreUnknown == SKIP_UNKNOWN:
                    continue
                elif self.ignoreUnknown == PRESERVE_UNKNOWN:
                    self._unknownTags.append((
                            _tagInfo(tagNum, sizeType, None, None), substream))
                    continue
                else:
                    raise ValueError("unknown tag in stream set")
            setattr(self, tag.name, tag.type(substream))

    def diff(self, other, ignoreUnknown=False):
        if type(self) != type(other):
            raise TypeError("invalid type")
        elif not ignoreUnknown and (self._unknownTags or other._unknownTags):
            raise ValueError("Cannot diff streams with unknown tags")

        out = []
        for tag in self._getTags():
            myvalue = getattr(self, tag.name)
            othervalue = getattr(other, tag.name)
            if isinstance(myvalue, StreamSet):
                substream = myvalue.diff(othervalue, ignoreUnknown)
            else:
                substream = myvalue.diff(othervalue)
            out.append((tag, substream))
        return self._pack(out, includeEmpty=True)

    def twm(self, diff, base, skip=None):
        if type(self) != type(base):
            raise TypeError("invalid type")
        if not diff:
            return False
        tagMap = dict((x.tag, x) for x in self._getTags())
        while diff:
            tagNum, sizeType, substream, diff = self._readTag(diff)
            tag = tagMap.get(tagNum)
            if not tag:
                raise NotImplementedError
            if skip and tag.name in skip:
                continue
            myvalue = getattr(self, tag.name)
            basevalue = getattr(base, tag.name)
            myvalue.twm(substream, basevalue)

    @classmethod
    def find(cls, tagNum, frozen):
        for tag in cls._getTags():
            if tag.tag == tagNum:
                break
        else:
            raise ValueError("unknown tag in stream set")

        while frozen:
            tagNum2, sizeType, substream, frozen = cls._readTag(frozen)
            if tagNum2 == tagNum:
                return tag.type(substream)
        return None


def splitFrozenStreamSet(frozen):
    raise NotImplementedError


def whiteOutFrozenStreamSet(frozen, skipId):
    raise NotImplementedError
