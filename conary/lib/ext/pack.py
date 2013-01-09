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


def pack(pfmt, *args):
    if pfmt[0] != '!':
        raise ValueError("format must begin with !")
    pfmt = pfmt[1:]
    pargs = list(args)

    sfmt = '!'
    sargs = []
    while pfmt:
        code, pfmt = pfmt[0], pfmt[1:]
        if code in 'BHI':
            sfmt += code
            sargs.append(pargs.pop(0))
        elif code == 'S':
            sizecode, pfmt = pfmt[0], pfmt[1:]
            string = pargs.pop(0)
            if sizecode in 'HI':
                sfmt += '%s%ds' % (sizecode, len(string))
                sargs.extend([len(string), string])
            elif sizecode.isdigit():
                while pfmt and pfmt[0].isdigit():
                    sizecode += pfmt[0]
                    pfmt = pfmt[1:]
                sfmt += '%ds' % int(sizecode)
                sargs.append(string)
            else:
                raise ValueError('# must be followed by H or I in format')
        else:
            raise ValueError('unknown character %r in format' % (code,))
    return struct.pack(sfmt, *sargs)


def unpack(pfmt, offset, data):
    data = data[offset:]
    origLen = len(data)
    if pfmt[0] != '!':
        raise ValueError("format must begin with !")
    pfmt = pfmt[1:]
    out = []
    while pfmt:
        code, pfmt = pfmt[0], pfmt[1:]
        if code == 'B':
            out.append(ord(data[:1]))
            data = data[1:]
        elif code == 'H':
            out.append(struct.unpack('>H', data[:2])[0])
            data = data[2:]
        elif code == 'I':
            out.append(struct.unpack('>I', data[:4])[0])
            data = data[4:]
        elif code == 'S':
            sizecode, pfmt = pfmt[0], pfmt[1:]
            if sizecode == 'H':
                size = struct.unpack('>H', data[:2])[0]
                data = data[2:]
            elif sizecode == 'I':
                size = struct.unpack('>I', data[:4])[0]
                data = data[4:]
            elif sizecode.isdigit():
                while pfmt and pfmt[0].isdigit():
                    sizecode += pfmt[0]
                    pfmt = pfmt[1:]
                size = int(sizecode)
            else:
                raise ValueError('# must be followed by H or I in format')
            out.append(data[:size])
            data = data[size:]
        elif code == 'D':
            sizebits = ord(data[0]) & 0xc0
            if sizebits == 0x00:
                size = ord(data[0])
                data = data[1:]
            elif sizebits == 0x40:
                size = struct.unpack('!H', data[:2])[0] & 0x3fff
                data = data[2:]
            elif sizebits == 0x80:
                size = struct.unpack('!I', data[:4])[0] & 0x3fffffff
                data = data[4:]
            else:
                raise ValueError("unimplemented dynamic size")
            out.append(data[:size])
            data = data[size:]
        else:
            raise ValueError('unknown character %r in format' % (code,))
    consumed = origLen - len(data)
    offset += consumed
    return offset, out


def dynamicSize(size):
    if size < 0x40:
        return chr(size)
    elif size < 0x4000:
        size |= 0x4000
        fmt = '!H'
    elif size < 0x40000000:
        size |= 0x80000000
        fmt = '!I'
    else:
        raise ValueError("unimplemented dynamic size")
    return struct.pack(fmt, size)
