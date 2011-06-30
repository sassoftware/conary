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
