#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Contains functions to assist in dealing with rpm files.
"""

import struct

NAME            = 1000
VERSION         = 1001
RELEASE         = 1002
EPOCH           = 1003
PREIN           = 1023
POSTIN          = 1024
PREUN           = 1025
POSTUN          = 1026
OLDFILENAMES    = 1027
FILEMODES       = 1030
FILERDEVS       = 1033
FILEFLAGS       = 1037 # bitmask; (1<<0 => config)
FILEUSERNAME    = 1039
FILEGROUPNAME   = 1040
SOURCERPM       = 1044
TRIGGERSCRIPTS  = 1065
PREINPROG       = 1085
POSTINPROG      = 1086
PREUNPROG       = 1087
POSTUNPROG      = 1088
DIRINDEXES      = 1116
BASENAMES       = 1117
DIRNAMES        = 1118
PAYLOADFORMAT     = 1124
PAYLOADCOMPRESSOR = 1125

def seekToData(f):
    """
    Accepts a python file object (positioned at the start of an rpm)
    and positions the file pointer at the gzipped cpio archive
    attached to it
    @param f: python file object to posititon at the gzipped cpio archive
    @type f: file
    @rtype: None
    """
    lead = f.read(96)
    leadMagic = struct.unpack("!i", lead[0:4])[0]

    if (leadMagic & 0xffffffffl) != 0xedabeedbl: 
	raise IOError, "file is not an RPM"

    # signature block
    sigs = f.read(16)
    (mag1, mag2, mag3, ver, reserved, entries, size) = \
	struct.unpack("!BBBBiii", sigs)

    if mag1 != 0x8e or mag2 != 0xad or mag3 != 0xe8  or ver != 01:
	raise IOError, "bad magic for signature block"

    f.seek(size + entries * 16, 1)

    place = f.tell()
    if place % 8:
	f.seek(8 - (place % 8), 1)

    # headers
    sigs = f.read(16)
    (mag1, mag2, mag3, ver, reserved, entries, size) = \
	struct.unpack("!BBBBiii", sigs)

    if mag1 != 0x8e or mag2 != 0xad or mag3 != 0xe8  or ver != 01:
	raise IOError, "bad magic for header"

    f.seek(size + entries * 16, 1)

class RpmHeader:

    def has_key(self, tag):
        return self.entries.has_key(tag)
    __contains__ = has_key

    def paths(self):
        if OLDFILENAMES in self:
            for path in self[OLDFILENAMES]:
                yield path
            return

        paths = self[DIRNAMES]
        indexes = self[DIRINDEXES]

        if type(indexes) is not list:
            indexes = [ indexes ]

        for (dirIndex, baseName) in zip(indexes, self[BASENAMES]):
            if paths[0]:
                yield paths[dirIndex] + '/' + baseName
            else:
                yield baseName

    def __getitem__(self, tag):
        (dataType, offset, count) = self.entries[tag]

        items = []
        while count:
            if dataType == 1:
                # RPM_CHAR_TYPE
                items.append(self.data[offset])
                offset += 1
            elif dataType == 2:
                # RPM_INT8_TYPE
                items.append(struct.unpack("B", self.data[offset])[0])
                offset += 1
            elif dataType == 3:
                # RPM_INT16_TYPE
                items.append(struct.unpack("!H", self.data[offset:offset+2])[0])
                offset += 2
            elif dataType == 4:
                # RPM_INT32_TYPE
                items.append(struct.unpack("!I", self.data[offset:offset+4])[0])
                offset += 4
            elif dataType == 6 or dataType == 8:
                # RPM_STRING_TYPE or RPM_STRING_ARRAY_TYPE
                s = ""
                while self.data[offset] != '\0':
                    s += self.data[offset]
                    offset += 1
                items.append(s)
                offset += 1

            count -= 1

        if (count == 1 or count == 0) and dataType == 6:
            # count isn't set for RPM_STRING_TYPE
            return items[0]

        return items

    def __init__(self, f):
        intro = f.read(16)
        (mag1, mag2, mag3, ver, reserved, entries, size) = \
            struct.unpack("!BBBBiii", intro)

        if mag1 != 0x8e or mag2 != 0xad or mag3 != 0xe8  or ver != 01:
            raise IOError, "bad magic for header"

        entryTable = f.read(entries * 16)

        self.entries = {}
        self.data = f.read(size)

        for i in range(entries):
            (tag, dataType, offset, count) = struct.unpack("!iiii", 
                                            entryTable[i * 16: i * 16 + 16])

            self.entries[tag] = (dataType, offset, count)

        place = f.tell()
        if place % 8:
            f.seek(8 - (place % 8), 1)

def readHeader(f):
    lead = f.read(96)
    leadMagic = struct.unpack("!i", lead[0:4])[0]

    if (leadMagic & 0xffffffffl) != 0xedabeedbl: 
	raise IOError, "file is not an RPM"

    sigs = RpmHeader(f)
    return RpmHeader(f)
