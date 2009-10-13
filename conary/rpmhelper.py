#
# Copyright (c) 2004-2008 rPath, Inc.
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

"""
Contains functions to assist in dealing with rpm files.
"""

import gzip

import itertools, struct, re
from conary.lib.sha1helper import *
from conary.deps import deps
from conary.lib import util

NAME            = 1000
VERSION         = 1001
RELEASE         = 1002
EPOCH           = 1003
SUMMARY         = 1004
DESCRIPTION     = 1005
VENDOR          = 1011
LICENSE         = 1014
SOURCE          = 1018
ARCH            = 1022
PREIN           = 1023
POSTIN          = 1024
PREUN           = 1025
POSTUN          = 1026
OLDFILENAMES    = 1027
FILEMODES       = 1030
FILERDEVS       = 1033
FILEFLAGS       = 1037 # bitmask: RPMFILE_* are bitmasks to interpret
FILEUSERNAME    = 1039
FILEGROUPNAME   = 1040
SOURCERPM       = 1044
PROVIDENAME     = 1047
REQUIRENAME     = 1049
RPMVERSION      = 1064
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

SIG_SHA1        = 269
SIG_SIZE        = 1000

RPMFILE_NONE       = 0
RPMFILE_CONFIG     = (1 <<  0)
RPMFILE_DOC        = (1 <<  1)
RPMFILE_ICON       = (1 <<  2)
RPMFILE_MISSINGOK  = (1 <<  3)
RPMFILE_NOREPLACE  = (1 <<  4)
RPMFILE_SPECFILE   = (1 <<  5)
RPMFILE_GHOST      = (1 <<  6)
RPMFILE_LICENSE    = (1 <<  7)
RPMFILE_README     = (1 <<  8)
RPMFILE_EXCLUDE    = (1 <<  9)
RPMFILE_UNPATCHED  = (1 <<  10)
RPMFILE_PUBKEY     = (1 <<  11)
RPMFILE_POLICY     = (1 <<  12)

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

class RpmHeader(object):
    __slots__ = ['entries', 'data', 'isSource']
    _tagListValues = set([
        DIRNAMES, BASENAMES, DIRINDEXES, FILEUSERNAME, FILEGROUPNAME])

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

    def get(self, item, default):
        if item in self:
            return self[item]

        return default

    def _getDepsetFromHeader(self, tag):
        depset = deps.DependencySet()
        binprefixre = re.compile('/bin/|/sbin/|/usr/bin/|/usr/sbin/|/usr/libexec/')
        flagre = re.compile('\((.*?)\)')
        depnamere = re.compile('(.*?)\(.*')

        for dep in self.get(tag, []):
            if dep.startswith('/'):
                # convert file deps to real Conary file deps
                # FIXME: find a way to share this with the code at
                # packagepolicy.py:2465
                if binprefixre.match(dep):
                    depset.addDep(deps.FileDependencies, deps.Dependency(dep))
            else:
                # convert anything inside () to a flag
                flags = flagre.findall(dep)
                if flags:
                    # the dependency name is everything until the first (
                    dep = depnamere.match(dep).group(1)
                    if len(flags) == 2:
                        # if we have (flags)(64bit), we need to pop
                        # the 64bit marking off the end and namespace the
                        # dependency name.
                        dep += '[%s]' %flags.pop()
                    flags = [ (x, deps.FLAG_SENSE_REQUIRED) for x in flags if x ]
                else:
                    flags = []
                depset.addDep(deps.RpmDependencies, deps.Dependency(dep, flags))
        return depset

    def getDeps(self):
        """
        Create two dependency sets that represent the requires and
        provides described in this RPM header object.

        @return: (requires, provides)
        @rtype: two-tuple of deps.DependencySet instances
        """
        reqset = self._getDepsetFromHeader(REQUIRENAME)
        provset = self._getDepsetFromHeader(PROVIDENAME)

        return reqset, provset

    def __getitem__(self, tag):
        if tag == OLDFILENAMES and tag not in self.entries:
            # mimic OLDFILENAMES using DIRNAMES and BASENAMES
            dirs = dict(enumerate(self[DIRNAMES]))
            paths = []
            for dirIndex, baseName in itertools.izip(self[DIRINDEXES],
                                                     self[BASENAMES]):
                paths.append(dirs[dirIndex] + baseName)

            return paths

        if tag in self._tagListValues and tag not in self.entries:
            # Lists that are not present are empty
            return []

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
            elif dataType in (6, 8, 9):
                # RPM_STRING_TYPE, RPM_STRING_ARRAY_TYPE, RPM_I18NSTRING_TYPE
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

    def __init__(self, f, sha1 = None, isSource = False, sigBlock = False):
        intro = f.read(16)
        (mag1, mag2, mag3, ver, reserved, entries, size) = \
            struct.unpack("!BBBBiii", intro)

        if mag1 != 0x8e or mag2 != 0xad or mag3 != 0xe8  or ver != 01:
            raise IOError, "bad magic for header"

        entryTable = f.read(entries * 16)

        self.isSource = isSource
        self.entries = {}
        self.data = f.read(size)

        if sha1 is not None:
            computedSha1 = sha1ToString(sha1String(intro + entryTable +
                                                   self.data))
            if computedSha1 != sha1:
                raise IOError, "bad header sha1"

        for i in range(entries):
            (tag, dataType, offset, count) = struct.unpack("!iiii", 
                                            entryTable[i * 16: i * 16 + 16])

            self.entries[tag] = (dataType, offset, count)

        if sigBlock:
            place = f.tell()
            if place % 8:
                f.seek(8 - (place % 8), 1)

def readHeader(f):
    lead = f.read(96)
    leadMagic = struct.unpack("!i", lead[0:4])[0]

    if (leadMagic & 0xffffffffl) != 0xedabeedbl: 
	raise IOError, "file is not an RPM"

    isSource = (struct.unpack('!H', lead[6:8])[0] == 1)

    sigs = RpmHeader(f, isSource = isSource, sigBlock = True)
    sha1 = sigs.get(SIG_SHA1, None)

    if SIG_SIZE in sigs:
        size = sigs[SIG_SIZE][0]
        totalSize = os.fstat(f.fileno()).st_size
        pos = f.tell()
        if size != (totalSize - pos):
            raise IOError, "file size does not match size specified by header"

    return RpmHeader(f, sha1 = sha1, isSource = isSource)


def getRpmLibProvidesSet(rpm):
    """
    Retreieve a dependency set that represents the rpmlib provides
    from the loaded rpm module
    @param rpm: the rpm module
    @type rpm: module
    @return: A dependency containing the virtual items that rpmlib provides
    @rtype: conary.deps.deps.DependencySet()
    """
    depset = deps.DependencySet()
    for prov in rpm.ds.Rpmlib():
        dep = deps.parseDep('rpm: '+prov.N())
        depset.union(dep)
    return depset

class BaseError(Exception):
    "Base exception class"

class UnknownPayloadFormat(BaseError):
    "The payload format is not supported"

class UnknownCompressionType(BaseError):
    "The payload format is not supported"

def extractRpmPayload(fileIn, fileOut):
    """
    Given a (seekable) file object containing an RPM package, extract the
    payload into the destination file. Only cpio payloads are supported for now.
    """
    uncompressed = UncompressedRpmPayload(fileIn)

    while 1:
        buf = uncompressed.read(16384)
        if not buf:
            break
        fileOut.write(buf)

def UncompressedRpmPayload(fileIn):
    """
    Given a (seekable) file object containing an RPM package, return
    a file-like object that can be used for streaming uncompressed content
    """
    fileIn.seek(0)
    header = readHeader(fileIn)
    # check to make sure that this is a cpio archive (though most rpms
    # are cpio).  If the tag does not exist, assume it's cpio
    if header.has_key(PAYLOADFORMAT):
        if header[PAYLOADFORMAT] != 'cpio':
            raise UnknownPayloadFormat(header[PAYLOADFORMAT])

    # check to see how the payload is compressed.  Again, if the tag
    # does not exist, assume that it's gzip.
    if header.has_key(PAYLOADCOMPRESSOR):
        compression = header[PAYLOADCOMPRESSOR]
    else:
        compression = 'gzip'

    if compression == 'gzip':
        decompressor = lambda fobj: gzip.GzipFile(fileobj=fobj)
    elif compression == 'bzip2':
        decompressor = lambda fobj: util.BZ2File(fobj)
    elif compression in ['lzma', 'xz']:
        decompressor = lambda fobj: util.LZMAFile(fobj)
    else:
        raise UnknownCompressionType(compression)

    # rewind the file to let seekToData do its job
    fileIn.seek(0)
    seekToData(fileIn)
    uncompressed = decompressor(fileIn)
    return uncompressed
