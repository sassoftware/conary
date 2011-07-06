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

import os
import tempfile
import zlib
# FIXME
#from conary.lib import digestlib
import hashlib as digestlib
from conary.lib.ext import file_utils


_BUFFER_SIZE = 1024 * 256


def sha1Uncompress((inFd, inStart, inSize), path, baseName, targetPath):
    outFd, tmpPath = tempfile.mkstemp(prefix='.ct' + baseName, dir=path)
    try:
        outFobj = os.fdopen(outFd, 'wb')
        digest = digestlib.sha1()
        decomp = zlib.decompressobj(31)

        inStop = inSize + inStart
        inAt = inStart
        while inAt < inStop:
            # read
            toRead = min(_BUFFER_SIZE, inStop - inAt)
            raw = file_utils.pread(inFd, toRead, inAt)
            if not raw:
                raise RuntimeError("short read")
            inAt += len(raw)

            # inflate
            clear = decomp.decompress(raw)
            if not clear:
                continue

            # digest and copy
            digest.update(clear)
            outFobj.write(clear)

        clear = decomp.flush()
        if clear:
            digest.update(clear)
            outFobj.write(clear)
        outFobj.close()

        if os.path.isdir(targetPath):
            os.rmdir(targetPath)
        os.rename(tmpPath, targetPath)

        return digest.digest()
    finally:
        try:
            os.unlink(tmpPath)
        except:
            pass


def sha1Copy((inFd, inStart, inSize), outFds):
    digest = digestlib.sha1()
    decomp = zlib.decompressobj(31)
    inStop = inSize + inStart
    inAt = inStart
    while inAt < inStop:
        # read
        toRead = min(_BUFFER_SIZE, inStop - inAt)
        raw = file_utils.pread(inFd, toRead, inAt)
        if not raw:
            raise RuntimeError("short read")
        inAt += len(raw)

        # copy (stil compressed)
        for outFd in outFds:
            raw2 = raw
            while raw2:
                written = os.write(outFd, raw2)
                if not written:
                    raise RuntimeError("short write")
                raw2 = raw2[written:]

        # inflate
        clear = decomp.decompress(raw)
        if not clear:
            continue

        # digest
        digest.update(clear)

    clear = decomp.flush()
    if clear:
        digest.update(clear)
    return digest.digest()
