#
# Copyright (c) 2009 rPath, Inc.
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

import itertools, rpm, os, pwd, stat

from conary.local.capsules import SingleCapsuleOperation

def rpmkey(hdr):
    return "%s-%s-%s.%s" % ( hdr['name'], hdr['version'],
                             hdr['release'], hdr['arch'])

class Callback:

    def __init__(self):
        self.fdnos = {}

    def __call__(self, what, amount, total, mydata, wibble):
        if what == rpm.RPMCALLBACK_TRANS_START:
            pass
        elif what == rpm.RPMCALLBACK_INST_OPEN_FILE:
            hdr, path = mydata
            fd = os.open(path, os.O_RDONLY)
            self.fdnos[rpmkey(hdr)] = fd
            return fd
        elif what == rpm.RPMCALLBACK_INST_CLOSE_FILE:
            hdr, path = mydata
            os.close(self.fdnos[rpmkey(hdr)])
        elif what == rpm.RPMCALLBACK_INST_PROGRESS:
            pass

class RpmCapsuleOperation(SingleCapsuleOperation):

    def __init__(self, *args, **kwargs):
        SingleCapsuleOperation.__init__(self, *args, **kwargs)

    def apply(self, fileDict):
        # force the nss modules to be loaded from outside of any chroot
        pwd.getpwall()

        rpmList = []

        ts = rpm.TransactionSet(self.root, rpm._RPMVSF_NOSIGNATURES)
        # we use a pretty heavy hammer
        ts.setProbFilter(rpm.RPMPROB_FILTER_IGNOREOS        |
                         rpm.RPMPROB_FILTER_IGNOREARCH      |
                         rpm.RPMPROB_FILTER_REPLACEPKG      |
                         rpm.RPMPROB_FILTER_REPLACENEWFILES |
                         rpm.RPMPROB_FILTER_REPLACEOLDFILES |
                         rpm.RPMPROB_FILTER_OLDPACKAGE)

        for troveCs, (pathId, path, fileId) in self.installs:
            localPath = fileDict[(pathId, fileId)]
            fd = os.open(localPath, os.O_RDONLY)
            hdr = ts.hdrFromFdno(fd)
            os.close(fd)
            ts.addInstall(hdr, (hdr, localPath), "i")
            hasTransaction = True

        removeList = []
        for trv in self.removes:
            ts.addErase("%s-%s-%s.%s" % (
                    trv.troveInfo.capsule.rpm.name(),
                    trv.troveInfo.capsule.rpm.version(),
                    trv.troveInfo.capsule.rpm.release(),
                    trv.troveInfo.capsule.rpm.arch()))

        ts.check()
        ts.order()
        cb = Callback()
        ts.run(cb, '')
        probs = ts.run(cb, '')
        if probs:
            raise ValueError(str(probs))
