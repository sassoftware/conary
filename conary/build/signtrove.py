#!/usr/bin/python
#
# Copyright (c) 2005 rPath, Inc.
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

import re
import sys
import base64
import urllib

from conary import callbacks
from conary.checkin import fullLabel
from conary.conarycfg import selectSignatureKey
from conary.deps import deps
from conary.conarycfg import selectSignatureKey
from conary.lib import log
from conary.lib.openpgpfile import KeyNotFound
from conary.repository.netclient import NetworkRepositoryClient
from conary.updatecmd import parseTroveSpec
from conary.repository import errors

class SignatureCallback(callbacks.SignatureCallback, callbacks.LineOutput):

    def getTroveInfo(self, got, need):
        if need != 0:
            self._message("Downloading trove info (%d of %d)..." 
                          % (got, need))

    def signTrove(self, got, need):
        if need != 0:
            self._message("Signing trove (%d of %d)..." 
                          % (got, need))

    def sendSignature(self, got, need):
        if need != 0:
            self._message("Sending signature (%d of %d)..." 
                          % (got, need))

def recurseTroveInfo(repos, trvInfo):
    trv = repos.getTrove(*trvInfo)
    trvs = [trvInfo]
    for subTrvInfo in trv.iterTroveList():
        trvs.extend(recurseTroveInfo(repos, subTrvInfo))
    return trvs

def signTroves(cfg, specStrList, recurse = False, callback = 0):
    troveStr = ""
    troves = []
    trv = []
    repos = NetworkRepositoryClient(cfg.repositoryMap)

    if not callback:
        if cfg.quiet:
            callback = callbacks.SignatureCallback()
        else:
            callback = SignatureCallback()

    for specStr in specStrList:
        name, versionStr, flavor = parseTroveSpec(specStr)

        try:
            trvList = repos.findTrove([ cfg.buildLabel ],
                                      (name, versionStr, flavor), cfg.flavor)
        except errors.TroveNotFound, e:
            log.error(str(e))
            return

        for trvInfo in trvList:
            troveStr += "%s=%s[%s]\n" % (trvInfo[0], trvInfo[1].asString(),
                                         deps.formatFlavor(trvInfo[2]))

            if recurse:
                troves.extend(recurseTroveInfo(repos, trvInfo))
            else:
                troves.append(trvInfo)

        trv += repos.getTroves(troves, withFiles = True)

    if cfg.interactive:
        print troveStr
        print "Total: %d troves" % len(troves)
        print "Are you sure you want to digitally sign these troves [y/N]?"
        answer = sys.stdin.readline()
        if ansert[0].upper() != 'Y':
            return

    n = len(troves)
    for i in range(n):
        callback.getTroveInfo(i+1,n)
        trvInfo = troves[i]
        signatureKey = selectSignatureKey(cfg, trv[i].getVersion().branch().label().asString())
        if signatureKey:
            try:
                trv[i].getDigitalSignature(signatureKey)
                if not cfg.quiet:
                    print "\nTrove: ",str(trvInfo[0]) + str(trvInfo[1].asString()) + " " + str(trvInfo[2]) + "\nis already signed by key: " + cfg.signatureKey
                    return
            except KeyNotFound:
                pass

    n = len(troves)
    for i in range(n):
        callback.signTrove(i+1,n)
        trvInfo = troves[i]
        signatureKey = selectSignatureKey(cfg, trv[i].getVersion().branch().label().asString())
        if signatureKey:
            try:
                trv[i].addDigitalSignature(signatureKey)
            except KeyNotFound:
                print "\nKey:", signatureKey, "is not in your keyring."
                return

    misfires = []
    for i in range(n):
        callback.sendSignature(i+1,n)
        trvInfo = troves[i]
        signatureKey = selectSignatureKey(cfg, trv[i].getVersion().branch().label().asString())
        if signatureKey:
            try:
                repos.addDigitalSignature(trvInfo[0],trvInfo[1],trvInfo[2], trv[i].getDigitalSignature(signatureKey) )
            except KeyNotFound:
                misfires.append(trvInfo[0])

    if misfires:
        kError = KeyNotFound('')
        kError.error = 'The following troves could not be signed: %s' \
                       % str(misfires)
        raise kError
