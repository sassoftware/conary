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

from conary import callbacks
from conary import conaryclient
from conary import trove
from conary.conarycfg import selectSignatureKey
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log
from conary.lib.openpgpfile import KeyNotFound
from conary.updatecmd import parseTroveSpec
from conary.repository import errors

class SignatureCallback(callbacks.SignatureCallback, callbacks.LineOutput):

    def signTrove(self, got, need):
        if need != 0:
            self._message("Signing trove (%d of %d)..." 
                          % (got, need))

def signTroves(cfg, specStrList, recurse = False, callback = None):
    troveStr = ""
    jobList = []
    trv = []
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if callback is None:
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

            jobList.append((trvInfo[0], (None, None), (trvInfo[1], trvInfo[2]),
                            True))

    if cfg.interactive:
        print troveStr
        print "Total: %d troves" % len(troves)
        answer = cmdline.askYn('Are you sure you want to digitally sign these troves [y/N]?', default=False)
        if not answer:
            return

    # We use a changeset here instead of getTroves because changeset knows
    # how to do efficient recursion.
    cs = repos.createChangeSet(jobList, withFiles = True,
                               withFileContents = False, recurse = recurse)

    totalNum = len([ x for x in cs.iterNewTroveList() ])
    misfires = []

    for i, trvCs in enumerate(cs.iterNewTroveList()):
        trv = trove.Trove(trvCs)
        callback.signTrove(i + 1, totalNum)

        label = trv.getVersion().branch().label()
        signatureKey = selectSignatureKey(cfg, label.asString())

        if not signatureKey:
            if not cfg.quiet:
                print "\nNo key is defined for label %s" % label
                return

            continue

        try:
            trv.getDigitalSignature(signatureKey)
            if not cfg.quiet:
                print "\nTrove: %s=%s[%s] is already signed by key: %s" \
                    % (trv.getName(), trv.getVersion(), 
                       deps.formatFlavor(trv.getFlavor()),
                       cfg.signatureKey)
                continue
        except KeyNotFound:
            pass

        try:
            trv.addDigitalSignature(signatureKey)
        except KeyNotFound:
            print "\nKey:", signatureKey, "is not in your keyring."
            return

        try:
            repos.addDigitalSignature(trv.getName(), trv.getVersion(),
                                      trv.getFlavor(),
                                      trv.getDigitalSignature(signatureKey))
        except (errors.AlreadySignedError, KeyNotFound):
            misfires.append(trv.getName())

    if misfires:
        raise errors.DigitalSignatureError('The following troves could not be signed: %s' % str(misfires))
