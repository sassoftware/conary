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


from conary import callbacks
from conary import conaryclient
from conary import trove
from conary.conarycfg import selectSignatureKey
from conary.conaryclient import cmdline
from conary.deps import deps
from conary.lib import log
from conary.lib.openpgpfile import KeyNotFound
from conary.cmds.updatecmd import parseTroveSpec
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
        print "Total: %d troves" % len(jobList)
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

        # Look for a public key for this key; don't catch the exception
        keyCache = trove.openpgpkey.getKeyCache()
        key = keyCache.getPublicKey(signatureKey)
        signatureKey = key.getFingerprint()

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
        except (errors.AlreadySignedError, KeyNotFound), e:
            misfires.append((trv.getName(), str(e)))

    if misfires:
        troves = [ x[0] for x in misfires ]
        errs = set(x[1] for x in misfires)
        raise errors.DigitalSignatureError('The following troves could not be signed: %s; reason(s): %s' % (troves, list(errs)))
