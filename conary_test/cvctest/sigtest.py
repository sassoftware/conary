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


from conary_test import rephelp
import os
from tempfile import mkdtemp

from conary.build import signtrove
from conary.deps import deps
from conary import callbacks
from conary import trove
from conary import versions
from conary import files
from conary.lib import openpgpkey
from conary.lib import openpgpfile
from conary.lib import util
from conary.lib import sha1helper
from conary.repository import errors, changeset, filecontents
from conary.checkin import checkout
from conary.cmds.updatecmd import doUpdate
from conary.trove import DigitalSignatureVerificationError
from conary import conarycfg
from conary.constants import version
from conary import checkin
from conary.build import cook
from conary.cmds import cvccmd

from conary_test import resources


testRecipe = """
class TestCase(PackageRecipe):
    name = "testcase"
    version = "1.0"
    clearBuildReqs()
    
    def setup(r):
        r.Create("/temp/foo")
"""

testGroup = """
class GroupTest(GroupRecipe):
    name = "group-test"
    version = "1.0"
    clearBuildRequires()

    def setup(r):
        r.add('testcase')
"""

outerGroup = """
class OuterGroup(GroupRecipe):
    name = 'group-outer'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.add("group-test")
        self.add("test:doc")
"""

unexpiredKey = """
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.2 (GNU/Linux)

mIsEQ5ZfhQEEANzow8IUOLx1tWzENLeMCJHgYeDy8lW6h5q2Okuq/rpVEQ+PyjwX
BXTYr/AKlDXyOvxBhWR6UCTN0o7p8rMjPGEGveU5TXSVBNOOCAwZupQpNLlSnCct
ei3g7QayQEtYVs13W5Ma/xZkbS6rR/7mflI4L/VOd85p7DEyzM2+Zu99AAYptBlS
U0EgMTAyNCBleHBpcmVzIERlYyAyMDI1iLwEEwECACYFAkOWX4UCGwMFCSWYBgAG
CwkIBwMCBBUCCAMEFgIDAQIeAQIXgAAKCRDU+PEnwme3nX+hBAC/ey7yEEdH+cer
G7NXclREVCuTETP5JzURgOEcuwLE+maTXE6GoPZ2CWiepKt2oxgKkF6DUuk3RlNX
VHU3l8zAfqG5TBhG0VGBfJTc4cyK7Y2SIhYlqJJFUZO8HGhGcGwSbQNOvACoAZg8
xUfoauqTQCYj7+RMrWkzJyHgyYIblg==
=riRT
-----END PGP PUBLIC KEY BLOCK-----
"""

expiredKey = """
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v1.4.2 (GNU/Linux)

mQEfBEMAtgMBCKDD33LAWuYxFqClhynF5EXTAWzweLmRpDNCfZrO6GwI8rSqroWv
PyDsI7to84BpfraUpnJrqA4f7Kq/ws8a6by544MjEfgBeMc4k9N/FOT0lNeFJ75c
eKZq3xlKeAwjAADBuHCqv7vnUzPGhkgje/qHKvCxxz0ZXc2KmGw77t0/YBRiHmwM
chFHYyF9ZsCdHLPK2gYMu3g3s8gBLzunZ+lNm8H4+NdYSqsIWurR4ouf+VZBiKXQ
Mp38Lt0i7Ec4M5mFJ9Rzu30fqMxwHwdZg8BYU7aEpaN9UDEbEPkJN0yM5yohw8Ah
ws9OuBHRK10Hgf0HcRBEJaQum4Q1bjjjzHWagZ3++v2axU3gzX+eZN3gfuRf02cA
Bim0ElJTQSAyMTkwIG5vIGNpcGhlcokBTgQTAQIAJAUCQwC2AwIbDwUJARTbAAYL
CQgHAwIDFQIDAxYCAQIeAQIXgAAKCRD3xUKp2kTkvcgyCKC3Kzv4Il1m4ehMcMAe
FCU4w6BPKN+jooJdI0TbPFTo5aEO2NFgsc8b2Ffnnht1MB8gi1g3AWB1NWn+sqfO
Zoxr7CAy2hzthvzixG8jFKjhDJjLyk+FVavOkKdR55xoxS1OFADNh7QwlBzdnQja
8L12+VYXm2DXpk45eKHKzNS38l17rncEUiRM/tjVtzT+zgg3OnMeETXI5cvtb7Up
xf3iwnLt8BwkEZ4pUsPdhA+As0ITDZ/A8FZu0myhdAhGLZDmZ54w1jlep957M/r5
iPKU1wBxVtOP2ZmjJaBH1PtoSVnTmZVdFYc03HAq2RNNpFpY+IJcXItcsfYKaeku
U5Uw3zY+TVHsLelFuTnewpgP8vrpxdI=
=SXEB
-----END PGP PUBLIC KEY BLOCK-----
"""

class SigTest(rephelp.RepositoryHelper):

    @staticmethod
    def _checkDigitalSig(trv, fingerprint):
        vSigs = trv.getDigitalSignature(fingerprint)
        sigVers = set()
        for sigBlock in vSigs:
            sigVers.add(sigBlock.version())
            for sig in sigBlock.signatures:
                assert(sig[0] == fingerprint)
        assert(set([trove._TROVESIG_VER_CLASSIC,
                    trove._TROVESIG_VER_NEW]) == sigVers)

    def testSignTrove(self):
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        self.addQuickTestComponent("test:doc", "1.0-1-1")
        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True

        self.assertRaises(errors.DigitalSignatureError, 
                          signtrove.signTroves, self.cfg, [ "test:doc" ])

        # get the public key
        keyRing = open(resources.get_path('conary_test', 'archive', 'pubring.gpg'))
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()

        # upload the public key
        repos = self.openRepository()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        signtrove.signTroves(self.cfg, [ "test:doc" ])

        # get the signed trove from the repository, verify that
        # everything is correct
        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')
        t = repos.getTrove('test:doc', v, f)
        self._checkDigitalSig(t, fingerprint)
        self.assertEqual(t.verifyDigitalSignatures(),
                (openpgpfile.TRUST_TRUSTED, [], set()))

        # add another signature.  This exercises code such as the
        # change set cache invalidation
        fingerprint2 = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        keyCache.getPrivateKey(fingerprint2, '111111')

        # upload the public key
        keyRing = open(resources.get_path('conary_test', 'archive', 'pubring.gpg'))
        keyData = openpgpfile.exportKey(fingerprint2, keyRing)
        keyData.seek(0)
        keyData = keyData.read()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)

        # sign with the second private key
        self.cfg.signatureKey = fingerprint2
        self.cfg.quiet = True
        signtrove.signTroves(self.cfg, [ "test:doc" ])

        # iterate over the signatures, making sure each signature version has
        # two sigs associated with it
        t = repos.getTrove('test:doc', v, f)
        self._checkDigitalSig(t, fingerprint)
        self._checkDigitalSig(t, fingerprint2)
        self.assertEqual(t.verifyDigitalSignatures(),
            (openpgpfile.TRUST_TRUSTED, [], set()))


        # attempt signing again with a key that already signed the trove
        self.assertRaises(errors.DigitalSignatureError,
                          signtrove.signTroves, self.cfg, [ "test:doc" ])

    def testSignTroveExpiration(self):
        # test that a key that hasn't expired will succeed
        fingerprint = "7CCD34B5C5D9CD1F637F6743D4F8F127C267B79D"
        repos = self.openRepository()
        repos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', unexpiredKey)
        self.addQuickTestComponent("test:doc", "1.0-1-1")
        self.cfg.signatureKey = fingerprint
        signtrove.signTroves(self.cfg, [ "test:doc" ])

        # test that a key that has expired won't succeed
        fingerprint = "69E3912FA8DDD94EDF172D4BF7C542A9DA44E4BD"
        repos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', unexpiredKey)
        self.cfg.signatureKey = fingerprint
        self.assertRaises(errors.DigitalSignatureError,
                          signtrove.signTroves, self.cfg, [ "test:doc" ])

    def testSignedUpdate(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        self.addQuickTestComponent("test:doc", "1.0-1-1")

        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True

        # put the public key into the repo
        keyRing = open(resources.get_path('conary_test', 'archive', 'pubring.gpg'))
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()
        repos = self.openRepository()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        signtrove.signTroves(self.cfg, [ "test:doc" ])

        # alter key's trust value and determine that the doUpdate
        # code properly verifies trust thresholds
        pubKey = keyCache.getPublicKey(fingerprint)
        pubKey.trustLevel = openpgpfile.TRUST_UNTRUSTED
        keyCache.publicDict[fingerprint] = pubKey
        self.cfg.trustThreshold = openpgpfile.TRUST_ULTIMATE
        self.logFilter.add()
        try:
            self.discardOutput(doUpdate, self.cfg, 'test:doc')
            self.fail("updatecmd.doUpdate did not properly check trust levels")
        except DigitalSignatureVerificationError:
            self.logFilter.compare(['warning: The trove test:doc has '
                'signatures generated with untrusted keys. You can either '
                'resign the trove with a key that you trust, or add one of the '
                'keys to the list of trusted keys (the trustedKeys '
                'configuration option). The keys that were not trusted are: '
                'F94E405E'])

        # An example how one can catch the digital signature verification
        # error for each unsigned trove (and display some message in a GUI
        # maybe). In this case we just raise a different exception.
        class MyException(Exception):
            errorIsUncatchable = True

        class C_(callbacks.UpdateCallback):
            def verifyTroveSignatures(self, trv):
                try:
                    return callbacks.UpdateCallback.verifyTroveSignatures(
                                    self, trv)
                except DigitalSignatureVerificationError:
                    raise MyException("Error in trove %s" % trv.getName())

        callback = C_(trustThreshold=self.cfg.trustThreshold)
        # We should catch our own exception now
        self.discardOutput(self.assertRaises, MyException,
                           doUpdate, self.cfg, "test:doc",
                           callback=callback)

    def testSignedCheckout(self):
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        self.addQuickTestComponent("test:source", "1.0-1-1")

        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True

        # put the public key into the repo
        keyRing = open(resources.get_path('conary_test', 'archive', 'pubring.gpg'))
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()
        repos = self.openRepository()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        signtrove.signTroves(self.cfg, [ "test:source" ])

        # alter key's trust value and determine that the doUpdate
        # code properly verifies trust thresholds
        pubKey = keyCache.getPublicKey(fingerprint)
        pubKey.trustLevel = openpgpfile.TRUST_UNTRUSTED
        keyCache.publicDict[fingerprint] = pubKey
        self.cfg.trustThreshold = openpgpfile.TRUST_ULTIMATE
        self.logFilter.add()
        try:
            checkout(repos, self.cfg, self.workDir, ["test"])
            self.fail("checkin.checkout did not properly verify trust levels")
        except DigitalSignatureVerificationError:
            self.logFilter.compare(['warning: The trove test:source has '
                'signatures generated with untrusted keys. You can either '
                'resign the trove with a key that you trust, or add one of the '
                'keys to the list of trusted keys (the trustedKeys '
                'configuration option). The keys that were not trusted are: '
                '90B1E477'])

    def testChangeKeyOwner(self):
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        self.addQuickTestComponent("test:source", "1.0-1-1")

        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True
        
        repos = self.openRepository()
        anotherRepos = self.setupUser(repos, self.cfg.buildLabel, 'another', 'anotherpass', None, None)
        
        # put the public key into the repo
        keyRing = open(resources.get_path('conary_test', 'archive', 'pubring.gpg'))
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()
        repos.addNewPGPKey(self.cfg.buildLabel, 'test', keyData)
        repos.changePGPKeyOwner(self.cfg.buildLabel, 'another', fingerprint)

        assert(repos.listUsersMainKeys(self.cfg.buildLabel, 'another')[0] == fingerprint)

    def testCheckoutMissingKey(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        self.cfg.signatureKey = fingerprint
        self.addQuickTestComponent("test:source", "1.0-1-1")
        signtrove.signTroves(self.cfg, [ "test:source" ])

        repos = self.openRepository()

        # utterly prevent the keycache from knowing about the key,
        # but give it a place to store a keyserver retrieved key.
        newKeyCache = openpgpkey.OpenPGPKeyFileCache()
        tmpPath = mkdtemp()
        pubRing = self.cfg.pubRing
        self.cfg.pubRing = [tmpPath + '/pubring.gpg']
        newKeyCache.publicPaths = self.cfg.pubRing
        keyCacheCallback = openpgpkey.KeyCacheCallback(repos, self.cfg)
        newKeyCache.setCallback(keyCacheCallback)

        openpgpkey.setKeyCache(newKeyCache)

        try:
            checkout(repos, self.cfg, self.workDir, ["test"])
            newKeyCache.getPublicKey(fingerprint)
        finally:
            self.cfg.pubRing = pubRing
            openpgpkey.setKeyCache(keyCache)
        util.rmtree(tmpPath)

    def testUpdateMissingKey(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        self.cfg.signatureKey = fingerprint
        self.addQuickTestComponent("test:doc", "1.0-1-1")

        signtrove.signTroves(self.cfg, [ "test:doc" ])

        repos = self.openRepository()

        # utterly prevent the keycache from knowing about the key,
        # but give it a place to store a keyserver retrieved key.
        newKeyCache = openpgpkey.OpenPGPKeyFileCache()
        tmpPath = mkdtemp()
        pubRing = self.cfg.pubRing
        self.cfg.pubRing = [tmpPath + '/pubring.gpg']
        newKeyCache.publicPaths = self.cfg.pubRing
        keyCacheCallback = openpgpkey.KeyCacheCallback(repos, self.cfg)
        newKeyCache.setCallback(keyCacheCallback)

        openpgpkey.setKeyCache(newKeyCache)

        try:
            self.updatePkg(self.rootDir, "test:doc")
            newKeyCache.getPublicKey(fingerprint)
        finally:
            self.cfg.pubRing = pubRing
            openpgpkey.setKeyCache(keyCache)
        util.rmtree(tmpPath)

    def testFileUpdateMissingKey(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'

        # make a changeset with useful stuff that could be installed
        cs = changeset.ChangeSet()
        flavor = deps.parseFlavor('')
        v = versions.VersionFromString('/%s/1.0-1-1'
                                       %self.cfg.buildLabel.asString()).copy()
        v.resetTimeStamps()
        t = trove.Trove('test:test', v, flavor, None)

        path = self.workDir + '/blah'
        f = open(path, 'w')
        f.write('hello, world!\n')
        f.close()
        pathId = sha1helper.md5String('/blah')
        f = files.FileFromFilesystem(path, pathId)

        fileId = f.fileId()
        t.addFile(pathId, '/blah', v, fileId)
        cs.addFile(None, fileId, f.freeze())
        cs.addFileContents(pathId, fileId, changeset.ChangedFileTypes.file,
                           filecontents.FromFilesystem(path),
                           f.flags.isConfig())
        diff = t.diff(None, absolute = 1)[0]
        cs.newTrove(diff)
        cs.addPrimaryTrove('test:test', v, flavor)

        # sign the changeset
        csPath = os.path.join(self.workDir, 'test-1.0-1.ccs')
        cs = cook.signAbsoluteChangeset(cs, fingerprint)
        cs.writeToFile(csPath)

        tmpPath = mkdtemp()

        keyCache = openpgpkey.getKeyCache()
        newKeyCache = openpgpkey.OpenPGPKeyFileCache()

        pubRing = self.cfg.pubRing

        self.cfg.pubRing = [tmpPath + '/pubring.gpg']
        keyCacheCallback = openpgpkey.KeyCacheCallback(None, self.cfg)
        newKeyCache.setCallback(keyCacheCallback)

        openpgpkey.setKeyCache(newKeyCache)

        try:
            self.updatePkg(self.rootDir, csPath)
        finally:
            self.cfg.pubRing = pubRing
            openpgpkey.setKeyCache(keyCache)

    def testUnaccessiblePubkey(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        pubRing = self.cfg.pubRing
        pubRing = '/not/a/valid/path/pubring.gpg'
        keyCache = openpgpkey.OpenPGPKeyFileCache()
        keyCache.setPublicPath(pubRing)
        # first seek with no callback
        self.assertRaises(openpgpfile.KeyNotFound, keyCache.getPublicKey,
                          fingerprint)
        repos = self.openRepository()
        keyCacheCallback = openpgpkey.KeyCacheCallback(repos, self.cfg)
        keyCache.setCallback(keyCacheCallback)
        # now seek with callback
        self.assertRaises(openpgpfile.KeyNotFound, keyCache.getPublicKey,
                          fingerprint)

    def testSignedClone(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.addQuickTestComponent("test:source", "1.0-1")

        v = versions.VersionFromString('/localhost@rpl:shadow/1.0-1')
        f = deps.parseFlavor('')

        self.cfg.signatureKey = fingerprint
        self.clone('/localhost@rpl:shadow',
                           'test:source=/localhost@rpl:linux')
        repos = self.openRepository()
        t = repos.getTrove('test:source', v, f)
        self._checkDigitalSig(t, fingerprint)

    def testSignedBranch(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.addQuickTestComponent("testcase:source",
                                   "/localhost@rpl:linux/1.0-1")

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1/branch/1')
        f = deps.parseFlavor('')

        self.cfg.signatureKey = fingerprint
        self.mkbranch("1.0-1", "localhost@rpl:branch",
                              "testcase:source", shadow = False)
        repos = self.openRepository()
        t = repos.getTrove('testcase:source', v, f)
        self._checkDigitalSig(t, fingerprint)

    def testSignedShadow(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.addQuickTestComponent("testcase:source",
                                   "/localhost@rpl:linux/1.0-1")

        v = versions.VersionFromString('/localhost@rpl:linux//shadow/1.0-1')
        f = deps.parseFlavor('')

        self.cfg.signatureKey = fingerprint
        self.mkbranch("1.0-1", "localhost@rpl:shadow",
                              "testcase:source", shadow = True)
        repos = self.openRepository()
        t = repos.getTrove('testcase:source', v, f)
        self._checkDigitalSig(t, fingerprint)

    # the point being that signtrove.signTroves should not complain if signatureKey is None
    def testSignNoKey(self):
        fingerprint = None

        self.addQuickTestComponent("test:doc", "1.0-1-1")
        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True
        signtrove.signTroves(self.cfg, [ "test:doc" ])

    def testInstallSignedTrove(self):
        # CNY-2555
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.addComponent('test:run', '1',
            fileContents = [('/usr/share/foo1', 'blah') ])
        self.addCollection('test', '1', [':run'])

        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True
        signtrove.signTroves(self.cfg, [ "test" ])

        # Install, have a non-existant keyring
        self.cfg.pubRing = "/some/keyring"
        openpgpkey.getKeyCache().reset()
        openpgpkey.getKeyCache().setPublicPath(self.cfg.pubRing)

        # This should change to an error when RMK-791 (related to CNY-2555) is
        # fixed
        self.logFilter.add()
        self.updatePkg("test")
        self.logFilter.remove()

        self.logFilter.compare("error: While caching PGP key: Permission denied: /some")

    # ensure selectSignatureKey actually implelemts signatureKeyMap correctly
    def testSelectSigKey(self):
        fingerprints = ['F7440D78FE813C882212C2BF8AC2828190B1E477',
                        'BOGUS',
                        '95B457D16843B21EA3FC73BBC7C32FC1F94E405E']

        cfg = conarycfg.ConaryConfiguration()

        cfg.configLine("signatureKey %s" % fingerprints[0])
        cfg.configLine("signatureKeyMap .*\.example\.com %s" % fingerprints[1])
        cfg.configLine("signatureKeyMap conary\.example\.com %s" % \
                       fingerprints[2])

        self.assertFalse(signtrove.selectSignatureKey( \
            cfg, 'conary.example.com@rpl:devel') == fingerprints[2],
                    "regexp for selectSignatureKey missed first match")

        self.assertFalse(signtrove.selectSignatureKey(cfg, 'WILDLYWRONG') != \
                    fingerprints[0],
                    "selectSignatureKey invoked signatureKeyMap incorrectly")

    def testSelectLocalKey(self):
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'

        cfg = conarycfg.ConaryConfiguration()

        cfg.configLine("signatureKey None")
        cfg.configLine("signatureKeyMap foo.example.com %s" % fingerprint)
        cfg.configLine("buildLabel foo.example.com@rpl:devel")

        self.assertFalse(not signtrove.selectSignatureKey(cfg, 'local@local'),
                    "selectSignatureKey didn't convert local to buildLabel")

    def testSelectWithLabel(self):
        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'

        labelStr = 'foo.example.com@rpl:devel'
        label = versions.Label(labelStr)

        cfg = conarycfg.ConaryConfiguration()
        cfg.configLine("signatureKey None")
        cfg.configLine("signatureKeyMap %s %s" % (labelStr, fingerprint))

        self.assertFalse(signtrove.selectSignatureKey(cfg, labelStr) != fingerprint,
                    "selectSignatureKey mismatched labelStr")

        self.assertFalse(signtrove.selectSignatureKey(cfg, labelStr) != \
                    signtrove.selectSignatureKey(cfg, label),
                    "selectSignatureKey did not accomodate a label object")

    def testUnknownSig(self):
        # the repository shouldn't blow up on this commit unless it's bene
        # run w/ requireSigs = True
        self.stopRepository(0)
        sigRepos = self.openRepository(0, requireSigs = True)

        fingerprint = 'F7440D78FE813C882212C2BF8AC2828190B1E477'
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')
        os.chdir(self.workDir)
        self.newpkg('testcase')
        os.chdir('testcase')
        self.writeFile('testcase.recipe', testRecipe)
        self.addfile('testcase.recipe')

        repos = self.openRepository()

        self.cfg.signatureKey = fingerprint

        try:
            checkin.commit(repos, self.cfg, 'testcase', None)
            self.fail("Repository should have required a signature")
        except DigitalSignatureVerificationError:
            pass

        self.stopRepository(0)
        repos = self.openRepository(0)
        checkin.commit(repos, self.cfg, 'testcase', None)

        self.cfg.signatureKey = None

    def testReposRequireSigs(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')

        callback = checkin.CheckinCallback()
        # make sure there's not a cached repo index 1
        self.stopRepository(1)
        sigRepos = self.openRepository(1, requireSigs = True)
        ascKey = open(resources.get_path('conary_test', 'archive', 'key.asc')).read()
        buildLabel = self.cfg.buildLabel
        signatureKey = self.cfg.signatureKey
        signatureKeyMap = self.cfg.signatureKeyMap
        # try block protects test suite from the alteration of self.cfg
        # or most especially, the effects of leaving a requireSigs repo around
        try:
            self.cfg.signatureKey = None
            self.cfg.signatureKeyMap = None
            self.cfg.buildLabel = versions.Label('localhost1@rpl:linux')
            sigRepos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', ascKey)
            try:
                self.addQuickTestComponent("test:doc", "/localhost1@rpl:devel/1.0-1-1")
                self.fail("Repository should have required a signature")
            except DigitalSignatureVerificationError:
                pass

            name = 'testcase'
            fullName = name + '=/localhost1@rpl:linux'
            # test commit codepath.
            origDir = os.getcwd()
            try:
                os.chdir(self.workDir)
                self.newpkg(name)
                os.chdir(name)
                self.writeFile(name + '.recipe', testRecipe)
                self.addfile(name + '.recipe')
                try:
                    checkin.commit(sigRepos, self.cfg, 'foobar', callback)
                    self.fail("Repository should have rejected  commit")
                except DigitalSignatureVerificationError:
                    pass
                # but we really need a source trove for the rest of the paths
                self.cfg.signatureKey = fingerprint
                checkin.commit(sigRepos, self.cfg, fullName, None)
                self.cfg.signatureKey = None
            finally:
                os.chdir(origDir)

           # test cook codepath
            try:
                self.cookItem(sigRepos, self.cfg, fullName,
                              callback = callback, ignoreDeps=True)
                self.fail("Repository should have rejected cook")
            except DigitalSignatureVerificationError:
                pass
            self.cfg.signatureKey = fingerprint
            self.cookItem(sigRepos, self.cfg, fullName,
                              callback = callback, ignoreDeps=True)
            self.cfg.signatureKey = None

            # test clone codepath
            try:
                self.clone('/localhost1@rpl:shadow',
                           'testcase:source=/localhost1@rpl:linux')
                self.fail("Repository should have rejected clone")
            except DigitalSignatureVerificationError:
                pass

            # test branch codepath
            try:
                self.mkbranch("1.0-1", "localhost1@rpl:shadow",
                              "testcase:source", shadow = False)
                self.fail("Repository should have rejected branch")
            except DigitalSignatureVerificationError:
                pass

            # test shadow codepath
            try:
                self.mkbranch("1.0-1", "localhost1@rpl:shadow",
                              "testcase:source", shadow = True)
                self.fail("Repository should have rejected shadow")
            except DigitalSignatureVerificationError:
                pass

        finally:
            self.cfg.buildLabel = buildLabel
            self.cfg.signatureKey = signatureKey
            self.cfg.signatureKeyMap = signatureKeyMap
            # this repo MUST be destroyed, other tests will fail against it.
            self.stopRepository(1)
            sigRepos = self.openRepository(1)
        self.addQuickTestComponent("test:doc",
                                   "/localhost1@rpl:devel/1.0-1-1")

    def testGetKeyFromRepo(self):
        refKeyData = "-----BEGIN PGP PUBLIC KEY BLOCK-----\nVersion: Conary %s\n\nmIsEQwCyngEEAJ3MC9HwzDve2JzvEdhS/eWfjLjtE1Hco/BxZEri4oz4XTScjmbmgPGrk84F\n2Pxb3sqNP3pESjKZO8yNYj5pF9ToVO3uzWKIRk3XGuu1MwheKO0ruOKS6wUJo1x2scrAHY5Y\noOFAQ1SGHWdX8qAHoupDEIO/35bHMeoD1BQeDmGVAAYptC1SU0EgMTAyNCB3aXRoIEl0ZXJh\ndGVkIFMySyBTSEEgQ2hlY2tzdW0gQ0FTVDWItAQTAQIAHgUCQwCyngIbAwYLCQgHAwIDFQID\nAxYCAQIeAQIXgAAKCRDHwy/B+U5AXijKA/0UbwMU5Dg2EMekqgUp4mirm+fFjV+S5fRZnY4N\ngj8wXN7BbEiVl9RTBt6O+abAEXWYNw85Nlkq77W49qz/XewymaiF9Q6ELJ1XQBKM7bwKRmmw\nO2dQS6eCQaRfOQ9XDeG01oWQDg+ISriSc9WlTdf+aP2VieB2wK55bUw4NWijtg==\n=OcwF\n-----END PGP PUBLIC KEY BLOCK-----" % version
        repos = self.openRepository()
        try:
            repos.getAsciiOpenPGPKey(self.cfg.buildLabel, 'NOT_A_VALID_FINGERPINT')
            self.fail("Didn't receive exception from invalid fingerprint")
        except openpgpkey.KeyNotFound:
            pass

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        keyData = repos.getAsciiOpenPGPKey(self.cfg.buildLabel, fingerprint)
        self.assertEqual(keyData, refKeyData)

    def testListRepoKeys(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        refUserIds = ['RSA 1024 with Iterated S2K SHA Checksum CAST5']
        repos = self.openRepository()
        fingList = repos.listUsersMainKeys(self.cfg.buildLabel, 'test')
        if fingList != [fingerprint]:
            self.fail("getUsersMainKeys returned incorrect results: expected %s but got %s" % (str([fingerprint]), str(fingList)))
        subKeyList = repos.listSubkeys(self.cfg.buildLabel, fingerprint)
        if subKeyList != []:
            self.fail("getSubkeys returned values where none should have been")
        keyUserIds = repos.getOpenPGPKeyUserIds(self.cfg.buildLabel, fingerprint)
        if keyUserIds != refUserIds:
            self.fail("getOpenPGPKeyUserIds return incorrect results: expected %s but got %s" %(refUserIds,keyUserIds))

        limitedRepos = self.setupUser(repos, self.cfg.buildLabel, 'member', 'foo', None, None)
        self.assertRaises(errors.InsufficientPermission, limitedRepos.listUsersMainKeys,
                          self.cfg.buildLabel, 1)
        badPwRepos = self.getRepositoryClient(user = 'test', password = 'busted')
        self.assertRaises(errors.InsufficientPermission,
            badPwRepos.listUsersMainKeys, self.cfg.buildLabel, 1)

    def testRecursiveGroupSigs(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')
        # make all components before assigning a key, so all start out blank
        self.addQuickTestComponent("test:doc", "1.0-1-1")

        self.makeSourceTrove('testcase', testRecipe)

        repos = self.openRepository()
        built = self.cookItem(repos, self.cfg, 'testcase', ignoreDeps = True)
        group = self.build(testGroup, "GroupTest")
        group = self.build(outerGroup, "OuterGroup")

        signatureKey = self.cfg.signatureKey
        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')

        try:
            # first sign the outer group trove
            #signtrove.signTroves(self.cfg, [ "group-outer" ])
            cvccmd.sourceCommand(self.cfg, ["sign", "group-outer"],
                              {'recurse' : True})

            # check the sig on each trove in the group, since group signing
            # should be recursive
            for trvName in ('group-outer', 'group-test', 'test:doc',
                            'testcase', 'testcase:runtime'):
                try:
                    t = repos.getTrove(trvName, v, f)
                    signature = t.getDigitalSignature(fingerprint)
                    self._checkDigitalSig(t, fingerprint)
                except openpgpfile.KeyNotFound:
                    self.fail('Recursive group signing failed to cover %s' %
                              trvName)
            # then check that the source trove wasn't signed... it wasn't
            # explicitly part of the group
            v = versions.VersionFromString('/localhost@rpl:linux/1.0-1')
            f = deps.parseFlavor('')
            t = repos.getTrove('testcase:source', v, f)
            try:
                t.getDigitalSignature(fingerprint)
                self.fail('signing group inadvendently signed source trove')
            except openpgpfile.KeyNotFound:
                pass

        finally:
            self.cfg.signatureKey = signatureKey

    def testNonRecursiveGroupSigs(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # supply the pass phrase for our private key
        keyCache = openpgpkey.getKeyCache()
        keyCache.getPrivateKey(fingerprint, '111111')
        # make all components before assigning a key, so all start out blank
        self.addQuickTestComponent("test:doc", "1.0-1-1")

        self.makeSourceTrove('testcase', testRecipe)

        repos = self.openRepository()
        built = self.cookItem(repos, self.cfg, 'testcase', ignoreDeps = True)
        group = self.build(testGroup, "GroupTest")
        group = self.build(outerGroup, "OuterGroup")

        signatureKey = self.cfg.signatureKey
        self.cfg.signatureKey = fingerprint
        self.cfg.quiet = True

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')

        try:
            # first sign the outer group trove
            #signtrove.signTroves(self.cfg, [ "group-outer" ])
            cvccmd.sourceCommand(self.cfg, ["sign", "group-outer"], {})

            try:
                t = repos.getTrove('group-outer', v, f)
                self._checkDigitalSig(t, fingerprint)
            except openpgpfile.KeyNotFound:
                self.fail('Group signing failed to cover %s' % t)

            # check the sig on each trove in the group, since group signing
            # should be recursive
            for trvName in ('group-test', 'test:doc', 'testcase',
                            'testcase:runtime'):
                try:
                    t = repos.getTrove(trvName, v, f)
                    t.getDigitalSignature(fingerprint)
                    self.fail('Group signing inadvenderntly signed %s' %
                              trvName)
                except openpgpfile.KeyNotFound:
                    pass
            # then check that the source trove wasn't signed... it wasn't
            # explicitly part of the group
            v = versions.VersionFromString('/localhost@rpl:linux/1.0-1')
            f = deps.parseFlavor('')
            t = repos.getTrove('testcase:source', v, f)
            try:
                signature = t.getDigitalSignature(fingerprint)
                self._checkDigitalSig(t, fingerprint)
                self.fail('signing group inadvendently signed source trove')
            except openpgpfile.KeyNotFound:
                pass

        finally:
            self.cfg.signatureKey = signatureKey

    def testSignatureAcls(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        # make all components before assigning a key, so all start out blank
        self.addQuickTestComponent("test:doc", "1.0-1-1")
        self.cfg.signatureKey = fingerprint
        rootLabel = versions.Label("localhost@rpl:linux")

        repos = self.openRepository()
        self.addUserAndRole(repos, rootLabel, 'foo', 'bar')
        repos.addAcl(rootLabel, 'foo', '.*', None)
        oldUser = self.cfg.user
        try:
            self.cfg.user = conarycfg.UserInformation()
            self.cfg.user.addServerGlob('localhost', 'foo', 'bar')
            self.assertRaises(errors.InsufficientPermission,
                    cvccmd.sourceCommand, self.cfg, ["sign", "test:doc"], {})
        finally:
            self.cfg.user = oldUser

    def testSubkeys(self):
        noSubKey = \
        "-----BEGIN PGP PUBLIC KEY BLOCK-----\n" \
        "Version: GnuPG v1.4.1 (GNU/Linux)\n" \
        "\n" \
        "mIsEQ9KKFQEEAMC+vuNBElp8lcqLnxMq46KyAfi2hrCJBbNV574dT3PyunDxG8Hg\n" \
        "ya9tbbBR1XBkTuepK7gXDGatrgnz9+GMZdFTAR7d1dl9ImCM6JvF8xKRiFQ6zyOL\n" \
        "1DanpmTztGrbASIRPQ/FpN2GS/x57S4fyK4P5xRv8yl7LThoUjhn6aULAAYptCJD\n" \
        "b25hcnkgdGVzdCBzdWl0ZSA8cm9vdEBsb2NhbGhvc3Q+iLQEEwECAB4FAkPSihUC\n" \
        "GwMGCwkIBwMCAxUCAwMWAgECHgECF4AACgkQMrAPW69lMl0yeAP+JthLveHvYZui\n" \
        "rqRYWaN1AybT+npLjQHHe2JcztYN/reZdFTRo2NdItB8zY+N0CDi28nu0mroFgky\n" \
        "Zc+pKq+yiJgV9er9tFAYIRugN2WiYuiAMJobdTOTy3EwFjE2OoPChnmLqzN8oOAq\n" \
        "+eQsAquptgG01pgIiEMn2+r+FW8JWUU=\n" \
        "=DoK/\n" \
        "-----END PGP PUBLIC KEY BLOCK-----\n"

        withSubKey = \
        "-----BEGIN PGP PUBLIC KEY BLOCK-----\n" \
        "Version: GnuPG v1.4.1 (GNU/Linux)\n" \
        "\n" \
        "mIsEQ9KKFQEEAMC+vuNBElp8lcqLnxMq46KyAfi2hrCJBbNV574dT3PyunDxG8Hg\n" \
        "ya9tbbBR1XBkTuepK7gXDGatrgnz9+GMZdFTAR7d1dl9ImCM6JvF8xKRiFQ6zyOL\n" \
        "1DanpmTztGrbASIRPQ/FpN2GS/x57S4fyK4P5xRv8yl7LThoUjhn6aULAAYptCJD\n" \
        "b25hcnkgdGVzdCBzdWl0ZSA8cm9vdEBsb2NhbGhvc3Q+iLQEEwECAB4FAkPSihUC\n" \
        "GwMGCwkIBwMCAxUCAwMWAgECHgECF4AACgkQMrAPW69lMl0yeAP+JthLveHvYZui\n" \
        "rqRYWaN1AybT+npLjQHHe2JcztYN/reZdFTRo2NdItB8zY+N0CDi28nu0mroFgky\n" \
        "Zc+pKq+yiJgV9er9tFAYIRugN2WiYuiAMJobdTOTy3EwFjE2OoPChnmLqzN8oOAq\n" \
        "+eQsAquptgG01pgIiEMn2+r+FW8JWUW4iwRD0oudAQQAtPmr1UCuJW9Q4fflhwSK\n" \
        "TZX4tBR+G7nVqk336kiqnbEyPR4MHfOx0e4wko+f0MpxrQbiHDTgcR4z2eIkMh8H\n" \
        "9ssY/Rot/Y0c+fh2m7/nc3lEPZS2le7bEVvK1VvFeRXC+aiCQ8JeHp0j32ae7NYz\n" \
        "TB96khEaX434+Bl9s3ZqDuEABimInwQYAQIACQUCQ9KLnQIbDAAKCRAysA9br2Uy\n" \
        "XaYrA/9dwgkR/VWGSkpVHPgd0wxHc5zzowitR8CECop5o1DwgleV7enoqgQTODz/\n" \
        "u3Gvjvw1RtXTY0gtB6m6CskkDcCl9wykbT/NpeZoRnPDZqbtd/8rSheaWoT6VytS\n" \
        "mW+TV23gbX1dYZt3tkUNBMJMs+B4gEwF9q9/FmvOcUDLB/1JBw==\n" \
        "=/Q6d\n" \
        "-----END PGP PUBLIC KEY BLOCK-----\n"

        fingerprint = "DD9F19D2B303A3D767E9994232B00F5BAF65325D"

        repos = self.openRepository()
        # add it w/o the subkey
        repos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', noSubKey)

        # and now with the subkey
        repos.addNewAsciiPGPKey(self.cfg.buildLabel, 'test', withSubKey)

        subKeyList = repos.listSubkeys(self.cfg.buildLabel, fingerprint)
        assert(subKeyList == [ '8B475E45DE1E91E982E6FD274325C94F6DAA5B21' ] )

    def testCookPackage(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.makeSourceTrove('testcase', testRecipe)

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')
        repos = self.openRepository()

        # excercise signatureKeyMap codepath
        signatureKeyMap = self.cfg.signatureKeyMap
        try:
            self.cfg.signatureKeyMap = [('localhost@rpl:linux', fingerprint)]

            built = self.cookItem(repos, self.cfg, 'testcase',
                                  ignoreDeps = True)
            trv = repos.getTrove('testcase', v, f)


            self.assertFalse(not trv.getSigs().digitalSigs.freeze(),
                        "Package was not signed when cooked")

            self.assertFalse(trv.verifyDigitalSignatures() !=
                (openpgpfile.TRUST_TRUSTED, [], set()),
                "Bad digital signature for cooked package")
        finally:
            self.cfg.signatureKeyMap = signatureKeyMap

    def testCookPackageCascade(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        self.makeSourceTrove('testcase', testRecipe)

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')
        repos = self.openRepository()

        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint

            built = self.cookItem(repos, self.cfg, 'testcase',
                                  ignoreDeps = True)

            # ensure components of cooked trove are also signed.
            trv = repos.getTrove('testcase:runtime', v, f)

            self.assertFalse(not trv.getSigs().digitalSigs.freeze(),
                        "Component was not signed when package was cooked")

            self.assertFalse(trv.verifyDigitalSignatures() !=
                   (openpgpfile.TRUST_TRUSTED, [], set()),
                        "Bad digital signature for coponent of cooked package")
        finally:
            self.cfg.signatureKey = signatureKey

    def testCookFileset(self):
        from buildtest.filesettest import basicFileset, packageRecipe

        self.buildRecipe(packageRecipe, "testRecipe")

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint

            pkg = self.build(basicFileset, "basicFileset")

            self.assertFalse(not pkg.getSigs().digitalSigs.freeze(),
                        "Fileset was not signed when cooked")

            self.assertFalse(pkg.verifyDigitalSignatures() !=
                    (openpgpfile.TRUST_TRUSTED, [], set()),
                    "Bad digital signature for cooked fileset")
        finally:
            self.cfg.signatureKey = signatureKey

    def testCookFilesetCascade(self):
        from buildtest.filesettest import basicFileset, packageRecipe

        repos = self.openRepository()
        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')
        self.buildRecipe(packageRecipe, "testRecipe")

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint

            pkg = self.build(basicFileset, "basicFileset")

            trv = repos.getTrove('test', v, f)

            # signatures should not propogate.
            self.assertFalse(trv.getSigs().digitalSigs.freeze(),
                        "Signature cascaded during fileset cook")
        finally:
            self.cfg.signatureKey = signatureKey

    def testCookGroup(self):
        self.addComponent("testcase:runtime", "1.0")
        self.addCollection("testcase", "1.0", [ ":runtime"])

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint
            grp = self.build(testGroup, "GroupTest")

            self.assertFalse(not grp.getSigs().digitalSigs.freeze(),
                        "Group was not signed when cooked")

            self.assertFalse(grp.verifyDigitalSignatures() !=
                    (openpgpfile.TRUST_TRUSTED, [], set()),
                    "Bad digital signature for gooked group")
        finally:
            self.cfg.signatureKey = signatureKey

    def testCookGroupCascade(self):
        self.addComponent("testcase:runtime", "1.0")
        self.addCollection("testcase", "1.0", [ ":runtime"])

        repos = self.openRepository()

        v = versions.VersionFromString('/localhost@rpl:linux/1.0-1-1')
        f = deps.parseFlavor('')

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint
            grp = self.build(testGroup, "GroupTest")

            trv = repos.getTrove('testcase', v, f)

            # signatures should not propogate.
            self.assertFalse(trv.getSigs().digitalSigs.freeze(),
                        "Signature cascaded during group cook")
        finally:
            self.cfg.signatureKey = signatureKey

    def testCookRedirect(self):
        from conary_test.redirecttest import packageRecipe, redirectBaseRecipe, \
             redirectRecipe

        self.build(packageRecipe, "testRecipe")
        self.build(redirectBaseRecipe, "testRedirect")

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint
            pkg = self.build(redirectRecipe, "testRedirect")
            self.assertFalse(not pkg.getSigs().digitalSigs.freeze(),
                        "Redirect was not signed when cooked")

            self.assertFalse(pkg.verifyDigitalSignatures() !=
                    (openpgpfile.TRUST_TRUSTED, [], set()),
                    "Bad digital signature for cooked redirect")
        finally:
            self.cfg.signatureKey = signatureKey

    def testCookRedirectCascade(self):
        from conary_test.redirecttest import packageRecipe, redirectBaseRecipe, \
             redirectRecipe

        trv = self.build(packageRecipe, "testRecipe")
        self.build(redirectBaseRecipe, "testRedirect")

        v = trv.version()
        f = trv.flavor()
        repos = self.openRepository()

        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'
        signatureKey = self.cfg.signatureKey
        try:
            self.cfg.signatureKey = fingerprint
            pkg = self.build(redirectRecipe, "testRedirect")

            trv = repos.getTrove('test:runtime', v, f)

            # signatures should not propogate.
            self.assertFalse(trv.getSigs().digitalSigs.freeze(),
                        "Signatures cascaded during redirect cook")
        finally:
            self.cfg.signatureKey = signatureKey

    def testAddingSigs(self):
        fingerprint = '95B457D16843B21EA3FC73BBC7C32FC1F94E405E'

        # get the public key
        keyRing = open(resources.get_path('conary_test', 'archive', 'pubring.gpg'))
        keyData = openpgpfile.exportKey(fingerprint, keyRing)
        keyData.seek(0)
        keyData = keyData.read()

        # force the repository to be setup before we use a broken password
        # to access it
        self.openRepository()
        repos = self.getRepositoryClient(user = 'test', password = 'busted')
        self.assertRaises(errors.InsufficientPermission,
            repos.addNewPGPKey, self.cfg.buildLabel, 'test', keyData)

        self.assertRaises(errors.InsufficientPermission,
            repos.addNewAsciiPGPKey, self.cfg.buildLabel, 'test', unexpiredKey)
