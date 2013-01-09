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


from conary import conaryclient
from conary.lib import openpgpfile

import sys

def addKey(cfg, server, user):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if server is None:
        server = cfg.buildLabel.getHost()

    if user is None:
        user = cfg.user.find(server)[0]

    asciiKey = sys.stdin.read()
    binaryKey = openpgpfile.parseAsciiArmorKey(asciiKey)

    repos.addNewPGPKey(server, user, binaryKey)

def displayKeys(cfg, server, user, showFingerprints = False):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if server is None:
        server = cfg.buildLabel.getHost()

    if user is None:
        user = cfg.user.find(server)[0]

    fingerPrints = repos.listUsersMainKeys(server, user)

    if showFingerprints:
        i = 0
    else:
        i = -8

    if not fingerPrints:
        print 'No keys found for user %s on server %s.' % (user, server)
    else:
        print 'Public key fingerprints for user %s on server %s:' \
                % (user, server)
        print "\n".join("    %s" % fp[i:] for fp in fingerPrints)

def showKey(cfg, server, fingerprint):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    if server is None:
        server = cfg.buildLabel.getHost()

    key = repos.getAsciiOpenPGPKey(server, fingerprint)
    print key
