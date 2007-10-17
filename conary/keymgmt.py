#
# Copyright (c) 2007 rPath, Inc.
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
