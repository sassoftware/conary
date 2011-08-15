#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
