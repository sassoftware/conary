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

from conary.lib.http import connection


class Opener(object):

    connectionFactory = connection.Connection

    def __init__(self, proxyMap=None, caCerts=None):
        self.proxyMap = proxyMap
        self.caCerts = caCerts
        self.cache = {}

    def clear(self):
        self.cache.clear()

    def open(self, url, body=None, headers=None):
        pass
