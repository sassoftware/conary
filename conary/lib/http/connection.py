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

"""
Extensions to the "HTTPConnection" class available from the httplib standard
library module.
"""

import glob
import httplib
import os


try:
    # Use m2crypto for checking server certificates
    from M2Crypto import SSL
    SSLVerificationError = SSL.Checker.SSLVerificationError
except ImportError:
    SSL = None

    class SSLVerificationError(Exception):
        # If M2Crypto is not installed, no verification is performed, so this
        # is just a placeholder to simplify exception handling
        pass


class HTTPSConnection(httplib.HTTPConnection):
    """
    HTTPS connection that supports m2crypto contexts plus some other features.

    m2crypto's httpslib isn't used here because it is too simple to bother
    inheriting.

    Currently supported "extra" features:
     * Can pass in a list of peer certificate authorities.
     * Can set the hostname used to check the peer's certificate.
    """
    default_port = httplib.HTTPS_PORT

    def __init__(self, host, port=None, strict=None, caCerts=None,
            commonName=None):
        httplib.HTTPConnection.__init__(self, host, port, strict)
        self.caCerts = caCerts
        self.commonName = commonName

        self.ssl_ctx = SSL.Context('sslv23')
        if caCerts:
            self.ssl_ctx.set_verify(SSL.verify_peer, depth=9)
            paths = []
            for path in caCerts:
                paths.extend(sorted(list(glob.glob(path))))
            for path in paths:
                if os.path.isdir(path):
                    self.ssl_ctx.load_verify_locations(capath=path)
                elif os.path.exists(path):
                    self.ssl_ctx.load_verify_locations(cafile=path)

    def connect(self):
        self.sock = SSL.Connection(self.ssl_ctx)
        self.sock.clientPostConnectionCheck = self.checkSSL
        self.sock.connect((self.host, self.port))

    def adopt(self, sock):
        """
        Set this connection's underlying socket to C{sock} and wrap it with the
        SSL connection object. Assume the socket is already open but has not
        exchanged any SSL traffic.
        """
        self.sock = SSL.Connection(self.ssl_ctx, sock)
        self.sock.setup_ssl()
        self.sock.set_connect_state()
        self.sock.connect_ssl()
        if not self.checkSSL(self.sock.get_peer_cert(), self.host):
            raise SSLVerificationError('post connection check failed')

    def close(self):
        # See M2Crypto/httpslib.py:67
        pass

    def checkSSL(self, cert, host):
        """
        Peer cert checker that will use an alternate hostname for the
        comparison, e.g. if the actual connect host is an IP this can be used
        to specify the original hostname.
        """
        if self.commonName:
            host = self.commonName
        checker = SSL.Checker.Checker()
        return checker(cert, host)
