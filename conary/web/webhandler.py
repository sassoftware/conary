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
import base64
import os

# kid imports
import kid

# apache imports
from mod_python import apache
from mod_python import Cookie
from mod_python.util import FieldStorage

import webauth

# helper class for web handlers
class WebHandler(object):
    content_type = "application/xhtml+xml"

    def _checkAuth(self, authToken):
        raise NotImplementedError

    def _getHandler(self, cmd, auth):
        """Overrideable method to return a handler method in an application-specific way
           Needs to raise AttributeError if that method does not exist."""
        raise NotImplementedError

    def _methodHandler(self):
        raise NotImplementedError

    def __init__(self, req, cfg):
        self.req = req
        self.cfg = cfg

        self.fields = FieldStorage(self.req)
        self.cookies = Cookie.get_cookies(self.req, Cookie.Cookie)
        self.writeFn = self.req.write
        
        self.req.content_type = self.content_type
        self.cmd = os.path.basename(self.req.path_info)
        if self.cmd.endswith("/"):
            self.cmd = self.cmd[:-1]
        
        if self.cmd.startswith('_'):
            raise apache.SERVER_RETURN, apache.HTTP_NOT_FOUND

    def _redirect(self, location):
        self.req.headers_out['Location'] = location
        raise apache.SERVER_RETURN, apache.HTTP_MOVED_PERMANENTLY

    def _handle(self):
        # both GET and POST events are treated the same way
        method = self.req.method.upper()
        if method in ("GET", "POST"):
            return self._methodHandler()
        else:
            return apache.HTTP_METHOD_NOT_ALLOWED
