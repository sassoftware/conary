#
# Copyright (c) 2005 rpath, Inc.
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

# kid imports
import kid

# apache imports
from mod_python import apache
from mod_python import Cookie
from mod_python.util import FieldStorage

import webauth


class WebHandler(object):
    def _checkAuth(self, authToken):
        raise NotImplementedError

    def _getHandler(self, cmd, auth):
        """Overrideable method to return a handler method in an application-specific way
           Needs to raise AttributeError if that method does not exist."""
        raise NotImplementedError

    def __init__(self, req, cfg):
        self.req = req
        self.cfg = cfg

    def _redirect(self, location):
        self.req.headers_out['Location'] = location
        return apache.HTTP_MOVED_PERMANENTLY

    def _handle(self):
        # both GET and POST events are treated the same way
        method = self.req.method.upper()
        if method in ("GET", "POST"):
            return self._method_handler()
        else:
            return apache.HTTP_METHOD_NOT_ALLOWED

    def _method_handler(self):
        cookies = Cookie.get_cookies(self.req, Cookie.Cookie) 
        if 'authToken' in cookies:
            auth = base64.decodestring(cookies['authToken'].value)
            self.authToken = auth.split(":")

            try:
                auth = self._checkAuth(self.authToken)
            except NotImplementedError:
                auth = webauth.Authorization()

            if not auth.passwordOK:
                cookie = Cookie.Cookie('authToken', '')
                cookie.expires = time.time() - 300
                Cookie.add_cookie(self.req, cookie)
                return self._redirect("login")
        else:
            auth = webauth.Authorization()
        self.auth = auth

        cmd = self.req.path_info
        if cmd.startswith("/"):
            cmd = cmd[1:]

        self.req.content_type = "text/html"
        if cmd.startswith("_"):
            return apache.HTTP_NOT_FOUND 
        try:
            method = self._getHandler(cmd, auth)
        except AttributeError:
            return apache.HTTP_NOT_FOUND
        self.fields = FieldStorage(self.req)

        d = dict(self.fields)
        d['auth'] = self.auth
        return method(**d)

    def _write(self, template, **values):
        template.write(self.req, encoding="utf-8", cfg = self.cfg, 
                       req=self.req, **values)

