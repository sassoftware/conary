#
# Copyright (c) 2005 Specifix, Inc.
#
# All rights reserved
#

# kid imports
import kid

# apache imports
from mod_python import apache
from mod_python import Cookie
from mod_python.util import FieldStorage

import webauth

HMACsalt = "salt"

class WebHandler(object):
    def _checkAuth(self, authToken):
        raise NotImplementedError

    def _getHandler(self, cmd):
        """Overrideable method to return a handler method in an application-specific way
           Needs to raise AttributeError if that method does not exist."""
        raise NotImplementedError

    def __init__(self, req, cfg):
        self.req = req
        self.cfg = cfg

    def _redirect(self, location):
        self.req.headers_out['Location'] = location
        return apache.HTTP_MOVED_TEMPORARILY

    def _handle(self):
        # both GET and POST events are treated the same way
        method = self.req.method.upper()
        if method in ("GET", "POST"):
            return self._method_handler()
        else:
            return apache.HTTP_METHOD_NOT_ALLOWED

    def _method_handler(self):
        cookies = Cookie.get_cookies(self.req, 
                                     Cookie.MarshalCookie, 
                                     secret = HMACsalt)
        if 'authToken' in cookies:
            self.authToken = cookies['authToken'].value

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
            method = self._getHandler(cmd)
        except AttributeError:
            return apache.HTTP_NOT_FOUND
        self.fields = FieldStorage(self.req)

        import sys
        print >>sys.stderr, method
        sys.stderr.flush()

        d = dict(self.fields)
        d['auth'] = self.auth
        return method(**d)

    def _getTemplate(self, name):
        cfg = self.cfg
        templatePath = cfg.fsRoot + cfg.webDir + '/' +  name + '.kid'
        t = kid.load_template(templatePath)
        return t

    def _write(self, template, **values):
        template.write(self.req, encoding="utf-8", cfg = self.cfg, 
                       req=self.req, **values)

