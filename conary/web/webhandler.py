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


import os

# apache imports
from mod_python import apache
from mod_python import Cookie
from mod_python.util import FieldStorage

# helper class for web handlers
class WebHandler(object):
    content_type = 'text/html; charset=utf-8'

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
        raise apache.SERVER_RETURN, apache.HTTP_MOVED_TEMPORARILY

    def _handle(self):
        # both GET and POST events are treated the same way
        method = self.req.method.upper()
        if method in ("GET", "POST"):
            return self._methodHandler()
        else:
            return apache.HTTP_METHOD_NOT_ALLOWED
