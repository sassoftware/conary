#
# Copyright (c) SAS Institute Inc.
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

import logging
import os
from conary.lib import log as cny_log
from conary.server import wsgi_hooks

log = logging.getLogger(__name__)


def _getVhostDir(environ):
    if 'CONARY_VHOST_DIR' in os.environ:
        vhostDir = os.environ['CONARY_VHOST_DIR']
    elif 'CONARY_VHOST_DIR' in environ:
        vhostDir = environ['CONARY_VHOST_DIR']
    if vhostDir and os.path.isdir(vhostDir):
        return vhostDir
    else:
        return None


def application(environ, start_response):
    cny_log.setupLogging(consoleLevel=logging.INFO, consoleFormat='apache')

    vhostDir = _getVhostDir(environ)
    if not vhostDir:
        log.error("The CONARY_VHOST_DIR environment variable must be set to "
                "an existing directory")
        start_response('500 Internal Server Error',
                [('Content-Type', 'text/plain')])
        return ["ERROR: The server is not configured correctly. Check the "
            "server's error logs.\r\n"]

    httphost = environ.get('HTTP_HOST', '').split(':')[0]
    if not httphost:
        start_response('400 Bad Request', [('Content-Type', 'text/plain')])
        return ["ERROR: No server name was supplied\r\n"]
    repohost = environ.get('HTTP_X_CONARY_SERVERNAME', '')
    for var in (httphost, repohost):
        if '..' in var or '/' in var or os.path.sep in var:
            start_response('400 Bad Request', [('Content-Type', 'text/plain')])
            return ["ERROR: Illegal header value\r\n"]
        if var:
            path = os.path.join(vhostDir, var)
            if os.path.isfile(path):
                break
    else:
        log.error("vhost path %s not found", path)
        start_response('404 Not Found', [('Content-Type', 'text/plain')])
        names = httphost
        if repohost and repohost != httphost:
            names += ' or ' + repohost
        return ["ERROR: No server named %s exists here\r\n" % names]
    environ['conary.netrepos.config_file'] = path
    return wsgi_hooks.makeApp({})(environ, start_response)
