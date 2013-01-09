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
