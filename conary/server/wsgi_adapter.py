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

import webob
import sys


def modpython_to_webob(mpreq, handler):
    # This could be written as a mod_python -> WSGI gateway, but this is much
    # more compact.
    from mod_python import apache
    mpreq.add_common_vars()
    environ = dict(mpreq.subprocess_env.items())
    environ['wsgi.version'] = (1, 0)
    if environ.get('HTTPS', '').lower() == 'on':
        environ['wsgi.url_scheme'] = 'https'
    else:
        environ['wsgi.url_scheme'] = 'http'
    environ['wsgi.input'] = mpreq
    environ['wsgi.errors'] = sys.stderr
    environ['wsgi.multithread'] = False
    environ['wsgi.multiprocess'] = True
    environ['wsgi.run_once'] = False

    request = webob.Request(environ)
    response = handler(request)

    mpreq.status = response.status_int
    for key, value in response.headerlist:
        if key.lower() == 'content-length':
            mpreq.set_content_length(int(value))
        elif key.lower() == 'content-type':
            mpreq.content_type = value
        else:
            mpreq.headers_out.add(key, value)
    for chunk in response.app_iter:
        mpreq.write(chunk)
    return apache.OK
