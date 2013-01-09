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
    for key, value in mpreq.headers_in.items():
        key = 'HTTP_' + key.upper().replace('-', '_')
        environ[key] = value

    request = webob.Request(environ)
    response = handler(request)

    mpreq.status = response.status_int
    def start_response(status, headers, exc_info=None):
        for key, value in response.headerlist:
            if key.lower() == 'content-length':
                mpreq.set_content_length(int(value))
            elif key.lower() == 'content-type':
                mpreq.content_type = value
            else:
                mpreq.headers_out.add(key, value)
        return mpreq
    for chunk in response(environ, start_response):
        mpreq.write(chunk)
    return apache.OK
