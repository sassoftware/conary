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


class RequestError(RuntimeError):
    """Failed to send the HTTP request."""

    def __init__(self, wrapped):
        self.wrapped = wrapped
        RuntimeError.__init__(self)


class AbortError(Exception):
    pass


class TransportError(Exception):
    pass


class ParameterError(TransportError):
    pass


class ResponseError(TransportError):

    def __init__(self, url, proxy, errcode, reason, headers=None):
        TransportError.__init__(self, url, proxy, errcode, reason)
        self.url = url
        self.proxy = proxy
        self.errcode = errcode
        self.reason = reason
        self.headers = headers

    def __str__(self):
        if self.proxy:
            via = " via %s proxy %s" % (self.proxy.scheme, self.proxy.hostport,)
        else:
            via = ""
        safe_url = str(self.url)
        if hasattr(safe_url, '__safe_str__'):
            safe_url = safe_url.__safe_str__()
        return "Error opening %s%s: %s %s" % (safe_url, via, self.errcode,
                self.reason)


def splitSocketError(error):
    """Break a socket.error into a message and a "everything else" tuple."""
    if len(error.args) > 1:
        msg = error[1]
        args = (error[0],)
    else:
        msg = error[0]
        args = ()
    return args, msg
