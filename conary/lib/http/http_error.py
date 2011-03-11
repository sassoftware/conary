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

    def __init__(self, url, proxy, errcode, reason):
        TransportError.__init__(self, url, proxy, errcode, reason)
        self.url = url
        self.proxy = proxy
        self.errcode = errcode
        self.reason = reason

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
