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


def splitSocketError(error):
    """Break a socket.error into a message and a "everything else" tuple."""
    if len(error.args) > 1:
        msg = error[1]
        args = (error[0],)
    else:
        msg = error[0]
        args = ()
    return args, msg
