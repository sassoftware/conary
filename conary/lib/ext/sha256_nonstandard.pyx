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


include "common.pxi"

cdef extern from "helper_sha256_nonstandard.h" nogil:
    void sha256ns_digest(unsigned char *data, int len, unsigned char *digest)


def digest(data):
    cdef unsigned char buf[32]
    sha256ns_digest(data, len(data), buf)
    return PyString_FromStringAndSize(<char*>buf, 32)
