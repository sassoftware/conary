#!/bin/sh
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


hgDir=`dirname $0`/..
if [[ -x /usr/bin/hg && -d $hgDir/.hg ]] ; then
    rev=`hg id -i`
elif [ -f $hgDir/.hg_archival.txt ]; then
    rev=`grep node $hgDir/.hg_archival.txt |cut -d' ' -f 2 |head -c 12`;
else
    rev= ;
fi ;
echo "$rev"
