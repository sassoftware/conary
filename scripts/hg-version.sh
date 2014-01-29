#!/bin/bash
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


top=$(dirname $0)/..
if [[ -x /usr/bin/hg && -d "$top/.hg" ]]
then
    hg id -i
elif [[ -x /usr/bin/git && -d "$top/.git" ]]
then
    rev=$(git rev-parse --short=12 HEAD)
    if ! git diff-index --quiet HEAD
    then
        rev="${rev}+"
    fi
    echo "$rev"
elif [[ -f "$top/.hg_archival.txt" ]]
then
    grep node $top/.hg_archival.txt |cut -d' ' -f 2 |head -c 12
    echo
elif grep -qv '$Format' "$top/.commit_id.txt"
then
    head -c 12 "$top/.commit_id.txt"
    echo
fi
