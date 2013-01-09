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


"""
The lib module provides code utility functions which are not
conary-specific.

@group Public Interfaces: util, openpgpkey
"""
# BW compatibility - old code will expect to see MainHandler in
# options.py.
from conary.lib.command import AbstractCommand
from conary.lib.mainhandler import MainHandler
from conary.lib import options as _options
_options.MainHandler = MainHandler
_options.AbstractCommand = AbstractCommand
