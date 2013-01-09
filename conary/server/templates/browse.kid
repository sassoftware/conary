<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
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

import string
from urllib import quote
?>
    <head/>
    <body>
        <div id="inner">
            <h2>Repository Browser</h2>

            <span py:for="l in string.uppercase" py:strip="True">
                <span py:if="totals[l]"><a href="browse?char=${l}" title="${totals[l]} trove(s)">${l}</a> |
                </span>
            </span>
            <?python
                total = 0
                for x in string.digits:
                    total += totals[x]
            ?>
            <span py:if="total">
                <a py:if="l not in string.digits and total" href="browse?char=0" title="${total} trove(s)">0-9</a>
            </span>

            <?python
                if char in string.digits:
                    char = "a digit"
                else:
                    char = "'%c'" % char
            ?>
            <h3>Troves beginning with ${char}</h3>
            <ul py:if="packages">
                <li py:for="package in packages">
                    <a href="troveInfo?t=${quote(package)}">${package}</a> <span py:if="package in components">[+]</span>
                    <ul id="components" py:if="package in components">
                        <li py:for="component in components[package]">
                            <a href="troveInfo?t=${quote(package)}:${quote(component)}">${component}</a>
                        </li>
                    </ul>
                </li>
            </ul>
            <p py:if="not packages">No matching troves found.</p>
        </div>
    </body>
</html>
