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
?>
    <!-- table of permissions -->
    <head/>
    <body>
        <div id="inner">
            <h2>Entitlement Keys for <span py:content="entClass"/></h2>
            <table class="entlist" id="entitlements">
                <thead>
                    <tr>
                        <td style="width: 25%;">Entitlement Key</td>
                        <td style="width: 25%;">Action</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, entKey in enumerate(sorted(entKeys))"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td py:content="entKey"/>
                        <td>
                            <a href="deleteEntitlementKey?entClass=${entClass};entKey=${entKey}">Delete Key</a>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p>
                <a href="addEntitlementKeyForm?entClass=${entClass}">Add Entitlement</a>
            </p>
        </div>
    </body>
</html>
