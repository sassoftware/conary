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
            <h2>Manage Entitlements</h2>
            <table class="manage-ents" id="entitlements">
                <thead>
                    <tr>
                        <td style="width: 25%;">Entitlement Class</td>
                        <td py:if="isAdmin" py:content="'Role'"/>
                        <td py:if="isAdmin" py:content="'Managing Role'"/>
                        <td style="text-align: right;">Action</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, (entClass, owner, roles) in enumerate(sorted(entClasses))"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td py:content="entClass"/>
                        <td>
                            <table py:if="isAdmin"><tbody><div py:for="role in sorted(roles)" py:strip="True">
                                <tr><td py:content="role"/></tr>
                            </div></tbody></table>
                        </td>
                        <td py:if="isAdmin" py:content="owner"/>
                        <td style="text-align: right;">
                            <a href="manageEntitlementForm?entClass=${entClass}">Manage Keys</a>
                            <span py:if="isAdmin">&nbsp;|&nbsp;
                               <a href="configEntClassForm?entClass=${entClass}">Edit Class</a>&nbsp;|&nbsp;
                               <a href="deleteEntClass?entClass=${entClass}">Delete Class</a>
                            </span>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p><a py:if="isAdmin" href="addEntClassForm">Add Entitlement Class</a></p>
        </div>
    </body>
</html>
