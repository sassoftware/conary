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
    <head/>
    <body>
        <div id="inner">
            <h2 py:content="entClass and 'Configure Entitlement Class' or 'Add Entitlement Class'"/>

            <form method="post" py:attrs="{ 'action' : entClass and 'configEntClass' or 'addEntClass' }">
                <table>
                    <tr>
                        <td>Entitlement Class:</td>
                        <td py:if="not entClass"><input name="entClass"/></td>
                        <td py:if="entClass">
                            <span py:content="entClass"/>
                            <input name="entClass" type="hidden" value="${entClass}"/>
                        </td>
                    </tr>
                    <tr>
                        <td>Roles:</td>
                        <td>
                            <select name="roles" multiple="true">
                                <option py:for="role in allRoles" py:content="role" py:value="${role}" py:attrs="{'selected': (role in currentRoles) and 'selected' or None}"/>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td>Managing Role:</td>
                        <td>
                            <select name="entOwner">
                                <option value="*none*" py:attrs="{'selected': (not ownerRole) and 'selected' or None}">(none)</option>
                                <option py:for="role in allRoles" py:content="role" py:value="${role}" py:attrs="{'selected': (role == ownerRole) and 'selected' or None}"/>
                            </select>
                        </td>
                    </tr>
                </table>
                <p>
                    <input py:if="not entClass" type="submit" value="Add Entitlement Class"/>
                    <input py:if="entClass" type="submit" value="Configure Entitlement Class"/>
                </p>
            </form>
        </div>
    </body>
</html>
