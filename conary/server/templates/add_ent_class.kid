<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
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
