<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
# Copyright (c) 2005 rpath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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
                        <td>Access Groups:</td>
                        <td>
                            <select name="userGroupList" multiple="true">
                                <option py:for="group in groups" py:content="group" py:value="${group}" py:attrs="{'selected': (group in accessGroups) and 'selected' or None}"/>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td>Managing Group:</td>
                        <td>
                            <select name="entOwner">
                                <option value="*none*" py:attrs="{'selected': (not ownerGroup) and 'selected' or None}">(none)</option>
                                <option py:for="group in groups" py:content="group" py:value="${group}" py:attrs="{'selected': (group == ownerGroup) and 'selected' or None}"/>
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
