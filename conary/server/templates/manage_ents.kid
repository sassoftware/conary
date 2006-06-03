<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--
 Copyright (c) 2005 rpath, Inc.

 This program is distributed under the terms of the Common Public License,
 version 1.0. A copy of this license should have been distributed with this
 source file in a file called LICENSE. If it is not present, the license
 is always available at http://www.opensource.org/licenses/cpl.php.

 This program is distributed in the hope that it will be useful, but
 without any warranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->
    <!-- table of permissions -->
    <head/>
    <body>
        <div id="inner">
            <h2>Manage Entitlements</h2>
            <table class="manage-ents" id="entitlements">
                <thead>
                    <tr>
                        <td style="width: 25%;">Entitlement Group</td>
                        <td py:if="isAdmin" py:content="'Permissions Group'"/>
                        <td py:if="isAdmin" py:content="'Managing Group'"/>
                        <td py:if="isAdmin" py:content="'Delete'"/>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, (entGroup, owner, permGroup) in enumerate(sorted(entGroups))"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td>
                            <a href="manageEntitlementForm?entGroup=${entGroup}" py:content="entGroup"/>
                        </td>
                        <td py:if="isAdmin" py:content="permGroup"/>
                        <td py:if="isAdmin">
                            <form action="entSetOwner" method="get">
                                <input type="hidden" name="entGroup" value="${entGroup}"/>
                                <select name="entOwner">
                                    <option value="*none*" py:attrs="{ 'selected' : owner is None and 'selected' or None }">(none)</option>
                                    <option py:for="group in groups" py:content="group" py:value="${group}" py:attrs="{ 'selected' : group == owner and 'selected' or None}"/>
                                </select>
                                <input type="submit" value="Update"/>
                            </form>
                        </td>
                        <td py:if="isAdmin"><a href="deleteEntGroup?entGroup=${entGroup}">X</a></td>
                    </tr>
                </tbody>
            </table>
            <p py:if="isAdmin"><a href="addEntGroupForm">Add Entitlement Group</a></p>
        </div>
    </body>
</html>
