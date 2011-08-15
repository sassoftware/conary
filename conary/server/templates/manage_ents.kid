<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--

Copyright (c) rPath, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

-->
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
