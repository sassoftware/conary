<?xml version='1.0' encoding='UTF-8'?>
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
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    <head/>
    <body>
        <div id="inner">
            <h2 py:content="modify and 'Edit Role' or 'Add Role'"></h2>

            <form method="post" action="${modify and 'manageRole' or 'addRole'}">
                <input py:if="modify" type="hidden" name="roleName" value="${role}" />
                <table class="add-form">
                    <tr>
                        <td id="header">Role Name:</td>
                        <td><input type="text" name="newRoleName" value="${role}"/></td>
                    </tr>
                    <tr>
                        <td id="header">Initial Users:</td>
                        <td>
                            <select name="memberList" multiple="multiple" size="10"
                                    style="width: 100%;">
                                <option py:for="userName in sorted(users)"
                                        value="${userName}"
                                        py:attrs="{'selected': (userName in members) and 'selected' or None}">
                                    ${userName}
                                </option>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td id="header">Role is admin:</td>
                        <td>
                            <input type="radio" name="roleIsAdmin" value="1" py:attrs="{'checked' : roleIsAdmin and 'checked' or None }"/>Yes
                            <input type="radio" name="roleIsAdmin" value="0" py:attrs="{'checked' : (not roleIsAdmin) and 'checked' or None }"/>No
                        </td>
                    </tr>
                    <tr>
                        <td id="header">Role can mirror:</td>
                        <td>
                            <input type="radio" name="canMirror" value="1" py:attrs="{'checked' : canMirror and 'checked' or None }"/>Yes
                            <input type="radio" name="canMirror" value="0" py:attrs="{'checked' : (not canMirror) and 'checked' or None }"/>No
                        </td>
                    </tr>
                </table>
                <p>
                    <input py:if="not modify" type="submit" value="Add Role" />
                    <input py:if="modify" type="submit" value="Submit Role Changes" />
                </p>
            </form>
        </div>
    </body>
</html>
