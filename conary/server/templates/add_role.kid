<?xml version='1.0' encoding='UTF-8'?>
<?python
# Copyright (c) 2005,2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
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
                            <input type="radio" name="isAdmin" value="1" py:attrs="{'checked' : isAdmin and 'checked' or None }"/>Yes
                            <input type="radio" name="isAdmin" value="0" py:attrs="{'checked' : (not isAdmin) and 'checked' or None }"/>No
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
