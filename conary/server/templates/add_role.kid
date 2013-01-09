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
