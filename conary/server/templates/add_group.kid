<?xml version='1.0' encoding='UTF-8'?>
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
<html xmlns:html="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    <head/>
    <body>
        <div id="inner">
            <h2 py:content="modify and 'Edit Group' or 'Add Group'"></h2>

            <form method="post" action="${modify and 'manageGroup' or 'addGroup'}">
                <input py:if="modify" type="hidden" name="userGroupId" value="${userGroupId}" />
                <table class="add-form">
                    <tr>
                        <td id="header">Group Name:</td>
                        <td><input type="text" name="userGroupName" value="${userGroupName}"/></td>
                    </tr>
                    <tr>
                        <td id="header">Initial Users:</td>
                        <td>
                            <select name="initialUserIds" multiple="multiple" size="10"
                                    style="width: 100%;">
                                <option py:for="userId, userName in users.items()"
                                        value="${userId}"
                                        py:attrs="{'selected': (userName in members) and 'selected' or None}">
                                    ${userName}
                                </option>
                            </select>
                        </td>
                    </tr>
                </table>
                <p>
                    <input py:if="not modify" type="submit" value="Add Group" />
                    <input py:if="modify" type="submit" value="Submit Group Changes" />
                </p>
            </form>
        </div>
    </body>
</html>
