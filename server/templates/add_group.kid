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
 without any waranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->

    ${html_header("Add Group")}
    <body>
        <h1>Conary Repository</h1>

        <ul class="menu">
            <li><a href="userlist">User List</a></li>
            <li class="highlighted">Add Permission</li>
        </ul>
        <ul class="menu submenu"> </ul>

        <div id="content">
            <h2>Add Group</h2>

            <form method="post" action="addGroup">
                <table class="add-form">
                    <tr>
                        <td id="header">Group Name:</td>
                        <td><input type="text" name="userGroupName"/></td>
                    </tr>
                    <tr>
                        <td id="header">Initial Users:</td>
                        <td>
                            <select name="initialUserIds" multiple="multiple" size="10"
                                    style="width: 100%;">
                                <option py:for="userId, userName in users.items()"
                                        py:content="userName" value="${userId}">${userName}</option>
                            </select>
                        </td>
                    </tr>
                </table>
                <p><input type="submit" value="Add Group" /></p>
            </form>

            ${html_footer()}
        </div>
    </body>
</html>
