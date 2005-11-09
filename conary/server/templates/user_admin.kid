<?xml version='1.0' encoding='UTF-8'?>
<html xmlns:html="http://www.w3.org/1999/xhtml"
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
    <!-- table of permissions -->
    <table class="user-admin" id="permissions" py:def="permTable(groupId, group, rows)">
        <thead>
            <tr>
                <td style="width: 55%;">Label</td>
                <td>Trove</td>
                <td>Write</td>
                <td>Capped</td>
                <td>Admin</td>
                <td>X</td>
                <td>E</td>
            </tr>
        </thead>
        <tbody>
            <tr py:for="i, row in rows"
                class="${i % 2 and 'even' or 'odd'}">
                <?python
                if row[1]:
                    label = row[1]
                else:
                    label = "ALL"
                if row[3]:
                    item = row[3]
                else:
                    item = "ALL"
                ?> 
                <td py:content="label"/>
                <td py:content="item"/>
                <td py:content="row[4] and 'yes' or 'no'"/>
                <td py:content="row[5] and 'yes' or 'no'"/>
                <td py:content="row[6] and 'yes' or 'no'"/>
                <td><a href="deletePerm?groupId=${groupId};labelId=${row[0]}&amp;itemId=${row[2]}" title="Delete Permission">X</a></td>
                <td><a href="editPermForm?group=${group};label=${label};trove=${item};writeperm=${row[4]};capped=${row[5]};admin=${row[6]}" title="Edit Permission">E</a></td>
            </tr>
            <tr py:if="not rows">
                <td>Group has no permissions.</td>
            </tr>
        </tbody>
    </table>

    <head/>
    <body>
        <div id="inner">
            <h2>Users</h2>
            <table class="user-admin" id="users">
                <thead>
                    <tr>
                        <td style="width: 25%;">Username</td>
                        <td>Member Of</td>
                        <td style="text-align: right;">Options</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, user in enumerate(netAuth.iterUsers())"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td>${user[1]}</td>
                        <td><div py:for="group in netAuth.iterGroupsByUserId(user[0])"
                                 py:content="group[1]" />
                        </td>
                        <td style="text-align: right;">
                            <a href="chPassForm?username=${user[1]}">Change Password</a> | 
                            <u>Groups</u> | 
                            <a href="deleteUser?username=${user[1]}">Delete</a>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p><a href="addUserForm">Add User</a></p>

            <h2>Groups</h2>
            <table class="user-admin" id="groups">
                <thead><tr><td style="width: 25%;">Group Name</td><td>Permissions</td><td style="text-align: right;">Options</td></tr></thead>
                <tbody>
                    <tr py:for="i, group in enumerate(netAuth.iterGroups())"
                        class="${i % 2 and 'even' or 'odd'}">
                    <?python #
                    rows = list(enumerate(netAuth.iterPermsByGroupId(group[0])))
                    ?>
                        <td><b>${group[1]}</b></td>
                        <td py:if="rows" py:content="permTable(group[0], group[1], rows)"/>
                        <td py:if="not rows" style="font-size: 80%;">Group has no permissions</td>
                        <td style="text-align: right;">
                            <a href="addPermForm?userGroupName=${group[1]}">Add Permission</a><br />
                            <a href="deleteGroup?userGroupId=${group[0]}">Delete</a> | 
                            <a href="manageGroupForm?userGroupName=${group[1]}">Manage</a>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p>
                <a href="addGroupForm">Add Group</a>
            </p>
        </div>
    </body>
</html>
