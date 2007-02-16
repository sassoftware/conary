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
    <table class="user-admin" id="permissions" py:def="permTable(group, rows)">
        <thead>
            <tr>
                <td style="width: 55%;">Label</td>
                <td>Trove</td>
                <td>Write</td>
                <td>Capped</td>
                <td>Admin</td>
                <td>Remove</td>
                <td>X</td>
                <td>E</td>
            </tr>
        </thead>
        <tbody>
            <tr py:for="i, row in rows"
                class="${i % 2 and 'even' or 'odd'}">
                <td py:content="row[0]"/>
                <td py:content="row[1]"/>
                <td py:content="row[2] and 'yes' or 'no'"/>
                <td py:content="row[3] and 'yes' or 'no'"/>
                <td py:content="row[4] and 'yes' or 'no'"/>
                <td py:content="row[5] and 'yes' or 'no'"/>
                <td><a href="deletePerm?group=${group};label=${row[0]}&amp;item=${row[1]}" title="Delete Permission">X</a></td>
                <td><a href="editPermForm?group=${group};label=${row[0]};trove=${row[1]};writeperm=${row[2]};capped=${row[3]};admin=${row[4]};remove=${row[5]}" title="Edit Permission">E</a></td>
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
                    <tr py:for="i, user in enumerate(netAuth.userAuth.getUserList())"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td>${user}</td>
                        <td><div py:for="group in netAuth.userAuth.getGroupsByUser(user)"
                                 py:content="group" />
                        </td>
                        <td style="text-align: right;"><a href="chPassForm?username=${user}">Change Password</a>&nbsp;|&nbsp;<a href="deleteUser?username=${user}">Delete</a></td>
                    </tr>
                </tbody>
            </table>
            <p><a href="addUserForm">Add User</a></p>

            <h2>Groups</h2>
            <table class="user-admin" id="groups">
                <thead><tr><td style="width: 25%;">Group Name</td><td>Mirror</td><td>Permissions</td><td style="text-align: right;">Options</td></tr></thead>
                <tbody>
                    <tr py:for="i, group in enumerate(netAuth.getGroupList())"
                        class="${i % 2 and 'even' or 'odd'}">
                    <?python #
                    rows = list(enumerate(netAuth.iterPermsByGroup(group)))
                    ?>
                        <td><b>${group}</b></td>
                        <td py:if="netAuth.groupCanMirror(group)" py:content="'yes'"/>
                        <td py:if="not netAuth.groupCanMirror(group)" py:content="'no'"/>
                        <td py:if="rows" py:content="permTable(group, rows)"/>
                        <td py:if="not rows" style="font-size: 80%;">Group has no permissions</td>
                        <td style="text-align: right;"><a href="addPermForm?userGroupName=${group}">Add&nbsp;Permission</a>&nbsp;|&nbsp;<a href="deleteGroup?userGroupName=${group}">Delete&nbsp;Group</a>&nbsp;|&nbsp;<a href="manageGroupForm?userGroupName=${group}">Edit&nbsp;Group</a></td>
                    </tr>
                </tbody>
            </table>
            <p>
                <a href="addGroupForm">Add Group</a>
            </p>
        </div>
    </body>
</html>
