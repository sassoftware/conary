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
    <table class="user-admin" id="permissions" py:def="permTable(role, rows)">
        <thead>
            <tr>
                <td style="width: 55%;">Label</td>
                <td>Trove</td>
                <td>Write</td>
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
                <td><a href="deletePerm?role=${role};label=${row[0]}&amp;item=${row[1]}" title="Delete Permission">X</a></td>
                <td><a href="editPermForm?role=${role};label=${row[0]};trove=${row[1]};writeperm=${row[2]};remove=${row[3]}" title="Edit Permission">E</a></td>
            </tr>
            <tr py:if="not rows">
                <td>Role has no permissions.</td>
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
                        <td style="text-align: right;">Action</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, user in enumerate(netAuth.userAuth.getUserList())"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td>${user}</td>
                        <td><div py:for="role in netAuth.userAuth.getRolesByUser(user)"
                                 py:content="role" />
                        </td>
                        <td style="text-align: right;"><a href="chPassForm?username=${user}">Change Password</a>&nbsp;|&nbsp;<a href="deleteUser?username=${user}">Delete</a></td>
                    </tr>
                </tbody>
            </table>
            <p><a href="addUserForm">Add User</a></p>

            <h2>Roles</h2>
            <table class="user-admin" id="roles">
                <thead><tr><td style="width: 25%;">Role</td><td>Admin</td><td>Mirror</td><td>Permissions</td><td style="text-align: right;">Action</td></tr></thead>
                <tbody>
                    <tr py:for="i, role in enumerate(netAuth.getRoleList())"
                        class="${i % 2 and 'even' or 'odd'}">
                    <?python #
                    rows = list(enumerate(netAuth.iterPermsByRole(role)))
                    ?>
                        <td><b>${role}</b></td>
                        <td py:if="netAuth.roleIsAdmin(role)" py:content="'yes'"/>
                        <td py:if="not netAuth.roleIsAdmin(role)" py:content="'no'"/>
                        <td py:if="netAuth.roleCanMirror(role)" py:content="'yes'"/>
                        <td py:if="not netAuth.roleCanMirror(role)" py:content="'no'"/>
                        <td py:if="rows" py:content="permTable(role, rows)"/>
                        <td py:if="not rows" style="font-size: 80%;">Role has no permissions</td>
                        <td style="text-align: right;"><a href="addPermForm?roleName=${role}">Add&nbsp;Permission</a>&nbsp;|&nbsp;<a href="deleteRole?roleName=${role}">Delete&nbsp;Role</a>&nbsp;|&nbsp;<a href="manageRoleForm?roleName=${role}">Edit&nbsp;Role</a></td>
                    </tr>
                </tbody>
            </table>
            <p>
                <a href="addRoleForm">Add Role</a>
            </p>
        </div>
    </body>
</html>
