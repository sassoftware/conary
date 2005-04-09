<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    
    <!-- table of permissions -->
    <table class="user-admin" id="permissions" py:def="permTable(groupId, rows)">
        <thead>
            <tr>
                <td style="width: 55%;">Label</td>
                <td>Item</td>
                <td>Write</td>
                <td>Capped</td>
                <td>Admin</td>
                <td>X</td>
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
                <td><a href="deletePerm?groupId=${groupId}&amp;labelId=${row[0]}&amp;itemId=${row[2]}" title="Delete Permission">X</a></td>
            </tr>
            <tr py:if="not rows">
                <td>Group has no permissions.</td>
            </tr>
        </tbody>
    </table>

    ${html_header(pageTitle)}
    <body>
        <h2>${pageTitle}</h2>

        <h3>Users</h3>

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
                        <u>Delete</u>
                    </td>
                </tr>
            </tbody>
        </table>
        <p><a href="addUserForm">Add User</a></p>

        <h3>Groups</h3>
        <table class="user-admin" id="groups">
            <thead><tr><td style="width: 25%;">Group Name</td><td>Permissions</td></tr></thead>
            <tbody>
                <tr py:for="i, group in enumerate(netAuth.iterGroups())"
                    class="${i % 2 and 'even' or 'odd'}">
                <?python #
                rows = list(enumerate(netAuth.iterPermsByGroupId(group[0])))
                ?>
                    <td><b>${group[1]}</b></td>
                    <td py:if="rows" py:content="permTable(group[0], rows)"/>
                    <td py:if="not rows" style="font-size: 80%;">Group has no permissions</td>
                </tr>
            </tbody>
        </table>
        <p>
            <a href="addPermForm">Add Permission</a><br />
            <a href="addGroupForm">Add Group</a>
        </p>

        ${html_footer()}
    </body>
</html>
