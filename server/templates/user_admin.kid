<?xml version='1.0' encoding='UTF-8'?>
<?python #
import kid; kid.enable_import()
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    
    <!-- table of permissions -->
    <table class="user-admin" id="permissions" py:def="permTable(rows)">
        <thead>
            <tr>
                <td style="width: 55%;">Label</td>
                <td>Item</td>
                <td>Write</td>
                <td>Capped</td>
                <td>Admin</td>
            </tr>
        </thead>
        <tbody>
            <tr py:for="i, row in enumerate(rows)"
                class="{i % 2 and 'even' or 'odd'}">
                <?python #
                if row[0]:
                    label = row[0]
                else:
                    label = "ALL"
                if row[1]:
                    item = row[1]
                else:
                    item = "ALL"
                ?> 
                <td py:content="label"/>
                <td py:content="item"/>
                <td py:content="row[2] and 'yes' or 'no'"/>
                <td py:content="row[3] and 'yes' or 'no'"/>
                <td py:content="row[4] and 'yes' or 'no'"/>
            </tr>
        </tbody>
    </table>

    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

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
                    class="{i % 2 and 'even' or 'odd'}">
                    <td>{user[1]}</td>
                    <td><div py:for="group in netAuth.iterGroupsByUserId(user[0])"
                             py:content="group[1]" />
                    </td>
                    <td style="text-align: right;"><a href="chPassForm?username={user[1]}">Change Password</a> | <u>Groups</u></td>
                </tr>
            </tbody>
        </table>
        <p><a href="addUserForm">Add User</a></p>

        <h3>Groups</h3>
        <table class="user-admin" id="groups">
            <thead><tr><td style="width: 25%;">Group Name</td><td>Permissions</td></tr></thead>
            <tbody>
                <tr py:for="i, group in enumerate(netAuth.iterGroups())"
                    class="{i % 2 and 'even' or 'odd'}">
                    <td><b>{group[1]}</b></td>
                    <td>{permTable(netAuth.iterPermsByGroupId(group[0]))}</td>
                </tr>
            </tbody>
        </table>
        <p><a href="addPermForm">Add Permission</a></p>

        {library.html_footer()}
    </body>
</html>
