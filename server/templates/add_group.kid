<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
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
