<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header(pageTitle)}
    <body>
        <h2>${pageTitle}</h2>

        <form method="post" action="addUser">
            <table>
                <tr><td>Username:</td><td><input type="text" name="user"/></td></tr>
                <tr><td>Password:</td><td><input type="password" name="password"/></td></tr>
            </table>
            <p><input type="submit"/></p>
        </form>

        ${html_footer()}
    </body>
</html>
