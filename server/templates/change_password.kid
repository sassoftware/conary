<?xml version='1.0' encoding='UTF-8'?>
<?python #
from templates import library
?>

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

        <form method="post" action="chPass">
            <table cellpadding="6">
                <tr><td>Changing password for:</td><td><b>{username}</b></td></tr>
                <tr py:if="askForOld"><td>Old password:</td><td><input type="password" name="oldPassword"/></td></tr>
                <tr><td>New password:</td><td><input type="password" name="password1"/></td></tr>
                <tr><td>Again:</td><td><input type="password" name="password2"/></td></tr>
            </table>
            <p><input type="submit"/></p>
            <input type="hidden" name="username" value="{username}" />
        </form>

        {library.html_footer()}
    </body>
</html>
