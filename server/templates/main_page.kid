<?xml version='1.0' encoding='UTF-8'?>
<?python # import library
from templates import library
?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:py="http://naeblis.cx/ns/kid#">
    {library.html_header(pageTitle)}
    <body>
        <h2>{pageTitle}</h2>

        <p>Welcome to the Conary Repository.</p>
        <ul>
        <li><a href="metadata">Metadata Management</a></li>
        <li><a href="userlist">User Administration</a></li>
        <li><a href="chPassForm">Change Password</a></li>
        </ul>

        {library.html_footer()}
    </body>
</html>
