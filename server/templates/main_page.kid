<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    ${html_header(pageTitle)}
    <body>
        <h1>${pageTitle}</h1>

        <ul class="menu"><li class="highlighted">Main Menu</li></ul>
        <ul class="menu submenu"> </ul>
        <div id="content">
            <p>Welcome to the Conary Repository.</p>
            <ul>
                <li><a href="metadata">Metadata Management</a></li>
                <li><a href="userlist">User Administration</a></li>
                <li><a href="chPassForm">Change Password</a></li>
            </ul>

            ${html_footer()}
        </div>
    </body>
</html>
