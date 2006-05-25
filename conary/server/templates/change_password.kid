<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
# Copyright (c) 2005 rpath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
?>
    <head/>
    <body>
        <div id="inner">
            <h2>Change Password</h2>

            <form method="post" action="chPass">
                <table cellpadding="6">
                    <tr><td>Changing password for:</td><td><b>${username}</b></td></tr>
                    <tr py:if="askForOld"><td>Old password:</td><td><input type="password" name="oldPassword"/></td></tr>
                    <tr><td>New password:</td><td><input type="password" name="password1"/></td></tr>
                    <tr><td>Again:</td><td><input type="password" name="password2"/></td></tr>
                </table>
                <p><input type="submit"/></p>
                <input type="hidden" name="username" value="${username}" />
            </form>
        </div>
    </body>
</html>
