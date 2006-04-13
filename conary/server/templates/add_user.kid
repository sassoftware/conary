<?xml version='1.0' encoding='UTF-8'?>
<html xmlns:html="http://www.w3.org/1999/xhtml"
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
            <h2>Add User</h2>

            <form method="post" action="addUser">
                <table>
                    <tr><td>Username:</td><td><input type="text" name="user"/></td></tr>
                    <tr><td>Password:</td><td><input type="password" name="password"/></td></tr>
                </table>
                <p><input type="submit"/></p>
            </form>
        </div>
    </body>
</html>
