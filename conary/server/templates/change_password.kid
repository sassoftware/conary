<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
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
