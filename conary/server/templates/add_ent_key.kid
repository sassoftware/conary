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
            <h2>Add Entitlement Key</h2>

            <form method="post" action="addEntitlementKey">
            <input type="hidden" value="${entClass}" name="entClass"/>
                <table>
                    <tr><td>Entitlement Class:</td><td><span py:content="entClass"/></td></tr>
                    <tr><td>Entitlement Key:</td><td><input size="64" name="entKey"/></td></tr>
                </table>
                <p><input type="submit" value="Add Entitlement Key"/></p>
            </form>
        </div>
    </body>
</html>
