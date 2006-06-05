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
            <h2>Add Entitlement Group</h2>

            <form method="post" action="addEntGroup">
                <table>
                    <tr><td>Entitlement Group:</td><td><input name="entGroup"/></td></tr>
                    <tr>
                        <td>Permissions Group:</td>
                        <td>
                            <select name="userGroup">
                                <option py:for="group in groups" py:content="group" py:value="${group}"/>
                            </select>
                        </td>
                    </tr>
                    <tr>
                        <td>Managing Group:</td>
                        <td>
                            <select name="entOwner">
                                <option value="*none*" selected="selected">(none)</option>
                                <option py:for="group in groups" py:content="group" py:value="${group}"/>
                            </select>
                        </td>
                    </tr>
                </table>
                <p><input type="submit" value="Add Entitlement Group"/></p>
            </form>
        </div>
    </body>
</html>
