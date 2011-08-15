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
    <!-- creates a selection dropdown based on a list -->
    <select py:def="makeSelect(elementName, items, selected = None)"
            name="${elementName}">
        <?python
            items = sorted(items)
            if 'ALL' in items:
                items.remove('ALL')
                items.insert(0, 'ALL')
        ?>
        <option py:for="value in items" py:content="value" value="${value}" py:attrs="{'selected': (selected == value) and 'selected' or None}" />
    </select>

    <head/>
    <body>
        <div id="inner">
            <h2>${operation} Permission</h2>
            <form method="post" action="${(operation == 'Edit') and 'editPerm' or 'addPerm'}">
                <input py:if="operation=='Edit'" name="oldlabel" value="${label}" type="hidden" />
                <input py:if="operation=='Edit'" name="oldtrove" value="${trove}" type="hidden" />
                <table class="add-form">
                    <tr>
                        <td id="header">Role:</td>
                        <td py:if="operation!='Edit'" py:content="makeSelect('role', roles, role)"/>
                        <td py:if="operation=='Edit'"><input name="role" value="${role}" readonly="readonly" type="text" /></td>
                    </tr>
                    <tr>
                        <td id="header">Label:</td>
                        <td py:content="makeSelect('label', labels, label)"/>
                    </tr>
                    <tr>
                        <td id="header">Trove:</td>
                        <td py:content="makeSelect('trove', troves, trove)"/>
                    </tr>
                    <tr>
                        <td id="header" rowspan="3">Options:</td>
                        <td><input type="checkbox" name="writeperm" py:attrs="{'checked': (writeperm) and 'checked' or None}" /> Write access</td>
                    </tr>
                    <tr>
                        <td><input type="checkbox" name="remove" py:attrs="{'checked': (remove) and 'checked' or None}" /> Remove</td>
                    </tr>

                </table>
                <p><input type="submit" value="${operation}"/></p>
            </form>
        </div>
    </body>
</html>
