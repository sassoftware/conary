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
    <!-- Change the rowspan on the Options: label to 3 when the capped 
         setting is enabled -->
    <body>
        <div id="inner">
            <h2>${operation} Permission</h2>
            <form method="post" action="${(operation == 'Edit') and 'editPerm' or 'addPerm'}">
                <input py:if="operation=='Edit'" name="oldlabel" value="${label}" type="hidden" />
                <input py:if="operation=='Edit'" name="oldtrove" value="${trove}" type="hidden" />
                <table class="add-form">
                    <tr>
                        <td id="header">Group:</td>
                        <td py:if="operation!='Edit'" py:content="makeSelect('group', groups, group)"/>
                        <td py:if="operation=='Edit'"><input name="group" value="${group}" readonly="readonly" type="text" /></td>
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
                        <td id="header" rowspan="2">Options:</td>
                        <td><input type="checkbox" name="writeperm" py:attrs="{'checked': (writeperm) and 'checked' or None}" /> Write access</td>
                    </tr>
                    <tr style="display: none;">
                        <td><input type="checkbox" name="capped" py:attrs="{'checked': (capped) and 'checked' or None}" /> Capped</td>
                    </tr>
                    <tr>
                        <td><input type="checkbox" name="admin" py:attrs="{'checked': (admin) and 'checked' or None}" /> Admin access</td>
                    </tr>

                </table>
                <p><input type="submit" value="${operation}"/></p>
            </form>
        </div>
    </body>
</html>
