<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
