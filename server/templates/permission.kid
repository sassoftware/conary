<?xml version='1.0' encoding='UTF-8'?>
<html xmlns:html="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--
 Copyright (c) 2005 rpath, Inc.

 This program is distributed under the terms of the Common Public License,
 version 1.0. A copy of this license should have been distributed with this
 source file in a file called LICENSE. If it is not present, the license
 is always available at http://www.opensource.org/licenses/cpl.php.

 This program is distributed in the hope that it will be useful, but
 without any waranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->
    <!-- creates a selection dropdown based on a list, optionally adding an ALL
         option at the top of the list. -->
    <select py:def="makeSelect(elementName, items, all = False)"
            name="${elementName}">
        <option py:if="all" value="">ALL</option>
        <option py:for="value in sorted(items)"
                py:content="value" value="${value}"/>
    </select>

    <head/>
    <body>
        <div id="inner">
            <h2>Add Permission</h2>
            <form method="post" action="addPerm">
                <table class="add-form">
                    <tr>
                        <td id="header">Group:</td>
                        <td py:content="makeSelect('group', groups)"/>
                    </tr>
                    <tr>
                        <td id="header">Label:</td>
                        <td py:content="makeSelect('label', labels, all = True)"/>
                    </tr>
                    <tr>
                        <td id="header">Trove:</td>
                        <td py:content="makeSelect('trove', troves, all = True)"/>
                    </tr>
                    <tr>
                        <td id="header" rowspan="3">Options</td>
                        <td><input type="checkbox" name="write" /> Write access</td>
                    </tr>
                    <tr style="display: none;">
                        <td><input type="checkbox" name="capped" /> Capped</td>
                    </tr>
                    <tr>
                        <td><input type="checkbox" name="admin" /> Admin access</td>
                    </tr>

                </table>
                <p><input type="submit" value="Add"/></p>
            </form>
        </div>
    </body>
</html>
