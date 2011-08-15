<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--

Copyright (c) rPath, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

-->
    <!-- table of permissions -->
    <head/>
    <body>
        <div id="inner">
            <h2>Entitlement Keys for <span py:content="entClass"/></h2>
            <table class="entlist" id="entitlements">
                <thead>
                    <tr>
                        <td style="width: 25%;">Entitlement Key</td>
                        <td style="width: 25%;">Action</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, entKey in enumerate(sorted(entKeys))"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td py:content="entKey"/>
                        <td>
                            <a href="deleteEntitlementKey?entClass=${entClass};entKey=${entKey}">Delete Key</a>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p>
                <a href="addEntitlementKeyForm?entClass=${entClass}">Add Entitlement</a>
            </p>
        </div>
    </body>
</html>
