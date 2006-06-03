<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--
 Copyright (c) 2005 rpath, Inc.

 This program is distributed under the terms of the Common Public License,
 version 1.0. A copy of this license should have been distributed with this
 source file in a file called LICENSE. If it is not present, the license
 is always available at http://www.opensource.org/licenses/cpl.php.

 This program is distributed in the hope that it will be useful, but
 without any warranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->
    <!-- table of permissions -->
    <head/>
    <body>
        <div id="inner">
            <h2>Entitlements for <span py:content="entGroup"/></h2>
            <table class="entlist" id="entitlements">
                <thead>
                    <tr>
                        <td style="width: 25%;">Entitlement Group</td>
                        <td style="width: 25%;">Delete</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="i, entitlement in enumerate(sorted(entitlements))"
                        class="${i % 2 and 'even' or 'odd'}">
                        <td py:content="entitlement"/>
                        <td>
                            <a href="deleteEntitlement?entGroup=${entGroup};entitlement=${entitlement}">X</a>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p>
                <a href="addEntitlementForm?entGroup=${entGroup}">Add Entitlement</a>
            </p>
        </div>
    </body>
</html>
