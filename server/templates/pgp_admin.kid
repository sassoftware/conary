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
    <!-- table of pgp keys -->
    <head/>
    <body>
        <div id="inner">
            <h2>PGP Keys</h2>
            <table class="user-admin" id="users">
                <thead>
                    <tr>
                        <td style="width: 40%;">Key Fingerprint</td>
                        <td style="width: 40%">Subkey Fingerprints</td>
                        <td style="text-align: right;">Options</td>
                    </tr>
                </thead>
                <tbody>
                    <tr py:for="key in keyTable.getUsersMainKeys(userId)">
                        <td>${key}</td>
                        <td><div py:for="subkey in keyTable.getSubkeys(key)"
                                 py:content="subkey" />
                        </td>
                        <td style="text-align: right;">
                            <u>Update</u>
                        </td>
                    </tr>
                </tbody>
            </table>
            <p><a href="pgpNewKeyForm">Add Key</a></p>

        </div>
    </body>
</html>
