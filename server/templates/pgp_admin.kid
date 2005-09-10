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
    <div py:def="generateOwnerListForm(fingerprint, users, userid = None)" py:strip="True">
      <form action="pgpChangeOwner" method="post">
        <input type="hidden" name="key" value="${fingerprint}"/>
        <select name="owner">
            <option value="${None}">--Nobody--</option>
            <option py:for="userId, userName in users.items()" value="${userName}"
                    py:attrs="{'selected': (userId==userid) and 'selected' or None}"
                    py:content="userName" />
        </select>
        <button type="submit" value="Change">Change Association</button>
      </form>
    </div>

    <div py:def="printKeyTableEntry(key, userId)" py:strip="True">
     <tr class='key-ids'>
      <td colspan="3">
          <div py:for="id in keyTable.getUserIds(key)" py:content="id"/>
      </td>
     </tr>
     <tr>
      <td>${key}</td>
      <td><div py:for="subkey in keyTable.getSubkeys(key)"
               py:content="subkey" />
      </td>
      <td py:if="admin" style="text-align: right;">${generateOwnerListForm(key, users, userId)}</td>
     </tr>
    </div>

    <!-- table of pgp keys -->
    <head/>
    <body>
        <div id="inner">
            <h2>${admin and "All " or "My "}PGP Keys</h2>
            NOTE: Keys owned by '--Nobody--' may not be used to sign troves.
            These keys are, for all intents and purposes, disabled.
            <table class="key-admin" id="users">
                <thead>
                    <tr class="key-ids">
                        <td colspan="3">Key User Ids</td>
                    </tr>
                    <tr>
                        <td style="width: 40%;">Key Fingerprint</td>
                        <td style="width: 40%">Subkey Fingerprints</td>
                        <td py:if="admin" style="text-align: right;">User Association</td>
                    </tr>
                </thead>
                <tbody>
                    <div py:for="key in keyTable.getUsersMainKeys(None)" py:strip="True">
                      ${printKeyTableEntry(key, None)}
                    </div>
                    <div py:for="userId, userName in users.items()" py:strip="True">
                      <div py:for="key in keyTable.getUsersMainKeys(userId)" py:strip="True">
                          ${printKeyTableEntry(key, userId)}
                      </div>
                    </div>
                </tbody>
            </table>
            <p><a href="pgpNewKeyForm">Add or Update a Key</a></p>

        </div>
    </body>
</html>
