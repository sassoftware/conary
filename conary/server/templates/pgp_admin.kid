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
    <div py:def="generateOwnerListForm(fingerprint, users, targUser = None)" py:strip="True">
      <form action="pgpChangeOwner" method="post">
        <input type="hidden" name="key" value="${fingerprint}"/>
        <select name="owner">
            <option py:for="userName in sorted(users)" value="${userName}"
                    py:attrs="{'selected': (userName==targUser) and 'selected' or None}"
                    py:content="userName" />
        </select>
        <button type="submit" value="Change">Change Association</button>
      </form>
    </div>

    <div py:def="breakKey(key)" py:strip="True">
        <?python
    brokenkey = ''
    for x in range(len(key)/4):
        brokenkey += key[x*4:x*4+4] + " "
        ?>
        ${brokenkey}
    </div>

    <div py:def="printKeyTableEntry(keyEntry, userName)" py:strip="True">
     <tr class="key-ids">
      <td>
        <div>pub: ${breakKey(keyEntry['fingerprint'])}</div>
        <div py:for="id in keyEntry['uids']"> uid: &#160; &#160; ${id}</div>
        <div py:for="subKey in keyEntry['subKeys']">sub: ${breakKey(subKey)}</div>
      </td>
      <td py:if="admin" style="text-align: right;">${generateOwnerListForm(keyEntry['fingerprint'], users, userName)}</td>
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
                    <tr>
                        <td>Key</td>
                        <td py:if="admin" style="text-align: right;">Owner</td>
                    </tr>
                </thead>
                <tbody>
                    <div py:for="userName in sorted(users)" py:strip="True">
                      <div py:for="keyEntry in openPgpKeys[userName]" py:strip="True">
                          ${printKeyTableEntry(keyEntry, userName)}
                      </div>
                    </div>
                </tbody>
            </table>
            <p><a href="pgpNewKeyForm">Add or Update a Key</a></p>

        </div>
    </body>
</html>
