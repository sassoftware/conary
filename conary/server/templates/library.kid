<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#">
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
    <head py:match="item.tag == '{http://www.w3.org/1999/xhtml}head'">
        <title>Conary Repository</title>
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/common.css" />
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/repository.css" />
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/custom.css" />
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/library.js"/>
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/repository.js"/>
    </head>
    <body py:match="item.tag == '{http://www.w3.org/1999/xhtml}body'">
        <?python
            import os
            from conary import constants
            lastchunk = methodName
            imagePath = os.path.join('usr', 'share', 'conary', 'web-common', 'images')
            if (os.path.exists(os.path.join(imagePath, 'corplogo.png')) and os.path.exists(os.path.join(imagePath, 'prodlogo.gif'))):
                branded = True
            else:
                branded = False
        ?>
        <div id="main">
            <a name="top" />
            <div id="top">
                <img id="topgradleft" src="${cfg.staticPath}/images/topgrad_left.png" alt="" />
                <img id="topgradright" src="${cfg.staticPath}/images/topgrad_right.png" alt="" />
                <div id="corpLogo">
                    <img py:if="branded" src="${cfg.staticPath}/images/corplogo.png" width="80" height="98" alt="rPath Logo" />
                </div>
                <div id="prodLogo">
                    <img py:if="branded" src="${cfg.staticPath}/images/prodlogo.gif" alt="rBuilder Online Logo" />
                    <div py:if="not branded" py:strip="True">
                    <h1>Conary Repository</h1>
                    </div>
                </div>
                <div id="topRight">
                    <div class="about">
                        <span py:if="not loggedIn" py:strip="True"><a href="login">Login</a></span>
                        <span py:if="loggedIn" py:strip="True">Welcome, <b>${currentUser}</b><span py:if="isAdmin">&nbsp;(administrator)</span>.&nbsp;&nbsp;<a href="logout">Logout</a></span>
                    </div>
                    <p style="font-size: smaller;">Conary Repository Server ${constants.version}</p>
                </div>
            </div>
            <ul class="menu">
                <li py:attrs="{'class': (lastchunk in ('', 'main', 'browse', 'troveInfo', 'files')) and 'highlighted' or None}"><a href="browse">Repository Browser</a></li>
                <li py:if="hasWrite" py:attrs="{'class': (lastchunk in ('pgpAdminForm', 'pgpNewKeyForm')) and 'highlighted' or None}"><a href="pgpAdminForm">PGP Keys</a></li>
                <li py:if="isAdmin" py:attrs="{'class': (lastchunk in ('userlist', 'addUserForm', 'addPermForm', 'editPermForm', 'manageRoleForm',)) and 'highlighted' or None}"><a href="userlist">Users and Roles</a></li>
                <li py:if="loggedIn" py:attrs="{'class': (lastchunk in ('chPassForm', )) and 'highlighted' or None}"><a href="chPassForm">Change Password</a></li>
                <li py:if="isAdmin" py:attrs="{'class': (lastchunk in ('manageEntitlements', 'manageEntitlementForm', 'addEntitlementKeyForm', 'addEntClassForm')) and 'highlighted' or None}"><a href="manageEntitlements">Manage Entitlements</a></li>
            </ul>
            <ul class="menu submenu">&nbsp;</ul>
            <div class="layout" py:replace="item[:]" />
            <div id="footer">
                <div>
                    <span id="topOfPage"><a href="#top">Top of Page</a></span>
                    <ul class="footerLinks">
                        <li><a href="http://www.rpath.com/">About rPath</a></li>
                    </ul>
                </div>
                <div id="bottomText">
                    <span id="copyright">Copyright &copy; 2005-2007 rPath. All Rights Reserved.</span>
                    <span id="tagline">rPath. The Software Appliance Company.</span>
                </div>

            </div>
        </div>
    </body>
</html>
