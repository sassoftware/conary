<?xml version='1.0' encoding='UTF-8'?>
<html xmlns:py="http://purl.org/kid/ns#"
      xmlns="http://www.w3.org/1999/xhtml">
<!--
 Copyright (c) 2005 rPath, Inc.

 This program is distributed under the terms of the Common Public License,
 version 1.0. A copy of this license should have been distributed with this
 source file in a file called LICENSE. If it is not present, the license
 is always available at http://www.opensource.org/licenses/cpl.php.

 This program is distributed in the hope that it will be useful, but
 without any warranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->
    <head py:match="item.tag == '{http://www.w3.org/1999/xhtml}head'">
        <title>Conary Repository (${cfg.serverName})</title>
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
            uri_minus_query = self.req.uri[self.req.uri.rfind('?')+1:]
            lastchunk = uri_minus_query[self.req.uri.rfind('/')+1:]
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
                        <span py:if="not hasWrite" py:strip="True"><a href="login">Login</a></span>
                        <span py:if="hasWrite" py:strip="True">Welcome, <b>${currentUser}</b><span py:if="isAdmin">&nbsp;(administrator)</span>.</span>
                    </div>
                    <p style="font-size: smaller;"><span style="font-weight:bold;" py:content="', '.join(cfg.serverName)" /><br />Conary Repository Server ${constants.version}</p>
                </div>
            </div>
            <ul class="menu">
                <li py:attrs="{'class': (lastchunk in ('', 'main', 'browse', 'troveInfo', 'files')) and 'highlighted' or None}"><a href="browse">Repository Browser</a></li>
                <li py:if="hasWrite" py:attrs="{'class': (lastchunk in ('metadata', 'getMetadata')) and 'highlighted' or None}"><a href="metadata">Metadata</a></li>
                <li py:if="hasWrite" py:attrs="{'class': (lastchunk in ('pgpAdminForm', 'pgpNewKeyForm')) and 'highlighted' or None}"><a href="pgpAdminForm">PGP Keys</a></li>
                <li py:if="isAdmin" py:attrs="{'class': (lastchunk in ('userlist', 'chPassForm', 'addUserForm', 'addPermForm', 'editPermForm', 'manageGroupForm',)) and 'highlighted' or None}"><a href="userlist">Users and Groups</a></li>
                <li py:if="not isAdmin and hasWrite" py:attrs="{'class': (lastchunk in ('chPassForm', )) and 'highlighted' or None}"><a href="chPassForm">Change Password</a></li>
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
                    <span id="copyright">Copyright &copy; 2005-2006 rPath. All Rights Reserved.</span>
                    <span id="tagline">rPath. The Software Appliance Company.</span>
                </div>

            </div>
        </div>
    </body>
</html>
