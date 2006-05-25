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
        <title></title>
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/common.css" />
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/repository.css" />
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/library.js"/>
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/repository.js"/>
    </head>
    <body py:match="item.tag == '{http://www.w3.org/1999/xhtml}body'">
        <?python
            from conary import constants
            uri_minus_query = self.req.uri[self.req.uri.rfind('?')+1:]
            lastchunk = uri_minus_query[self.req.uri.rfind('/')+1:]
        ?>
        <h1>${self.cfg.serverName}</h1>

        <ul class="menu">
            <li py:attrs="{'class': (lastchunk in ('', 'main', 'browse', 'troveInfo', 'files')) and 'highlighted' or None}"><a href="browse">Repository Browser</a></li>
            <li py:if="hasWrite" py:attrs="{'class': (lastchunk in ('metadata',)) and 'highlighted' or None}"><a href="metadata">Metadata</a></li>
            <li py:if="hasWrite" py:attrs="{'class': (lastchunk in ('pgpAdminForm', 'pgpNewKeyForm')) and 'highlighted' or None}"><a href="pgpAdminForm">PGP Keys</a></li>
            <li py:if="isAdmin" py:attrs="{'class': (lastchunk in ('userlist', 'chPassForm', 'addUserForm', 'addPermForm', 'editPermForm', 'manageGroupForm',)) and 'highlighted' or None}"><a href="userlist">Users and Groups</a></li>
            <li py:if="not isAdmin and hasWrite" py:attrs="{'class': (lastchunk in ('chPassForm', )) and 'highlighted' or None}"><a href="chPassForm">Change Password</a></li>
        </ul>
        <ul class="menu submenu">&nbsp;</ul>
        <div id="content">
            <p style="float: right; font-size: smaller;">
                <span py:if="not hasWrite" py:strip="True"><a href="login">Login</a></span>
                <span py:if="hasWrite" py:strip="True">Welcome, <b>${currentUser}</b><span py:if="isAdmin">&nbsp;(administrator)</span>.</span>
            </p>
            <div id="inner" py:replace="item[:]" />
            <hr />
            <span class="copyright">
                <b>Conary Repository Server ${constants.version}</b>&nbsp;
                Copyright &#169; 2004-2006 <a href="http://www.rpath.com/">rPath, Inc.</a>
            </span>
        </div>
    </body>
</html>
