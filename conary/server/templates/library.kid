<?xml version='1.0' encoding='UTF-8'?>
<html xmlns:py="http://purl.org/kid/ns#" xmlns="http://www.w3.org/1999/xhtml">
<!--
 Copyright (c) 2005 rPath, Inc.

 This program is distributed under the terms of the Common Public License,
 version 1.0. A copy of this license should have been distributed with this
 source file in a file called LICENSE. If it is not present, the license
 is always available at http://www.opensource.org/licenses/cpl.php.

 This program is distributed in the hope that it will be useful, but
 without any waranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->
    <head py:match="item.tag == 'head'">
        <title></title>
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/common.css" />
        <link rel="stylesheet" type="text/css" href="${cfg.staticPath}/css/repository.css" />
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/library.js"/>
        <script language="javascript1.2" src="${cfg.staticPath}/javascript/repository.js"/>
    </head>
    <body py:match="item.tag == 'body'" xmlns="http://www.w3.org/1999/xhtml">
        <h1>Conary Repository</h1>
        <ul class="menu"><li class="highlighted">Conary Repository</li></ul>
        <ul class="menu submenu"> </ul>

        <div id="content">
            <div id="inner" py:replace="item[:]" />
            <hr />
            <span class="copyright">
                <b>Conary Repository Server</b>
                Copyright &#169; 2004-2006 <a href="http://www.rpath.com/">rPath, Inc.</a>
            </span>
        </div>
    </body>
</html>
