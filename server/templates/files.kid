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
 without any waranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
-->

    <div id="fileList" py:def="fileList(files)">
        <table style="width: 100%;">
            <tr py:for="pathId, path, fileId, version, fObj in files">
                <td>${fObj.modeString()}</td>
                <td>${fObj.inode.owner()}</td>
                <td>${fObj.inode.group()}</td>
                <td>${fObj.sizeString()}</td>
                <td>${fObj.timeString()}</td>
                <td>${path}</td>
            </tr>
        </table>
    </div>

    ${html_header("Main Menu")}
    <body>
        <h1>Conary Repository</h1>
        <ul class="menu"><li class="highlighted">Main Menu</li></ul>
        <ul class="menu submenu"> </ul>

        <div id="content">
            <h2>Files in <a href="troveInfo?t=${troveName}">${troveName}</a></h2>

            ${fileList(fileIters)}
        
            ${html_footer()}
        </div>
    </body>
</html>
