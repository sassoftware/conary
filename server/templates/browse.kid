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
    ${html_header("Main Menu")}
    <body>
        <h1>Repository Browser</h1>
        <ul class="menu"><li class="highlighted">Main Menu</li></ul>
        <ul class="menu submenu"> </ul>

        <div id="content">
            <h2>Repository Browser</h2>

            <ul>
                <li py:for="package in packages">
                    <a href="troveInfo?t=${package}">${package}</a> <span py:if="package in components">[+]</span>
                    <ul id="components" py:if="package in components">
                        <li py:for="component in components[package]">
                            <a href="troveInfo?t=${package}:${component}">${component}</a>
                        </li>
                    </ul>
                </li>
            </ul>
            ${html_footer()}
        </div>
    </body>
</html>
