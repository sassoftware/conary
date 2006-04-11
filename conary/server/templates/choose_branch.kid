<?xml version='1.0' encoding='UTF-8'?>
<?python
# Copyright (c) 2005 rpath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
?>
<html xmlns:html="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    <head/>
    <body>
        <div id="inner">
            <h2>Choose Branch</h2>
            <form method="post" action="getMetadata">
                <input type="hidden" name="troveName" value="${troveName}" />
                Choose a branch:

                <select name="branch" id="branch">
                    <option py:for="branch in branches"
                            py:content="branch.label().asString().split('@')[-1]"
                            value="${branch.freeze()}"/>
                </select>

                <input py:if="source" type="hidden" name="source" value="${source}" />
                <input type="submit" />
            </form>
        </div>
    </body>
</html>
