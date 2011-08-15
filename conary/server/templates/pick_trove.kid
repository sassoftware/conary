<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
?>
    <head/>
    <body>
        <div id="inner">
            <h2>Update Metadata</h2>
            <form action="chooseBranch" method="post">
                <p>
                    <div class="formHeader">Pick a trove:</div>
                    <select name="troveNameList" size="12" multiple="multiple" style="width: 50%;">
                        <option py:for="trove in troveList"
                                value="${trove}" py:content="trove"/>
                    </select>
                </p>
                <p><div class="formHeader">Or enter a trove name:</div><input type="text" name="troveName"/></p>
                <p><input type="submit" /></p>
                <p><input type="submit" value="Freshmeat" name="source" /></p>
            </form>
        </div>
    </body>
</html>
