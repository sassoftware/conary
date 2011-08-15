<?xml version='1.0' encoding='UTF-8'?>
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
<html xmlns="http://www.w3.org/1999/xhtml"
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
