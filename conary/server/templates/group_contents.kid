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

    <div id="fileList" py:def="troveList(troves)">
        <table style="width: 100%; padding: 0.5em;">
            <tr style="background: #eeeeee; font-size: 120%; font-weight: bold;">
                <td style="width: 25%;">Trove Name</td>
                <td style="width: 25%;">Version</td>
                <td style="width: 50%;">Flavor</td>
            </tr>
            <tr py:for="i, (name, version, flavor) in enumerate(troves)" py:attrs="{'style': 'background: %s' % (i%2 and '#f5f5f5' or '#ffffff')}">
                <?python #
                    from urllib import quote
                    from conary.web.repos_web import flavorWrap
                    url = "files?t=%s;v=%s;f=%s" % (quote(name), quote(version.freeze()), quote(flavor.freeze()))
                ?>
                <td style="vertical-align: top;"><a href="${url}">${name}</a></td>
                <td style="vertical-align: top;">${str(version)}</td>
                <td>${flavorWrap(flavor)}</td>
            </tr>
        </table>
    </div>

    <head/>
    <body>
        <div id="inner">
            <h2>Troves in <a href="troveInfo?t=${troveName}">${troveName}</a></h2>

            ${troveList(troves)}
        </div>
    </body>
</html>
