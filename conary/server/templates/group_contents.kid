<?xml version='1.0' encoding='UTF-8'?>
<html xmlns:html="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
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
                    from conary.server.http import flavorWrap
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
