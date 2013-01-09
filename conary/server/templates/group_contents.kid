<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<?python
#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
