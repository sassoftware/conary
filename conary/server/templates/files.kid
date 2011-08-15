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

    <div id="fileList" py:def="fileList(files)">
        <table style="width: 100%;">
            <tr py:for="pathId, path, fileId, version, fObj in files">
                <?python #
                    from conary.lib import sha1helper
                    import os
                    from urllib import quote

                    url = "getFile?path=%s;pathId=%s;fileId=%s;fileV=%s" % (os.path.basename(path),
                                                                                sha1helper.md5ToString(pathId),
                                                                                sha1helper.sha1ToString(fileId),
                                                                                quote(version.asString()))
                ?>
                <td>${fObj.modeString()}</td>
                <td>${fObj.inode.owner()}</td>
                <td>${fObj.inode.group()}</td>
                <td>${fObj.sizeString()}</td>
                <td>${fObj.timeString()}</td>
                <td><a href="${url}">${path}</a></td>
            </tr>
        </table>
    </div>

    <head/>
    <body>
        <div id="inner">
            <h2>Files in <a href="troveInfo?t=${troveName}">${troveName}</a></h2>

            ${fileList(fileIters)}
        </div>
    </body>
</html>
