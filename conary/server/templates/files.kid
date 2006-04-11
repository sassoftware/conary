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
