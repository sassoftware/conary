<?xml version='1.0' encoding='UTF-8'?>
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
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
    <div id="fileList" py:def="fileList(files)">
        <table style="width: 100%;">
            <tr py:for="pathId, path, fileId, version, fObj in files">
                <?python #
                    from conary.lib import sha1helper
                    import os
                    from urllib import quote

                    url = "getFile?path=%s;pathId=%s;fileId=%s;fileV=%s" % (quote(os.path.basename(path)),
                                                                                sha1helper.md5ToString(pathId),
                                                                                sha1helper.sha1ToString(fileId),
                                                                                quote(version.asString()))
                ?>
                <td>${fObj.modeString()}</td>
                <td>${fObj.inode.owner()}</td>
                <td>${fObj.inode.group()}</td>
                <td>${fObj.sizeString()}</td>
                <td>${fObj.timeString()}</td>
                <td><a href="${url}">${path.decode('utf8', 'replace')}</a></td>
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
