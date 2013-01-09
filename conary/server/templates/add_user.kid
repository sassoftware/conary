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
    <head/>
    <body>
        <div id="inner">
            <h2>Add User</h2>

            <form method="post" action="addUser">
                <table>
                    <tr><td>Username:</td><td><input type="text" name="user"/></td></tr>
                    <tr><td>Password:</td><td><input type="password" name="password"/></td></tr>
                </table>
                <p><input type="submit"/></p>
            </form>
        </div>
    </body>
</html>
