<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--

Copyright (c) rPath, Inc.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

-->
    <!-- table of pgp keys -->
    <head/>
    <body>
        <div id="inner">
            <h2>PGP Key Submission</h2>
	    <form method="POST" action="submitPGPKey">
	    	<table class="user-admin" id="users">
        		<thead>
				<tr>
					<td>Paste PGP Key Here</td>
				</tr>
                	</thead>
                	<tbody>
                    		<tr>
					<td witdh="100%"><textarea name="keyData" rows="40" cols="80"/></td>
	                	</tr>
                	</tbody>
            	</table>
	    <font size="1">
	    <p>* Submit only one key at a time</p>
	    <p>* Submit ONLY your own public keys</p>
	    <p>* Once a key is submitted it CANNOT be removed</p>
	    </font>
            <p><button type="submit">Submit Key</button></p>
	    </form>
        </div>
    </body>
</html>
