<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:py="http://purl.org/kid/ns#"
      py:extends="'library.kid'">
<!--
 Copyright (c) 2005 rpath, Inc.

 This program is distributed under the terms of the Common Public License,
 version 1.0. A copy of this license should have been distributed with this
 source file in a file called LICENSE. If it is not present, the license
 is always available at http://www.opensource.org/licenses/cpl.php.

 This program is distributed in the hope that it will be useful, but
 without any warranty; without even the implied warranty of merchantability
 or fitness for a particular purpose. See the Common Public License for
 full details.
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
