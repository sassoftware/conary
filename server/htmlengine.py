#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import traceback

from lib.metadata import MDClass
from fmtroves import TroveCategories, LicenseCategories

class HtmlEngine:

    styleSheet = """
div.formHeader {
    float: left;
    font-weight: bold;
    width: 16%;
}

div.warning {
    color: red;
}

div.tbHeader {
    font-size: 150%;
    color: white;
    background-color: red;
    font-weight: bold;
}

h2 {
    font-size: 150%;
    color: white;
    background-color: #333399;
    font-weight: bold;
}

td {
    vertical-align: top;
}

hr {
    border: 0px;
    height: 2px;
    color: black;
    background-color: black;
}

tr.header {
    background-color: #dddddd;
    font-weight: bold;
    font-size: 105%;
}

"""

    def htmlHeader(self, pageTitle=""):
        self.writeFn("""
<html>
    <head>
        <title>%s</title>
        <style>
            %s
        </style>
        <script language="javascript">
            function append(selId, inputId) {
                sel = document.getElementById(selId);
                text = document.getElementById(inputId).value;
                
                if(text != "") {
                    sel.options[sel.length] = new Option(text, text);
                    document.getElementById(inputId).value="";
                }
            }

            function removeSelected(selId) {
                sel = document.getElementById(selId);
                sel.remove(sel.selectedIndex);
            }

            function selectAll(selId) {
                sel = document.getElementById(selId);
                for (i=0; i < sel.length; i++) {
                    sel.options[i].selected = true;
                }
            }        

            function setValue(selId, entryId) {
                sel = document.getElementById(selId);
                entry = document.getElementById(entryId);
                entry.value = sel.options[sel.selectedIndex].value;
            }

            function updateMetadata() {
                selectAll('urlList');
                selectAll('licenseList');
                selectAll('categoryList');
                document.getElementById('submitButton').submit();
            }
        </script> 
    </head>
<body>""" % (pageTitle, self.styleSheet))

    def htmlFooter(self, home=None):
        self.writeFn("<hr />")
        if home:
            self.writeFn('<a href="%s">Home</a>' % home)
        self.writeFn("</body></html>")

    def htmlPickTrove(self, troveList=[], action="chooseBranch"):
        troveSelection = self.makeSelect(troveList, "troveNameList", size=12, expand="50%")

        self.writeFn("""
<form action="%s" method="post">
<p><div class="formHeader">Pick a trove:</div>%s</p>
<p><div class="formHeader">Or enter a trove name:</div><input type="text" name="troveName"></p>
<p><input type="submit"></p>
</form>
        """ % (action, troveSelection))
       
    def htmlPickBranch(self, troveName, branchList, action="getMetadata"):
        branchSelection = self.makeSelect(branchList, "branch")

        self.writeFn("""
<form method="post" action="%s">
<input type="hidden" name="troveName" value="%s" />
Choose a branch: %s
<input type="submit" />
</form>
"""     % (action, troveName, branchSelection))

    def htmlPageTitle(self, title=""):
        self.writeFn("""<h2>%s</h2>""" % title)

    def htmlWarning(self, warning=""):
        self.writeFn("""<div class="warning">%s</div>""" % warning)
        
    # XXX this is just a placeholder for a real editor
    def htmlMetadataEditor(self, troveName, branch, metadata):
        branchStr = branch.asString().split("/")[-1]
        branchFrz = branch.freeze()

        if "version" in metadata:
            # the only number that matters in the metadata version is the source revision
            versionStr = metadata["version"].split("-")[-1]
        else:
            versionStr = "Initial Version"

        licenses = [x for x in LicenseCategories.values() if "::" in x]
        licenses.sort()
        categories = [x for x in TroveCategories.values() if x.startswith('Topic') and '::' in x]
        categories.sort()
        
        self.writeFn("""
<h4>Branch: %s</h4>
<h4>Metadata revision: %s</h4>
<form method="post" action="updateMetadata">
<table style="width: 100%%;" cellpadding="8">
<tr><td>Short Description:</td><td><input style="width: 53%%;" type="text" name="shortDesc" value="%s" /></td></tr>
<tr><td>Long Description:</td><td><textarea style="width: 53%%;" name="longDesc" rows="4" cols="60">%s</textarea></td></tr>
<tr><td>URLs:</td><td>%s<br />%s</td></tr>
<tr><td>Licenses:</td><td>%s<br />%s</td></tr>
<tr><td>Categories:</td><td>%s<br />%s</td></tr>
</table>
<p><button id="submitButton" onclick="javascript:updateMetadata();">Save Changes</button></p>
<input type="hidden" name="branch" value="%s" />
<input type="hidden" name="troveName" value="%s" />
</form>
"""     % (branchStr, versionStr,
           metadata["shortDesc"][0],
           metadata["longDesc"][0],
           self.makeSelect(metadata["url"], "urlList", size=4,
                           expand="53%", multiple=True,
                           onClick="setValue('urlList', 'newUrl')"),
           self.makeSelectAppender("newUrl", "urlList"),
           self.makeSelect(metadata["license"], "licenseList", size=4, expand="53%", multiple=True),
           self.makeSelectAppenderList("newLicense", "licenseList", licenses),
           self.makeSelect(metadata["category"], "categoryList", size=4, expand="53%", multiple=True),
           self.makeSelectAppenderList("newCategory", "categoryList", categories), 
           branchFrz, troveName)
          )

        self.writeFn("""
<form method="post" action="getMetadata">
<input type="hidden" name="branch" value="%s" />
<input type="hidden" name="troveName" value="%s" />
<input type="hidden" name="source" value="freshmeat" />
<input type="submit" value="Fetch from Freshmeat" />
</form>
"""     % (branchFrz, troveName)
        )
 
    def htmlUpdateSuccessful(self, troveName, branchStr):
        self.writeFn("""Successfully updated %s's metadata on branch %s.""" 
            % (troveName, branchStr))

    def htmlUserlist(self, userlist):
        self.writeFn("""
<table cellpadding="4">
<tr class="header"><td>Username</td><td>Write access</td></tr>
""")
        for user, id, write, admin in userlist:
            if write:
                permStr = "Read/Write"
            else:
                permStr = "Read-Only"
            if admin:
                permStr += " (admin)"
            self.writeFn("<tr><td>%s</td><td>%s</td></tr>\n" % (user, permStr))

        self.writeFn("</table>")
        self.writeFn("""
<p><a href="addUserForm">Add User</a></p>
""")

    def htmlMainPage(self):
        self.writeFn("""
<p>Welcome to the Conary Repository.</p>
<ul>
<li><a href="metadata">Metadata Management</a></li>
<li><a href="userlist">User Administration</a></li>
<li><a href="chPassForm">Change Password</a></li>
</ul>
""")

    def htmlAddUserForm(self):
        self.writeFn("""
<form method="post" action="addUser"
<table>
<tr><td>Username:</td><td><input type="text" name="user"></td></tr>
<tr><td>Password:</td><td><input type="password" name="password"></td></tr>
<tr><td>Write access:</td><td><input type="checkbox" name="write"></td></tr>
<tr><td>Admin access:</td><td><input type="checkbox" name="admin"></td></tr>
</table>
<p><input type="submit"></p>
</form>
""")

    def htmlChPassForm(self, username):
        self.writeFn("""
<form method="post" action="chPass">
<table cellpadding="6">
<tr><td>Changing password for:</td><td><b>%s</b></td></tr>
<tr><td>Old password:</td><td><input type="password" name="oldPassword"></td></tr>
<tr><td>New password:</td><td><input type="password" name="password1"></td></tr>
<tr><td>Again:</td><td><input type="password" name="password2"></td></tr>
</table>
<p><input type="submit"></p>
</form>
""" % username)

    def makeSelectAppender(self, name, selectionName):
        """Generates an input box and add/remove button pair to manage a list of arbitrary
           items in a selection."""
        s = """
<input style="width: 40%%;" type="text" name="%s" id="%s" />
<input style="width: 6%%;" type="button" onclick="javascript:append('%s', '%s');" value="Add" />
<input style="width: 6%%;" type="button" onclick="javascript:removeSelected('%s');" value="Remove" />
"""     % (name, name, selectionName, name, selectionName)
        return s

    def makeSelectAppenderList(self, name, selectionName, items):
        """Generates an selection and add/remove button pair to manage a list of arbitrary
           items in a selection."""
        inputId = name + "Select"

        s = self.makeSelect(items, inputId, expand="40%", blank=True)
        s += """
<input style="width: 6%%;" type="button" onclick="javascript:append('%s', '%s');" value="Add" />
<input style="width: 6%%;" type="button" onclick="javascript:removeSelected('%s');" value="Remove" />
"""     % (selectionName, inputId, selectionName)
        return s

    def makeSelect(self, items, name, default=None, size=1, expand=False,
                   multiple=False, onClick="", blank=False):
        """Generate a html <select> dropdown or selection list based on a dictionary or a list.
           If 'items' is a dictionary, use the dictionary value as the option value, and display
           the key to the user. If 'items' is a list, use the list item for both."""
        if expand:
            style = """width: %s;""" % expand
        else:
            style = ""

        if multiple:
            multiple = "multiple"
        else:
            multiple = ""
            
        s = """<select onclick="javascript:%s;" name="%s" id="%s" %s size="%d" style="%s">\n""" %\
            (onClick, name, name, multiple, size, style)
        if blank:
            s += """<option value="">--</option>"""

        # generate [(data, friendlyName), ...)] from either a list or a dict
        if isinstance(items, list):
            items = zip(items, items)
        elif isinstance(items, dict):
            items = items.items()

        for key, item in items:
            s += """<option value="%s">%s</option>\n""" % (key, item)
        s += """</select>"""

        return s

    def stackTrace(self, wfile):
        self.setWriter(wfile.write)
        self.htmlHeader("Server Error")
        wfile.write("""<div class="tbHeader">Server Error</div>""")
        wfile.write("<pre>")
        traceback.print_exc(file = wfile)
        wfile.write("</pre>")
        self.htmlFooter()

    def setWriter(self, writeFn):
        self.writeFn = writeFn
