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
from metadata import MDClass

class HtmlEngine:

    styleSheet = """
div.formHeader {
    float: left;
    font-weight: bold;
    width: 16%;
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

            function updateMetadata() {
                alert("updating");
                selectAll('urlList');
                selectAll('licenseList');
                selectAll('categoryList');
                document.getElementById('submitButton').submit();
            }
        </script> 
    </head>
<body>""" % (pageTitle, self.styleSheet))

    def htmlFooter(self):
        self.writeFn("""
<hr />
</body></html>""")

    def htmlPickTrove(self, troveList=[], action="chooseBranch"):
        troveSelection = self._genSelect(troveList, "troveNameList", size=12, expand=True)

        self.writeFn("""
<form action="/%s" method="post">
<p><div class="formHeader">Pick a trove:</div>%s</p>
<p><div class="formHeader">Or enter a trove name:</div><input type="text" name="troveName"></p>
<p><input type="submit"></p>
</form>
        """ % (action, troveSelection))
       
    def htmlPickBranch(self, troveName, branchList, action="getMetadata"):
        branchSelection = self._genSelect(branchList, "branch")

        self.writeFn("""
<form method="post" action="%s">
<input type="hidden" name="troveName" value="%s" />
Choose a branch: %s
<input type="submit" />
</form>
"""     % (action, troveName, branchSelection))

    def htmlPageTitle(self, title=""):
        self.writeFn("""<h2>%s</h2>""" % title)

    # XXX this is just a placeholder for a real editor
    def htmlMetadataEditor(self, troveName, branch, metadata):
        branchStr = branch.asString()
        branchFrz = branch.freeze()

        if "version" in metadata:
            # the only number that matters in the metadata version is the source revision
            versionStr = metadata["version"].trailingVersion().asString().split("-")[-2]
        else:
            versionStr = "Initial Version"

        self.writeFn("""
<h2>Metadata for %s</h2>
<h4>Branch: %s</h4>
<h4>Metadata revision: %s</h4>
<form method="post" action="updateMetadata">
<table style="width: 100%%;">
<tr><td>Short Description:</td><td><input style="width: 50%%;" type="text" name="shortDesc" value="%s" /></td></tr>
<tr><td>Long Description:</td><td><textarea style="width: 50%%;" name="longDesc" rows="4" cols="60">%s</textarea></td></tr>
<tr><td>URLs:</td><td>%s<br />%s</td></tr>
<tr><td>Licenses:</td><td>%s<br /> %s</td></tr>
<tr><td>Categories:</td><td>%s<br />%s</td></tr>
</table>
<p><button id="submitButton" onClick="javascript:updateMetadata();">Save Changes</button></p>
<input type="hidden" name="branch" value="%s" />
<input type="hidden" name="troveName" value="%s" />
</form>

""" %   (troveName, branchStr, versionStr,
         metadata[MDClass.SHORT_DESC][0],
         metadata[MDClass.LONG_DESC][0],
         self._genSelect(metadata[MDClass.URL], "urlList", size=4, expand=True, multiple=True),
         self._genSelectAppender("newUrl", "urlList"),
         self._genSelect(metadata[MDClass.LICENSE], "licenseList", size=4, expand=True, multiple=True),
         self._genSelectAppender("newLicense", "licenseList"),
         self._genSelect(metadata[MDClass.CATEGORY], "categoryList", size=4, expand=True, multiple=True),
         self._genSelectAppender("newCategory", "categoryList"),
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


    def _genSelectAppender(self, name, selectionName):
        """Generates an input box and add/remove button pair to manage a list of arbitrary
           items in a selection."""
        inputId = name + "Input"
        s = """
<input type="text" name="%s" id="%s" />
<input type="button" onClick="javascript:append('%s', '%s');" value="Add" />
<input type="button" onClick="javascript:removeSelected('%s');" value="Remove" />""" %\
            (name, inputId, selectionName, inputId, selectionName)
        return s

    def _genSelect(self, items, name, default=None, size=1, expand=False, multiple=False):
        """Generate a html <select> dropdown or selection list based on a dictionary or a list.
           If 'items' is a dictionary, use the dictionary value as the option value, and display
           the key to the user. If 'items' is a list, use the list item for both."""
        if expand:
            style = """width: 50%;"""
        else:
            style = ""

        if multiple:
            multiple = "multiple"
        else:
            multiple = ""
            
        s = """<select name="%s" id="%s" %s size="%d" style="%s">\n""" % (name, name, multiple, size, style)

        # generate [(data, friendlyName), ...)] from either a list or a dict
        if isinstance(items, list):
            items = zip(items, items)
        elif isinstance(items, dict):
            items = items.items()

        for key, item in items:
            s += """<option value="%s">%s</option>\n""" % (item, key)
        s += """</select>"""

        return s

    def setWriter(self, writeFn):
        self.writeFn = writeFn
