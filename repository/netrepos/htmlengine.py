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
    def htmlHeader(self, pageTitle=""):
        self.writeFn("""
<html>
    <head>
        <title>%s</title>
    </head>
<body>""" % pageTitle)

    def htmlFooter(self):
        self.writeFn("""</body></html>""")

    def htmlPickTrove(self, troveList=[], action="chooseBranch"):
        troveSelection = self._genSelect(troveList, "troveNameList", size=12, expand=True)

        self.writeFn("""
<h2>Metadata</h2>
<form action="/%s" method="post">
<table>
    <tr>
        <td valign="top">Pick a trove:</td>
        <td>%s</td>
    </tr>
    <tr>
        <td>Or enter a trove name:</td>
        <td><input type="text" name="troveName"></td>
    </tr>
 </table>

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
    def htmlMetadataEditor(self, troveName, branchStr, metadata):
        self.writeFn("""
<h2>Metadata for %s</h2>
<h3>Branch: %s</h3>
<table>
<tr><td><b>Short Description:</b></td><td><input type="text" name="shortDesc" value="%s" /></td></tr>
<tr><td><b>Long Description:</b></td><td><input type="text" name="longDesc" value="%s" /></td></tr>
<tr><td><b>URLs:</b></td><td>%s</td></tr>
<tr><td><b>Licenses:</b></td><td>%s</td></tr>
<tr><td><b>Categories:</b></td><td>%s</td></tr>
</table>
""" %   (troveName, branchStr,
         metadata[MDClass.SHORT_DESC][0],
         metadata[MDClass.LONG_DESC][0],
         ", ".join(metadata[MDClass.URL]),
         ", ".join(metadata[MDClass.LICENSE]),
         ", ".join(metadata[MDClass.CATEGORY]) ))


    def _genSelect(self, items, name, default=None, size=1, expand=False):
        """Generate a html <select> dropdown or selection list based on a dictionary or a list.
           If 'items' is a dictionary, use the dictionary value as the option value, and display
           the key to the user. If 'items' is a list, use the list item for both."""
        if expand:
            style = """width: 100%;"""
        else:
            style = ""
        s = """<select name="%s" size="%d" style="%s">\n""" % (name, size, style)

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
