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
from lib import metadata
import xml.parsers.expat

from htmlengine import HtmlEngine
from lib.metadata import MDClass

class ServerError(Exception):
    def __str__(self):
        return self.str
        
class InvalidServerCommand(ServerError):
    str = """Invalid command passed to server."""

class InsufficientPermission(ServerError):
    str = """Insufficient permission for requested operation."""

class HttpHandler(HtmlEngine):
    def __init__(self, repServer):
        self.repServer = repServer
        self.troveStore = repServer.repos.troveStore
        
        # "command name": (command handler, page title, 
        #       (requires auth, requires write access, requires admin))
        self.commands = {
             # metadata commands
             "":               (self.mainpage, "Conary Repository",         
                               (True, True, False)),
             "metadata":       (self.metadataCmd, "View Metadata",          
                               (True, True, False)),
             "chooseBranch":   (self.chooseBranchCmd, "View Metadata",      
                               (True, True, False)),
             "getMetadata":    (self.getMetadataCmd, "View Metadata",       
                               (True, True, False)),
             "updateMetadata": (self.updateMetadataCmd, "Metadata Updated", 
                               (True, True, False)),
             # user administration commands
             "userlist":       (self.userlistCmd, "User Administration",    
                               (True, True, True)),
             "addUserForm":    (self.addUserFormCmd, "Add User",            
                               (True, True, True)),
             "addUser":        (self.addUserCmd, "Add User",                
                               (True, True, True)),
             # change password commands
             "chPassForm":     (self.chPassFormCmd, "Change Password",
                               (True, False, False)),
             "chPass":         (self.chPassCmd, "Change Password",
                               (True, False, False)),
             "test":           (self.test, "Testing",                       
                               (True, True, False)),
                        }

    def requiresAuth(self, cmd):
        if cmd in self.commands:
            return self.commands[cmd][2][0]
        else:
            return True

    def handleCmd(self, writeFn, cmd, authToken=None, fields=None):
        """Handle either an HTTP POST or GET command."""
        self.setWriter(writeFn)
        if cmd.endswith('/'):
            cmd = cmd[:-1]
    
        if cmd in self.commands:
            handler = self.commands[cmd][0]
            pageTitle = self.commands[cmd][1]
        else:
            raise InvalidServerCommand

        needWrite = self.commands[cmd][2][1]
        needAdmin = self.commands[cmd][2][2]
        if not self.repServer.auth.check(authToken, write=needWrite, admin=needAdmin):
            raise InsufficientPermission

        if cmd == "":
	    home = None
        else:
	    home = self.repServer.urlBase

        self.htmlHeader(pageTitle)
        handler(authToken, fields)
        self.htmlFooter(home)

    def mainpage(self, authToken, fields):
        self.htmlPageTitle("Conary Repository")
        self.htmlMainPage()

    def test(self, authToken, fields):
        self.htmlPageTitle("Testing")
        self.writeFn("<pre>Authentication token: " + str(authToken))
        self.writeFn("\n\nFields: " + str(fields) + "</pre>")

    def metadataCmd(self, authToken, fields):
        troveList = [x for x in self.repServer.repos.iterAllTroveNames() if x.endswith(':source')]

        self.htmlPageTitle("Metadata")
        self.htmlPickTrove(troveList)

    def chooseBranchCmd(self, authToken, fields):
        if fields.has_key('troveName'):
            troveName = fields['troveName'].value
        else:
            troveName = fields['troveNameList'].value
        
        branches = {}
        for version in self.troveStore.iterTroveVersions(troveName):
            branch = version.branch().freeze()

            branchName = branch.split("@")[-1]
            branches[branch] = branchName

        if len(branches) == 1:
            self._getMetadata(troveName, branches.keys()[0])
            return

        self.htmlPageTitle("Please choose a branch:")
        self.htmlPickBranch(troveName, branches)

    def getMetadataCmd(self, authToken, fields):
        troveName = fields['troveName'].value
        branch = fields['branch'].value
        if 'source' in fields:
            source = fields['source'].value
        else:
            source = None

        self._getMetadata(troveName, branch, source)

    def _getMetadata(self, troveName, branch, source=None):
        branch = self.repServer.thawVersion(branch)

        self.htmlPageTitle("Metadata for %s" % troveName)
        if source == "freshmeat":
            try:
                md = metadata.fetchFreshmeat(troveName[:-7])
            except xml.parsers.expat.ExpatError:
                md = None
                self.htmlWarning("No Freshmeat record found.")
        else:
            md = self.troveStore.getMetadata(troveName, branch)

        # fill a stub
        if not md:
            md = {
                    "shortDesc":  [ "" ],
                    "longDesc":   [ "" ],
                    "url":        [],
                    "license":    [],
                    "category":   [],
                 }

        self.htmlMetadataEditor(troveName, branch, md)
  
    def updateMetadataCmd(self, authToken, fields):
        branch = self.repServer.thawVersion(fields["branch"].value)
        troveName = fields["troveName"].value

        self.troveStore.updateMetadata(troveName, branch,
            fields["shortDesc"].value,
            fields["longDesc"].value,
            fields.getlist("urlList"),
            fields.getlist("licenseList"),
            fields.getlist("categoryList"),
            "C"
        )

        self.htmlPageTitle("Update Successful")
        self.htmlUpdateSuccessful(troveName, branch.asString().split("/")[-1])
       
    def userlistCmd(self, authToken, fields):
        self.htmlPageTitle("User List")
        userlist = list(self.repServer.auth.iterUsers())
        self.htmlUserlist(userlist)

    def addUserFormCmd(self, authToken, fields):
        self.htmlPageTitle("Add User")
        self.htmlAddUserForm()

    def addUserCmd(self, authToken, fields):
        user = fields["user"].value
        password = fields["password"].value
       
        if fields.has_key("write"):
            write = True
        else:
            write = False

        if fields.has_key("admin"):
            admin = True
        else:
            admin = False
        self.repServer.auth.add(user, password, write=write, admin=admin)
        self.writeFn("""User added successfully. <a href="userlist">Return</a>""")
        
    def chPassFormCmd(self, authToken, fields):
        self.htmlPageTitle("Change Password")

        if fields.has_key("username"):
            username = fields["username"].value
            askForOld = False
        else:
            username = authToken[0]
            askForOld = True
            
        self.htmlChPassForm(username, askForOld)
        
    def chPassCmd(self, authToken, fields):
        username = fields["username"].value
        if username != authToken[0]:
            if not self.repServer.auth.check(authToken, admin=True):
                raise InsufficientPermission
        
        if fields.has_key("oldPassword"):
            oldPassword = fields["oldPassword"].value
        else:
            oldPassword = None
        p1 = fields["password1"].value
        p2 = fields["password2"].value

        self.htmlPageTitle("Change Password")
        if authToken[1] != oldPassword and authToken[0] == username:
            self.writeFn("""<div class="warning">Error: old password is incorrect</div>""")
        elif p1 != p2:
            self.writeFn("""<div class="warning">Error: passwords do not match</div>""")
        elif oldPassword == p1:
            self.writeFn("""<div class="warning">Error: old and new passwords identical, not changing.</div>""")
        else:
            self.repServer.auth.changePassword(username, p1)
            self.writeFn("""<div>Password successfully changed.</div>""")
