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
import metadata

from htmlengine import HtmlEngine
from metadata import MDClass
from repository.netrepos import netserver

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
        self.troveStore = repServer.troveStore
        
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

    def metadataCmd(self, authToken, fields, troveName=None):
        troveList = [x for x in self.repServer.troveStore.iterTroveNames() if x.endswith(':source')]
        troveList.sort()

        # pick the next trove in the list
        # or stay on the previous trove if canceled
        if "troveName" in fields:
            troveName = fields["troveName"].value
        elif troveName in troveList:
            loc = troveList.index(troveName)
            if loc < len(troveList):
                troveName = troveList[loc+1]

        self.htmlPageTitle("Metadata")
        self.htmlPickTrove(troveList, troveName=troveName)

    def chooseBranchCmd(self, authToken, fields):
        if fields.has_key('troveName'):
            troveName = fields['troveName'].value
        else:
            troveName = fields['troveNameList'].value
        
        if fields.has_key('source'):
            source = fields['source'].value.lower()
        else:
            source = None
        
        branches = {}
        versions = self.repServer.getTroveVersionList(authToken,
            netserver.SERVER_VERSIONS[-1], { troveName : None }, "")
        
        for version in versions[troveName]:
            version = self.repServer.thawVersion(version)
            branch = version.branch().freeze()

            branchName = branch.split("@")[-1]
            branches[branch] = branchName

        if len(branches) == 1:
            self._getMetadata(fields, troveName, branches.keys()[0], source)
            return

        self.htmlPageTitle("Please choose a branch:")
        self.htmlPickBranch(troveName, branches)

    def getMetadataCmd(self, authToken, fields):
        troveName = fields['troveName'].value
        branch = fields['branch'].value
        if 'source' in fields:
            source = fields['source'].value.lower()
        else:
            source = "local"

        self._getMetadata(fields, troveName, branch, source)

    def _getMetadata(self, fields, troveName, branch, source):
        branch = self.repServer.thawVersion(branch)

        self.htmlPageTitle("Metadata for %s" % troveName)
        if source == "freshmeat":
            if "freshmeatName" in fields:
                fmName = fields["freshmeatName"].value
            else:
                fmName = troveName[:-7]
            try:
                md = metadata.fetchFreshmeat(fmName)
            except metadata.NoFreshmeatRecord:
                md = None
                self.htmlWarning("No Freshmeat record found.")
        else:
            md = self.troveStore.getMetadata(troveName, branch)

        if not md: # fill a stub
            md = metadata.Metadata(None) 
        self.htmlMetadataEditor(troveName, branch, md, md.getSource())
  
    def updateMetadataCmd(self, authToken, fields):
        branch = self.repServer.thawVersion(fields["branch"].value)
        troveName = fields["troveName"].value

        self.troveStore.updateMetadata(troveName, branch,
            fields["shortDesc"].value,
            fields["longDesc"].value,
            fields.getlist("urlList"),
            fields.getlist("licenseList"),
            fields.getlist("categoryList"),
            fields["source"].value,
            "C"
        )

        self.metadataCmd(authToken, fields, troveName)
        
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
        self.repServer.addUser(authToken, 0, user, password)
        self.repServer.addAcl(authToken, 0, user, "", "", write, True, admin)

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
        admin = self.repServer.auth.check(authToken, admin=True)
        
        if username != authToken[0]:
            if not admin:
                raise InsufficientPermission
        
        if fields.has_key("oldPassword"):
            oldPassword = fields["oldPassword"].value
        else:
            oldPassword = None
        p1 = fields["password1"].value
        p2 = fields["password2"].value

        self.htmlPageTitle("Change Password")
        if authToken[1] != oldPassword and authToken[0] == username and not admin:
            self.writeFn("""<div class="warning">Error: old password is incorrect</div>""")
        elif p1 != p2:
            self.writeFn("""<div class="warning">Error: passwords do not match</div>""")
        elif oldPassword == p1:
            self.writeFn("""<div class="warning">Error: old and new passwords identical, not changing.</div>""")
        else:
            self.repServer.auth.changePassword(username, p1)
            self.writeFn("""<div>Password successfully changed.</div>""")
