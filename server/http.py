#
# Copyright (c) 2004-2005 Specifix, Inc.
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
import os
import sys
import traceback

import kid
import templates

from repository.netrepos import netserver
from web.webhandler import WebHandler
from web.fields import strFields, intFields, listFields, boolFields
from web.webauth import getAuth

from mod_python import apache
from mod_python.util import FieldStorage

class ServerError(Exception):
    def __str__(self):
        return self.str
        
class InvalidPassword(ServerError):
    str = """Incorrect password."""

def checkAuth(write = False, admin = False):
    def deco(func):
        def wrapper(self, **kwargs):
            # XXX two xmlrpc calls here could possibly be condensed to one
            # first check the password only
            if not self.repServer.auth.check(kwargs['auth']):
                raise InvalidPassword
            # now check for proper permissions
            if not self.repServer.auth.check(kwargs['auth'], write=write, admin=admin):
                raise netserver.InsufficientPermission
            else:
                return func(self, **kwargs)
        return wrapper
    return deco

class HttpHandler(WebHandler):
    def __init__(self, req, cfg, repServer):
        WebHandler.__init__(self, req, cfg)

        self.repServer = repServer
        self.troveStore = repServer.troveStore

        if 'server.templates' in sys.modules:
            self.templatePath = os.path.dirname(sys.modules['server.templates'].__file__) + os.path.sep
        else:
            self.templatePath = os.path.dirname(sys.modules['templates'].__file__) + os.path.sep
                        
       
    def _getHandler(self, cmd):
        try:
            method = self.__getattribute__(cmd)
        except AttributeError:
            method = self.main
        return method

    def _methodHandler(self):
        """Handle either an HTTP POST or GET command."""
        self.writeFn = self.req.write
        cmd = os.path.basename(self.req.path_info)

        # return a possible error code from getAuth (malformed auth header, etc)
        auth = getAuth(self.req)
        if type(auth) is int:
            return auth

        if cmd.startswith('_'):
            return apache.HTTP_NOT_FOUND

        self.req.content_type = "application/xhtml+xml"

        try:
            method = self._getHandler(cmd)
        except AttributeError:
            return apache.HTTP_NOT_FOUND
        self.fields = FieldStorage(self.req)

        d = dict(self.fields)
        d['auth'] = auth

        try:
            return method(**d)
        except netserver.InsufficientPermission:
            # good password but no permission, don't ask for a new password
            return apache.HTTP_FORBIDDEN
        except InvalidPassword:
            # if password is invalid, request a new one
            return self._requestAuth()
        except:
            self._write("error", error = traceback.format_exc())
            return apache.OK

    def _requestAuth(self):
        self.req.err_headers_out['WWW-Authenticate'] = \
            'Basic realm="Conary Repository"'
        return apache.HTTP_UNAUTHORIZED

    def _write(self, templateName, **values):
        path = os.path.join(self.templatePath, templateName + ".kid")
        t = kid.load_template(path)
        self.writeFn(t.serialize(encoding="utf-8", cfg = self.cfg, **values))

    @checkAuth(write=True)
    def main(self, auth):
        self._write("main_page")
        return apache.OK

    @checkAuth(write = True)
    @strFields(troveName = "")
    def metadata(self, auth, troveName):
        troveList = [x for x in self.repServer.troveStore.iterTroveNames() if x.endswith(':source')]
        troveList.sort()

        # pick the next trove in the list
        # or stay on the previous trove if canceled
        if troveName in troveList:
            loc = troveList.index(troveName)
            if loc < (len(troveList)-1):
                troveName = troveList[loc+1]

        self._write("pick_trove", troveList = troveList,
                                  troveName = troveName)
        return apache.OK

    @checkAuth(write = True)
    @strFields(troveName = "", troveNameList = "", source = "")
    def chooseBranch(self, auth, troveName, troveNameList, source):
        if not troveName:
            if not troveNameList:
                self._write("error", error = "You must provide a trove name.")
                return apache.OK
            troveName = troveNameList
       
        source = source.lower()
        
        versions = self.repServer.getTroveVersionList(auth,
            netserver.SERVER_VERSIONS[-1], { troveName : None })
        
        branches = {}
        for version in versions[troveName]:
            version = self.repServer.thawVersion(version)
            branches[version.branch()] = True

        branches = branches.keys()
        if len(branches) == 1:
            return self._redirect("getMetadata?troveName=%s;branch=%s" %\
                (troveName, branches[0].freeze()))
        else:
            self._write("choose_branch",
                           branches = branches,
                           troveName = troveName,
                           source = source)
        return apache.OK

    @checkAuth(write = True)
    @strFields(troveName = None, branch = None, source = "", freshmeatName = "")
    def getMetadata(self, auth, troveName, branch, source, freshmeatName):
        branch = self.repServer.thawVersion(branch)

        if source.lower() == "freshmeat":
            if freshmeatName:
                fmName = freshmeatName
            else:
                fmName = troveName[:-7]
            try:
                md = metadata.fetchFreshmeat(fmName)
            except metadata.NoFreshmeatRecord:
                self._write("error", error = "No Freshmeat record found.")
                return
        else:
            md = self.troveStore.getMetadata(troveName, branch)

        if not md: # fill a stub
            md = metadata.Metadata(None)

        self._write("metadata", metadata = md, branch = branch,
                                troveName = troveName)
        return apache.OK

    @checkAuth(write = True)
    @listFields(str, selUrl = [], selLicense = [], selCategory = [])
    @strFields(troveName = None, branch = None, shortDesc = "",
               longDesc = "", source = None)
    def updateMetadata(self, auth, troveName, branch, shortDesc,
                       longDesc, source, selUrl, selLicense,
                       selCategory):
        branch = self.repServer.thawVersion(branch)
        
        self.troveStore.updateMetadata(troveName, branch,
                                       shortDesc, longDesc,
                                       selUrl, selLicense,
                                       selCategory, source, "C")
        return self._redirect("metadata?troveName=%s" % troveName)
    
    @checkAuth(write = True, admin = True)
    def userlist(self, auth):
        self._write("user_admin", netAuth = self.repServer.auth)
        return apache.OK

    @checkAuth(write = True, admin = True)
    def addPermForm(self, auth):
        groups = (x[1] for x in self.repServer.auth.iterGroups())
        labels = (x[1] for x in self.repServer.auth.iterLabels())
        troves = (x[1] for x in self.repServer.auth.iterItems())
    
        self._write("permission", groups=groups, labels=labels, troves=troves)
        return apache.OK

    @checkAuth(write = True, admin = True)
    @strFields(group = None, label = "", trove = "",
               write = "off", capped = "off", admin = "off")
    def addPerm(self, auth, group, label, trove,
                write, capped, admin):
        write = (write == "on")
        capped = (capped == "on")
        admin = (admin == "on")
       
        self.repServer.auth.addAcl(group, trove, label,
                                   write, capped, admin)
        self._write("notice", message = "Permission successfully added.",
                                 link = "User Administration",
                                 url = "userlist")
        return apache.OK
  
    @checkAuth(write = True, admin = True)
    def addGroupForm(self, auth):
        users = dict(self.repServer.auth.iterUsers())
        self._write("add_group", users = users)
        return apache.OK
   
    @checkAuth(write = True, admin = True)
    @strFields(userGroupName = None)
    @listFields(int, initialUserIds = [])
    def addGroup(self, auth, userGroupName, initialUserIds):
        newGroupId = self.repServer.auth.addGroup(userGroupName)
        for userId in initialUserIds:
            self.repServer.auth.addGroupMember(newGroupId, userId)

        return self._redirect("userlist")
 
    @checkAuth(write = True, admin = True)
    @strFields(groupId = None, labelId = "", itemId = "")
    def deletePerm(self, auth, groupId, labelId, itemId):
        # labelId and itemId are optional parameters so we can't
        # default them to None: the fields decorators treat that as
        # required, so we need to reset them to None here:
        if not labelId:
            labelId = None
        if not itemId:
            itemId = None
        self.repServer.auth.deletePermission(groupId, labelId, itemId)
        return self._redirect("userlist")

    @checkAuth(write = True, admin = True)
    def addUserForm(self, auth):
        self._write("add_user")
        return apache.OK

    @checkAuth(write = True, admin = True)
    @strFields(user = None, password = None)
    @boolFields(write = False, admin = False)
    def addUser(self, auth, user, password, write, admin):
        self.repServer.addUser(auth, 0, user, password)
        self.repServer.addAcl(auth, 0, user, "", "", write, True, admin)

        return self._redirect("userlist")

    @checkAuth()
    @strFields(username = "")
    def chPassForm(self, auth, username):
        if username:
            askForOld = False
        else:
            username = auth[0]
            askForOld = True
        
        self._write("change_password", username = username, askForOld = askForOld)
        return apache.OK
   
    @checkAuth()
    @strFields(username = None, oldPassword = "",
               password1 = None, password2 = None)
    def chPass(self, auth, username, oldPassword,
               password1, password2):
        admin = self.repServer.auth.check(auth, admin=True)
        
        if username != auth[0]:
            if not admin:
                raise netserver.InsufficientPermission
        
        if auth[1] != oldPassword and auth[0] == username and not admin:
            self._write("error", error = "Error: old password is incorrect")
        elif password1 != password2:
            self._write("error", error = "Error: passwords do not match")
        elif oldPassword == password1:
            self._write("error", error = "Error: old and new passwords identical, not changing")
        else:
            self.repServer.auth.changePassword(username, password1)
            if admin:
                returnLink = ("User Administration", "userlist")
            else:
                returnLink = ("Main Menu", "main")

            self._write("notice", message = "Password successfully changed",
                        link = returnLink[0], url = returnLink[1])
        return apache.OK
