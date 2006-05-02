#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from mod_python import apache
from urllib import unquote
import itertools
import kid
import os
import string
import sys
import textwrap
import traceback

from conary import metadata
from conary import versions
from conary import conarycfg
from conary.deps import deps
from conary.repository import shimclient
from conary.repository.errors import GroupAlreadyExists, PermissionAlreadyExists, InsufficientPermission
from conary.repository.netrepos import netserver
from conary.server import templates
from conary.web.fields import strFields, intFields, listFields, boolFields
from conary.web.webauth import getAuth
from conary.web.webhandler import WebHandler

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
            if not self.repos.getUserGroups(self.serverName):
                raise InvalidPassword
            # now check for proper permissions
            if write or admin:
                if not self.repServer.auth.check(self.authToken, write=write, admin=admin):
                    raise InsufficientPermission

            return func(self, **kwargs)
        return wrapper
    return deco

class HttpHandler(WebHandler):
    def __init__(self, req, cfg, repServer, protocol, port):
        WebHandler.__init__(self, req, cfg)

        self.repServer = repServer
        self.troveStore = repServer.troveStore

        self._protocol = protocol
        self._port = port

        if 'conary.server.templates' in sys.modules:
            self.templatePath = os.path.dirname(sys.modules['conary.server.templates'].__file__) + os.path.sep
        else:
            self.templatePath = os.path.dirname(sys.modules['templates'].__file__) + os.path.sep

    def _getHandler(self, cmd):
        try:
            method = self.__getattribute__(cmd)
        except AttributeError:
            method = self._404
        if not callable(method):
            method = self._404
        return method

    def _getAuth(self):
        return getAuth(self.req)

    def _methodHandler(self):
        """Handle either an HTTP POST or GET command."""

        auth = self._getAuth()
        self.authToken = auth

        if type(auth) is int:
            raise apache.SERVER_RETURN, auth

        cfg = conarycfg.ConaryConfiguration(readConfigFiles = False)
        cfg.repositoryMap = self.repServer.map
        cfg.user.addServerGlob(self.repServer.name, auth[0], auth[1])
        self.repos = shimclient.ShimNetClient(
            self.repServer, self._protocol, self._port, auth,
            cfg.repositoryMap, cfg.user)
        self.serverName = self.repServer.name

        if not self.cmd:
            self.cmd = "main"

        try:
            method = self._getHandler(self.cmd)
        except AttributeError:
            raise apache.SERVER_RETURN, apache.HTTP_NOT_FOUND

        d = dict(self.fields)
        d['auth'] = auth
        try:
            output = method(**d)
            self.req.write(output)
            return apache.OK
        except InsufficientPermission:
            if auth[0] == "anonymous":
                # if an anonymous user raises InsufficientPermission,
                # ask for a real login.
                return self._requestAuth()
            else:
                # if a real user raises InsufficientPermission, forbid access.
                return apache.HTTP_FORBIDDEN
        except InvalidPassword:
            # if password is invalid, request a new one
            return self._requestAuth()
        except apache.SERVER_RETURN:
            raise
        except:
            self.req.write(self._write("error", shortError = "Error", error = traceback.format_exc()))
            return apache.OK

    def _requestAuth(self):
        self.req.err_headers_out['WWW-Authenticate'] = \
            'Basic realm="Conary Repository"'
        return apache.HTTP_UNAUTHORIZED

    def _write(self, templateName, **values):
        path = os.path.join(self.templatePath, templateName + ".kid")
        t = kid.load_template(path)
        return t.serialize(encoding="utf-8", cfg = self.cfg, **values)

    @checkAuth(write=True)
    def main(self, auth):
        return self._write("main_page")

    @strFields(char = '')
    @checkAuth(write=False)
    def browse(self, auth, char):
        defaultPage = False
        if not char:
            char = 'A'
            defaultPage = True
        troves = self.repos.troveNamesOnServer(self.serverName)

        # keep a running total of each letter we see so that the display
        # code can skip letters that have no troves
        totals = dict.fromkeys(list(string.digits) + list(string.uppercase), 0)
        packages = []
        components = {}

        # In order to jump to the first letter with troves if no char is specified
        # We have to iterate through troves twice.  Since we have hundreds of troves,
        # not thousands, this isn't too big of a deal.  In any case this will be
        # removed soon when we move to a paginated browser
        for trove in troves:
            totals[trove[0].upper()] += 1
        if defaultPage:
            for x in string.uppercase:
                if totals[x]:
                    char = x
                    break

        if char in string.digits:
            char = '0'
            filter = lambda x: x[0] in string.digits
        else:
            filter = lambda x, char=char: x[0].upper() == char

        for trove in troves:
            if not filter(trove):
                continue
            if ":" not in trove:
                packages.append(trove)
            else:
                package, component = trove.split(":")
                l = components.setdefault(package, [])
                l.append(component)

        # add back troves that do not have a parent package container
        # to the package list
        noPackages = set(components.keys()) - set(packages)
        for x in noPackages:
            for component in components[x]:
                packages.append(x + ":" + component)

        return self._write("browse", packages = sorted(packages), components = components, char = char, totals = totals)

    @strFields(t = None, v = "")
    @checkAuth(write=False)
    def troveInfo(self, auth, t, v):
        t = unquote(t)
        leaves = self.repos.getTroveVersionList(self.serverName, {t: [None]})
        if t not in leaves:
            return self._write("error",
                               error = '%s was not found on this server.' %t)

        versionList = sorted(leaves[t].keys(), reverse = True)

        if not v:
            reqVer = versionList[0]
        else:
            try:
                reqVer = versions.ThawVersion(v)
            except (versions.ParseError, ValueError):
                try:
                    reqVer = versions.VersionFromString(v)
                except:
                    return self._write("error",
                                       error = "Invalid version: %s" %v)

        try:
            query = [(t, reqVer, x) for x in leaves[t][reqVer]]
        except KeyError:
            return self._write("error",
                               error = "Version %s of %s was not found on this server."
                               %(reqVer, t))
        troves = self.repos.getTroves(query, withFiles = False)
        metadata = self.repos.getMetadata([t, reqVer.branch()], reqVer.branch().label())
        if t in metadata:
            metadata = metadata[t]

        return self._write("trove_info", troveName = t, troves = troves,
            versionList = versionList,
            reqVer = reqVer,
            metadata = metadata)

    @strFields(t = None, v = None, f = "")
    @checkAuth(write=False)
    def files(self, auth, t, v, f):
        v = versions.ThawVersion(v)
        f = deps.ThawDependencySet(f)
        parentTrove = self.repos.getTrove(t, v, f, withFiles = False)
        # non-source group troves only show contained troves
        if t.startswith('group-') and not t.endswith(':source'):
            troves = sorted(parentTrove.iterTroveList(strongRefs=True))
            return self._write("group_contents", troveName = t, troves = troves)
        fileIters = []
        # XXX: Needs to be optimized
        # the walkTroveSet() will request a changeset for every
        # trove in the chain.  then iterFilesInTrove() will
        # request it again just to retrieve the filelist.
        for trove in self.repos.walkTroveSet(parentTrove, withFiles = False):
            files = self.repos.iterFilesInTrove(
                trove.getName(),
                trove.getVersion(),
                trove.getFlavor(),
                withFiles = True,
                sortByPath = True)
            fileIters.append(files)
        return self._write("files",
            troveName = t,
            fileIters = itertools.chain(*fileIters))

    @strFields(path = None, pathId = None, fileId = None, fileV = None)
    @checkAuth(write=False)
    def getFile(self, auth, path, pathId, fileId, fileV):
        from mimetypes import guess_type
        from conary.lib import sha1helper

        pathId = sha1helper.md5FromString(pathId)
        fileId = sha1helper.sha1FromString(fileId)
        ver = versions.VersionFromString(fileV)

        fileObj = self.repos.getFileVersion(pathId, fileId, ver)
        contents = self.repos.getFileContents([(fileId, ver)])[0]

        if fileObj.flags.isConfig():
            self.req.content_type = "text/plain"
        else:
            typeGuess = guess_type(path)

            self.req.headers_out["Content-Disposition"] = "attachment; filename=%s;" % path
            if typeGuess[0]:
                self.req.content_type = typeGuess[0]
            else:
                self.req.content_type = "application/octet-stream"

        self.req.headers_out["Content-Length"] = fileObj.sizeString()
        return contents.get().read()

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

        return self._write("pick_trove", troveList = troveList,
            troveName = troveName)

    @checkAuth(write = True)
    @strFields(troveName = "", troveNameList = "", source = "")
    def chooseBranch(self, auth, troveName, troveNameList, source):
        if not troveName:
            if not troveNameList:
                return self._write("error", error = "You must provide a trove name.")
            troveName = troveNameList

        source = source.lower()

        versions = self.repServer.getTroveVersionList(self.authToken,
            netserver.SERVER_VERSIONS[-1], { troveName : None })

        branches = {}
        for version in versions[troveName]:
            version = self.repServer.thawVersion(version)
            branches[version.branch()] = True

        branches = branches.keys()
        if len(branches) == 1:
            self._redirect("getMetadata?troveName=%s;branch=%s" %\
                (troveName, branches[0].freeze()))
        else:
            return self._write("choose_branch",
                           branches = branches,
                           troveName = troveName,
                           source = source)

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
                return self._write("error", error = "No Freshmeat record found.")
        else:
            md = self.troveStore.getMetadata(troveName, branch)

        if not md: # fill a stub
            md = metadata.Metadata(None)

        return self._write("metadata", metadata = md, branch = branch,
                                troveName = troveName)

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
        self._redirect("metadata?troveName=%s" % troveName)

    @checkAuth(admin = True)
    def userlist(self, auth):
        return self._write("user_admin", netAuth = self.repServer.auth)

    @checkAuth(admin = True)
    @strFields(userGroupName = "")
    def addPermForm(self, auth, userGroupName):
        groups = self.repServer.auth.getGroupList()
        labels = self.repServer.auth.getLabelList()
        troves = self.repServer.auth.getItemList()

        return self._write("permission", operation='Add', group=userGroupName, trove=None,
            label=None, groups=groups, labels=labels, troves=troves,
            writeperm=None, capped=None, admin=None)

    @checkAuth(admin = True)
    @strFields(group = None, label = "", trove = "")
    @intFields(writeperm = None, capped = None, admin = None)
    def editPermForm(self, auth, group, label, trove, writeperm, capped, admin):
        groups = self.repServer.auth.getGroupList()
        labels = self.repServer.auth.getLabelList()
        troves = self.repServer.auth.getItemList()

        return self._write("permission", operation='Edit', group=group, label=label,
            trove=trove, groups=groups, labels=labels, troves=troves,
            writeperm=writeperm, capped=capped, admin=admin)

    @checkAuth(admin = True)
    @strFields(group = None, label = "", trove = "",
               writeperm = "off", capped = "off", admin = "off")
    def addPerm(self, auth, group, label, trove,
                writeperm, capped, admin):
        writeperm = (writeperm == "on")
        capped = (capped == "on")
        admin = (admin == "on")

        try:
            self.repServer.addAcl(self.authToken, 0, group, trove, label,
               writeperm, capped, admin)
        except PermissionAlreadyExists, e:
            return self._write("error", shortError="Duplicate Permission",
                error = "Permissions have already been set for %s, please go back and select a different User, Label or Trove." % str(e))

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(group = None, label = "", trove = "",
               oldlabel = "", oldtrove = "",
               writeperm = "off", capped = "off", admin = "off")
    def editPerm(self, auth, group, label, trove, oldlabel, oldtrove,
                writeperm, capped, admin):
        writeperm = (writeperm == "on")
        capped = (capped == "on")
        admin = (admin == "on")

        try:
            self.repServer.editAcl(auth, 0, group, oldtrove, oldlabel, trove,
               label, writeperm, capped, admin)
        except PermissionAlreadyExists, e:
            return self._write("error", shortError="Duplicate Permission",
                error = "Permissions have already been set for %s, please go back and select a different User, Label or Trove." % str(e))

        self._redirect("userlist")

    @checkAuth(admin = True)
    def addGroupForm(self, auth):
        users = self.repServer.auth.userAuth.getUserList()
        return self._write("add_group", modify = False, userGroupName = None, users = users, members = [])

    @checkAuth(admin = True)
    @strFields(userGroupName = None)
    def manageGroupForm(self, auth, userGroupName):
        users = self.repServer.auth.userAuth.getUserList()
        members = set(self.repServer.auth.getGroupMembers(userGroupName))

        return self._write("add_group", userGroupName = userGroupName, users = users, members = members, modify = True)

    @checkAuth(admin = True)
    @strFields(userGroupName = None, newUserGroupName = None)
    @listFields(str, memberList = [])
    def manageGroup(self, auth, userGroupName, newUserGroupName, memberList):
        if userGroupName != newUserGroupName:
            try:
                self.repServer.auth.renameGroup(userGroupName, newUserGroupName)
            except GroupAlreadyExists:
                return self._write("error", shortError="Invalid Group Name",
                    error = "The group name you have chosen is already in use.")

            userGroupName = newUserGroupName

        self.repServer.auth.updateGroupMembers(userGroupName, memberList)

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(newUserGroupName = None)
    @listFields(str, memberList = [])
    def addGroup(self, auth, newUserGroupName, memberList):
        try:
            self.repServer.auth.addGroup(newUserGroupName)
        except GroupAlreadyExists:
            return self._write("error", shortError="Invalid Group Name",
                error = "The group name you have chosen is already in use.")

        self.repServer.auth.updateGroupMembers(newUserGroupName, memberList)

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(userGroupName = None)
    def deleteGroup(self, auth, userGroupName):
        self.repServer.auth.deleteGroup(userGroupName)
        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(group = None, label = None, item = None)
    def deletePerm(self, auth, group, label, item):
        # labelId and itemId are optional parameters so we can't
        # default them to None: the fields decorators treat that as
        # required, so we need to reset them to None here:
        if not label or label == "ALL":
            label = None
        if not item or item == "ALL":
            item = None

        self.repServer.auth.deleteAcl(group, label, item)
        self._redirect("userlist")

    @checkAuth(admin = True)
    def addUserForm(self, auth):
        return self._write("add_user")

    @checkAuth(admin = True)
    @strFields(user = None, password = None)
    @boolFields(write = False, admin = False)
    def addUser(self, auth, user, password, write, admin):
        self.repServer.addUser(self.authToken, 0, user, password)
        self.repServer.addAcl(self.authToken, 0, user, "", "", write, True, admin)

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(username = None)
    def deleteUser(self, auth, username):
        self.repServer.auth.deleteUserByName(username)
        self._redirect("userlist")

    @checkAuth()
    @strFields(username = "")
    def chPassForm(self, auth, username):
        if username:
            askForOld = False
        else:
            username = self.authToken[0]
            askForOld = True

        return self._write("change_password", username = username, askForOld = askForOld)

    @checkAuth()
    @strFields(username = None, oldPassword = "",
               password1 = None, password2 = None)
    def chPass(self, auth, username, oldPassword,
               password1, password2):
        admin = self.repServer.auth.check(self.authToken, admin=True)

        if username != self.authToken[0]:
            if not admin:
                raise InsufficientPermission

        if self.authToken[1] != oldPassword and self.authToken[0] == username and not admin:
            return self._write("error", error = "Error: old password is incorrect")
        elif password1 != password2:
            return self._write("error", error = "Error: passwords do not match")
        elif oldPassword == password1:
            return self._write("error", error = "Error: old and new passwords identical, not changing")
        else:
            self.repServer.auth.changePassword(username, password1)
            if admin:
                returnLink = ("User Administration", "userlist")
            else:
                returnLink = ("Main Menu", "main")

            return self._write("notice", message = "Password successfully changed",
                link = returnLink[0], url = returnLink[1])

    @checkAuth(admin=True)
    @strFields(key=None, owner="")
    def pgpChangeOwner(self, auth, owner, key):
        if not owner or owner == '--Nobody--':
            owner = None
        self.repServer.changePGPKeyOwner(self.authToken, 0, owner, key)
        self._redirect('pgpAdminForm')

    @checkAuth(write = True)
    def pgpAdminForm(self, auth):
        admin = self.repServer.auth.check(self.authToken,admin=True)
        userId = self.repServer.auth.getUserIdByName(self.authToken[0])

        if admin:
            users = dict(self.repServer.auth.iterUsers())
            users[None] = '--Nobody--'
        else:
            users = {userId: self.authToken[0]}

        # build a dict of useful information about each user's OpenPGP Keys
        # xml-rpc calls must be made before kid template is invoked
        openPgpKeys = {}
        for userId in users.keys():
            keys = []
            for fingerprint in self.repServer.listUsersMainKeys(self.authToken, 0, userId):
                keyPacket = {}
                keyPacket['fingerprint'] = fingerprint
                keyPacket['subKeys'] = self.repServer.listSubkeys(self.authToken, 0, fingerprint)
                keyPacket['uids'] = self.repServer.getOpenPGPKeyUserIds(self.authToken, 0, fingerprint)
                keys.append(keyPacket)
            openPgpKeys[userId] = keys

        return self._write("pgp_admin", users = users, admin=admin, openPgpKeys = openPgpKeys)

    @checkAuth(write = True)
    def pgpNewKeyForm(self, auth):
        return self._write("pgp_submit_key")

    @checkAuth(write = True)
    @strFields(keyData = "")
    def submitPGPKey(self, auth, keyData):
        self.repServer.addNewAsciiPGPKey(self.authToken, 0, self.authToken[0], keyData)
        self._redirect('pgpAdminForm')

    @strFields(search = '')
    @checkAuth(write = False)
    def getOpenPGPKey(self, auth, search, **kwargs):
        from conary.lib.openpgpfile import KeyNotFound
        # This function mimics limited key server behavior. The keyserver line
        # for a gpg command must be formed manually--because gpg doesn't
        # automatically know how to talk to limited key servers.
        # A correctly formed gpg command looks like:
        # 'gpg --keyserver=REPO_MAP/getOpenPGPKey?search=KEY_ID --recv-key KEY_ID'
        # example: 'gpg --keyserver=http://admin:111111@localhost/conary/getOpenPGPKey?search=F7440D78FE813C882212C2BF8AC2828190B1E477 --recv-key F7440D78FE813C882212C2BF8AC2828190B1E477'
        # repositories that allow anonymous users do not require userId/passwd
        try:
            keyData = self.repServer.getAsciiOpenPGPKey(self.authToken, 0, search)
        except KeyNotFound:
            return self._write("error", shortError = "Key Not Found", error = "OpenPGP Key %s is not in this repository" %search)
        return self._write("pgp_get_key", keyId = search, keyData = keyData)


def flavorWrap(f):
    f = str(f).replace(" ", "\n")
    f = f.replace(",", " ")
    f = f.replace("\n", "\t")
    f = textwrap.wrap(f, expand_tabs=False, replace_whitespace=False)
    return ",\n".join(x.replace(" ", ",") for x in f)
