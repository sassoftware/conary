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


from urllib import unquote
import itertools
import kid
import os
import string
import textwrap
import webob
from webob import exc

from conary import trove
from conary import versions
from conary import conarycfg
from conary.deps import deps
from conary.errors import WebError
from conary.repository import shimclient, errors
from conary.server import templates
from conary.web.fields import strFields, intFields, listFields, boolFields
from conary.web.webauth import getAuth


def checkAuth(write=False, admin=False):
    def deco(func):
        def wrapped(self, **kwargs):
            if write and not self.hasWrite:
                raise exc.HTTPForbidden()
            if admin and not self.isAdmin:
                raise exc.HTTPForbidden()
            return func(self, **kwargs)
        return wrapped
    return deco


class ReposWeb(object):

    responseFactory = webob.Response

    def __init__(self, cfg, repositoryServer, authToken=None):
        self.cfg = cfg
        self.repServer = repositoryServer
        self.authToken = authToken
        #self.repServer.__class__ = shimclient.NetworkRepositoryServer
        self.templatePath = os.path.dirname(templates.__file__)

    # Request processing

    def _handleRequest(self, request):
        self.request = request
        try:
            try:
                return self._getResponse()
            except exc.HTTPException, err:
                return err
        finally:
            self.repServer.reset()
            self.repos = None

    def _getResponse(self):
        if self.authToken is None:
            self.authToken = getAuth(self.request)
        auth = self.authToken

        # Repository setup
        self.serverNameList = self.repServer.serverNameList
        cfg = conarycfg.ConaryConfiguration(readConfigFiles=False)
        cfg.repositoryMap = self.repServer.map
        for serverName in self.serverNameList:
            cfg.user.addServerGlob(serverName, auth[0], auth[1])
        self.repos = shimclient.ShimNetClient(
                server=self.repServer,
                protocol=self.request.scheme,
                port=self.request.server_port,
                authToken=auth,
                cfg=cfg,
                )

        # Check if the request is sane
        methodName = self.request.path_info_peek() or 'main'
        method = None
        if methodName and methodName[0] != '_':
            method = getattr(self, methodName, None)
        if not method:
            raise exc.HTTPNotFound()
        self.methodName = methodName

        # Do authn/authz checks
        if auth[0] != 'anonymous':
            self.loggedIn = self.repServer.auth.checkPassword(auth)
            if not self.loggedIn:
                return self._requestAuth()
        else:
            self.loggedIn = False

        # Run the method
        self.hasWrite = self.repServer.auth.check(auth, write=True)
        self.isAdmin = self.repServer.auth.authCheck(auth, admin=True)
        params = self.request.params.mixed()
        try:
            result = method(auth=auth, **params)
        except (exc.HTTPForbidden, errors.InsufficientPermission):
            if self.loggedIn:
                raise exc.HTTPForbidden()
            else:
                return self._requestAuth()
        except WebError, err:
            result = self._write("error", error=str(err))

        # Convert response if necessary
        if isinstance(result, basestring):
            result = self.responseFactory(
                    body=result,
                    content_type='text/html',
                    )
        return result

    # Helper methods

    def _requestAuth(self, detail=None):
        raise exc.HTTPUnauthorized(
                detail=detail,
                headers=[('WWW-Authenticate',
                    'Basic realm="Conary Repository"')],
                )

    def _write(self, templateName, **values):
        path = os.path.join(self.templatePath, templateName + ".kid")
        t = kid.load_template(path)
        return t.serialize(encoding = "utf-8",
                           output = 'xhtml-strict',

                           cfg = self.cfg,
                           methodName = self.methodName,
                           hasWrite = self.hasWrite,
                           loggedIn = self.loggedIn,
                           isAdmin = self.isAdmin,
                           isAnonymous = not self.loggedIn,
                           hasEntitlements = True,
                           currentUser = self.authToken[0],
                           **values)

    def _redirect(self, url):
        url = self.request.relative_url(url, to_application=True)
        raise exc.HTTPFound(location=url)

    @checkAuth(write=False)
    def main(self, auth):
        self._redirect("browse")

    @checkAuth(write=True)
    def login(self, auth):
        self._redirect("browse")

    def logout(self, auth):
        if self.loggedIn:
            self._requestAuth()
        else:
            self._redirect('browse')

    @strFields(char = '')
    @checkAuth(write=False)
    def browse(self, auth, char):
        defaultPage = False
        if not char:
            char = 'A'
            defaultPage = True
        # since the repository is multihomed and we're not doing any
        # label filtering, a single call will return all the available
        # troves. We use the first repository name here because we have to
        # pick one,,,
        troves = self.repos.troveNamesOnServer(self.serverNameList[0])

        # keep a running total of each letter we see so that the display
        # code can skip letters that have no troves
        totals = dict.fromkeys(list(string.digits) + list(string.uppercase), 0)
        packages = []
        components = {}

        # In order to jump to the first letter with troves if no char is specified
        # We have to iterate through troves twice.  Since we have hundreds of troves,
        # not thousands, this isn't too big of a deal.  In any case this will be
        # removed soon when we move to a paginated browser
        for trv in troves:
            totals[trv[0].upper()] += 1
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

        for trv in troves:
            if not filter(trv):
                continue
            if ":" not in trv:
                packages.append(trv)
            else:
                package, component = trv.split(":")
                l = components.setdefault(package, [])
                l.append(component)

        # add back troves that do not have a parent package container
        # to the package list
        noPackages = set(components.keys()) - set(packages)
        for x in noPackages:
            for component in components[x]:
                packages.append(x + ":" + component)

        return self._write("browse", packages = sorted(packages),
                           components = components, char = char, totals = totals)

    @strFields(t = None, v = "")
    @checkAuth(write=False)
    def troveInfo(self, auth, t, v):
        t = unquote(t)
        leaves = {}
        for serverName in self.serverNameList:
            newLeaves = self.repos.getTroveVersionList(serverName, {t: [None]})
            leaves.update(newLeaves)
        if t not in leaves:
            return self._write("error",
                               error = '%s was not found on this server.' %t)

        versionList = sorted(leaves[t].keys(), reverse = True)

        if not v:
            reqVer = versionList[0]
        else:
            try:
                reqVer = versions.VersionFromString(v)
            except (versions.ParseError, ValueError):
                try:
                    reqVer = versions.ThawVersion(v)
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
        mdata = self.repos.getMetadata([t, reqVer.branch()], reqVer.branch().label())
        if t in mdata:
            mdata = mdata[t]

        return self._write("trove_info", troveName = t, troves = troves,
            versionList = versionList,
            reqVer = reqVer,
            metadata = mdata)

    @strFields(t = None, v = None, f = "")
    @checkAuth(write=False)
    def files(self, auth, t, v, f):
        try:
            v = versions.VersionFromString(v)
        except (versions.ParseError, ValueError):
            v = versions.ThawVersion(v)
        f = deps.ThawFlavor(f)
        parentTrove = self.repos.getTrove(t, v, f, withFiles = False)
        # non-source group troves only show contained troves
        if trove.troveIsGroup(t):
            troves = sorted(parentTrove.iterTroveList(strongRefs=True))
            return self._write("group_contents", troveName = t, troves = troves)
        fileIters = []
        for n, v, f in self.repos.walkTroveSet(parentTrove, withFiles = False):
            files = self.repos.iterFilesInTrove(n, v, f,
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

        fileStream = self.repos.getFileVersion(pathId, fileId, ver)
        try:
            contents = self.repos.getFileContents([(fileId, ver)])[0]
        except (errors.FileStreamMissing, errors.FileStreamNotFound):
            return self._write("error",
                    error="The content of that file is not available.")
        except errors.FileHasNoContents, err:
            return self._write("error", error=str(err))

        response = self.responseFactory(body_file=contents.get())

        if fileStream.flags.isConfig():
            response.content_type = "text/plain"
        else:
            typeGuess = guess_type(path)
            response.content_disposition = "attachment; filename=%s;" % path
            if typeGuess[0]:
                response.content_type = typeGuess[0]
            else:
                response.content_type = "application/octet-stream"
        response.charset = None
        response.content_length = fileStream.contents.size()
        return response

    @checkAuth(admin = True)
    def userlist(self, auth):
        return self._write("user_admin",
                netAuth=self.repServer.auth,
                ri=self.repServer.ri,
                )

    @checkAuth(admin = True)
    @strFields(roleName = "")
    def addPermForm(self, auth, roleName):
        roles = self.repServer.auth.getRoleList()
        labels = self.repServer.auth.getLabelList()
        troves = self.repServer.auth.getItemList()

        return self._write("permission", operation='Add',
                           role=roleName, trove=None, label=None,
                           roles=roles, labels=labels, troves=troves,
                           writeperm=None, admin=None, remove=None)

    @checkAuth(admin = True)
    @strFields(role = None, label = "", trove = "")
    @intFields(writeperm = None, remove = None)
    def editPermForm(self, auth, role, label, trove, writeperm,
                     remove):
        roles = self.repServer.auth.getRoleList()
        labels = self.repServer.auth.getLabelList()
        troves = self.repServer.auth.getItemList()

        #remove = 0
        return self._write("permission", operation='Edit', role=role,
                           label=label, trove=trove, roles=roles,
                           labels=labels, troves=troves,
                           writeperm=writeperm, remove=remove)

    @checkAuth(admin = True)
    @strFields(role = None, label = "", trove = "",
               writeperm = "off", admin = "off", remove = "off")
    def addPerm(self, auth, role, label, trove, writeperm, admin, remove):
        writeperm = (writeperm == "on")
        admin = (admin == "on")
        remove = (remove== "on")

        try:
            self.repServer.addAcl(self.authToken, 60, role, trove, label,
                                  write = writeperm, remove = remove)
        except errors.PermissionAlreadyExists, e:
            return self._write("error", shortError = "Duplicate Permission",
                               error = ("Permissions have already been set "
                                        "for %s, please go back and select a "
                                        "different User, Label or Trove."
                                        % str(e)))

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(role = None, label = "", trove = "",
               oldlabel = "", oldtrove = "",
               writeperm = "off", remove = "off")
    def editPerm(self, auth, role, label, trove, oldlabel, oldtrove,
                writeperm, remove):
        writeperm = (writeperm == "on")
        remove = (remove == "on")

        try:
            self.repServer.editAcl(auth, 60, role, oldtrove, oldlabel,
                                   trove, label, write = writeperm,
                                   canRemove = remove)
        except errors.PermissionAlreadyExists, e:
            return self._write("error", shortError="Duplicate Permission",
                               error = ("Permissions have already been set "
                                        "for %s, please go back and select "
                                        "a different User, Label or Trove."
                                        % str(e)))

        self._redirect("userlist")

    @checkAuth(admin = True)
    def addRoleForm(self, auth):
        users = self.repServer.auth.userAuth.getUserList()
        return self._write("add_role", modify = False, role = None,
                           users = users, members = [], canMirror = False,
                           roleIsAdmin=False,
                           acceptFlags='',
                           troveAccess=None,
                           )

    @checkAuth(admin = True)
    @strFields(roleName = None)
    def manageRoleForm(self, auth, roleName):
        users = self.repServer.auth.userAuth.getUserList()
        members = set(self.repServer.auth.getRoleMembers(roleName))
        canMirror = self.repServer.auth.roleCanMirror(roleName)
        roleIsAdmin = self.repServer.auth.roleIsAdmin(roleName)
        flags = self.repServer.auth.getRoleFilters([roleName])[roleName]
        troveAccess = [((n, versions.VersionFromString(v), deps.ThawFlavor(f)), recursive)
                for ((n, v, f), recursive)
                in self.repServer.ri.listTroveAccess(roleName)]

        return self._write("add_role", role = roleName,
                           users = users, members = members,
                           canMirror = canMirror, roleIsAdmin = roleIsAdmin,
                           modify=True,
                           acceptFlags=flags[0],
                           troveAccess=troveAccess,
                           )

    @checkAuth(admin = True)
    @strFields(roleName = None, newRoleName = None, acceptFlags='')
    @listFields(str, memberList = [])
    @intFields(canMirror = False)
    @intFields(roleIsAdmin = False)
    def manageRole(self, auth, roleName, newRoleName, memberList,
                   canMirror, roleIsAdmin, acceptFlags):
        if roleName != newRoleName:
            try:
                self.repServer.auth.renameRole(roleName, newRoleName)
            except errors.RoleAlreadyExists:
                return self._write("error", shortError="Invalid Role Name",
                    error = "The role name you have chosen is already in use.")

            roleName = newRoleName

        self.repServer.auth.updateRoleMembers(roleName, memberList)
        self.repServer.auth.setMirror(roleName, canMirror)
        self.repServer.auth.setAdmin(roleName, roleIsAdmin)
        acceptFlags = deps.parseFlavor(acceptFlags, raiseError=True)
        self.repServer.auth.setRoleFilters({roleName: (acceptFlags, None)})

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(newRoleName = None, acceptFlags='')
    @listFields(str, memberList = [])
    @intFields(canMirror = False)
    @intFields(roleIsAdmin = False)
    def addRole(self, auth, newRoleName, memberList, canMirror,
                roleIsAdmin, acceptFlags):
        try:
            self.repServer.auth.addRole(newRoleName)
        except errors.RoleAlreadyExists:
            return self._write("error", shortError="Invalid Role Name",
                error = "The role name you have chosen is already in use.")

        self.repServer.auth.updateRoleMembers(newRoleName, memberList)
        self.repServer.auth.setMirror(newRoleName, canMirror)
        self.repServer.auth.setAdmin(newRoleName, roleIsAdmin)
        acceptFlags = deps.parseFlavor(acceptFlags, raiseError=True)
        self.repServer.auth.setRoleFilters({newRoleName: (acceptFlags, None)})

        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(roleName = None)
    def deleteRole(self, auth, roleName):
        self.repServer.auth.deleteRole(roleName)
        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(role = None, label = None, item = None)
    def deletePerm(self, auth, role, label, item):
        # labelId and itemId are optional parameters so we can't
        # default them to None: the fields decorators treat that as
        # required, so we need to reset them to None here:
        if not label or label == "ALL":
            label = None
        if not item or item == "ALL":
            item = None

        self.repServer.auth.deleteAcl(role, label, item)
        self._redirect("userlist")

    @checkAuth(admin = True)
    def addUserForm(self, auth):
        return self._write("add_user")

    @checkAuth(admin = True)
    @strFields(user = None, password = None)
    @boolFields(write = False, admin = False, remove = False)
    def addUser(self, auth, user, password, write, admin, remove):
        self.repServer.addUser(self.authToken, 0, user, password)
        self._redirect("userlist")

    @checkAuth(admin = True)
    @strFields(username = None)
    def deleteUser(self, auth, username):
        self.repServer.auth.deleteUserByName(username)
        self._redirect("userlist")

    @checkAuth()
    @strFields(username = "")
    def chPassForm(self, auth, username):
        if not self.loggedIn:
            return self._requestAuth()
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
        admin = self.repServer.auth.authCheck(self.authToken, admin=True)

        if username != self.authToken[0]:
            if not admin:
                raise errors.InsufficientPermission

        if self.authToken[1] != oldPassword and self.authToken[0] == username and not admin:
            return self._write("error", error = "Error: old password is incorrect")
        elif password1 != password2:
            return self._write("error", error = "Error: passwords do not match")
        elif oldPassword == password1:
            return self._write("error", error = "Error: old and new passwords identical, not changing")
        else:
            message = "Password successfully changed."
            self.repServer.auth.changePassword(username, password1)
            if admin:
                returnLink = ("User Administration", "userlist")
            else:
                message += " You should close your web browser and log back in again for changes to take effect."
                returnLink = ("Main Menu", "main")

            return self._write("notice", message = message,
                link = returnLink[0], url = returnLink[1])

    @checkAuth()
    @strFields(entClass = None)
    def addEntitlementKeyForm(self, auth, entClass):
        return self._write("add_ent_key", entClass = entClass)

    @checkAuth()
    @strFields(entClass = None)
    def configEntClassForm(self, auth, entClass):
        allRoles = self.repServer.auth.getRoleList()

        ownerRole = self.repServer.auth.getEntitlementClassOwner(auth, entClass)
        currentRoles = self.repServer.auth.getEntitlementClassesRoles(
            auth, [entClass])[entClass]

        return self._write("add_ent_class", allRoles = allRoles,
                           entClass = entClass, ownerRole = ownerRole,
                           currentRoles = currentRoles)

    @checkAuth()
    @strFields(entClass = None)
    def deleteEntClass(self, auth, entClass):
        self.repServer.auth.deleteEntitlementClass(auth, entClass)
        self._redirect('manageEntitlements')

    @checkAuth()
    @strFields(entClass = None, entKey = None)
    def addEntitlementKey(self, auth, entClass, entKey):
        try:
            self.repServer.auth.addEntitlementKey(auth, entClass, entKey)
        except errors.EntitlementKeyAlreadyExists:
            return self._write("error",
                               error="Entitlement key already exists")
        self._redirect('manageEntitlementForm?entClass=%s' % entClass)

    @checkAuth()
    @strFields(entClass = None, entKey = None)
    def deleteEntitlementKey(self, auth, entClass, entKey):
        self.repServer.auth.deleteEntitlementKey(auth, entClass, entKey)
        self._redirect('manageEntitlementForm?entClass=%s' % entClass)

    @checkAuth()
    def manageEntitlements(self, auth):
        entClassList = self.repServer.listEntitlementClasses(auth, 0)

        if self.isAdmin:
            entClassInfo = [
                (x, self.repServer.auth.getEntitlementClassOwner(auth, x),
                 self.repServer.auth.getEntitlementClassesRoles(auth, [x])[x])
                for x in entClassList ]
        else:
            entClassInfo = [ (x, None, None) for x in entClassList ]

        roles = self.repServer.auth.getRoleList()

        return self._write("manage_ents", entClasses = entClassInfo,
                           roles = roles)

    @checkAuth(admin = True)
    def addEntClassForm(self, auth):
        allRoles = self.repServer.auth.getRoleList()
        return self._write("add_ent_class", allRoles = allRoles,
                           entClass = None, ownerRole = None,
                           currentRoles = [])

    @checkAuth()
    @strFields(entClass = None, entOwner = None)
    @listFields(str, roles = [])
    def addEntClass(self, auth, entOwner, roles, entClass):
        if len(roles) < 1:
            return self._write("error", error="No roles specified")
        try:
            self.repServer.auth.addEntitlementClass(auth, entClass,
                                                    roles[0])
            self.repServer.auth.setEntitlementClassesRoles(
                auth, { entClass : roles })
        except errors.RoleNotFound:
            return self._write("error", error="Role does not exist")
        except errors.EntitlementClassAlreadyExists:
            return self._write("error",
                               error="Entitlement class already exists")
        if entOwner != '*none*':
            self.repServer.auth.addEntitlementClassOwner(auth, entOwner,
                                                         entClass)

        self._redirect('manageEntitlements')

    @checkAuth()
    @strFields(entClass = None, entOwner = None)
    @listFields(str, roles = [])
    def configEntClass(self, auth, entOwner, roles, entClass):
        self.repServer.auth.setEntitlementClassesRoles(auth,
                                                       { entClass : roles } )
        if entOwner != '*none*':
            self.repServer.auth.addEntitlementClassOwner(auth, entOwner,
                                                         entClass)

        self._redirect('manageEntitlements')

    @checkAuth()
    @strFields(entClass = None)
    def manageEntitlementForm(self, auth, entClass):
        entKeys = [ x for x in
                    self.repServer.auth.iterEntitlementKeys(auth, entClass) ]
        return self._write("entlist", entKeys = entKeys,
                           entClass = entClass)

    @checkAuth(admin=True)
    @strFields(key=None, owner="")
    def pgpChangeOwner(self, auth, owner, key):
        if not owner or owner == '--Nobody--':
            owner = None
        self.repServer.changePGPKeyOwner(self.authToken, 0, owner, key)
        self._redirect('pgpAdminForm')

    @checkAuth(write = True)
    def pgpAdminForm(self, auth):
        admin = self.repServer.auth.authCheck(self.authToken,admin=True)

        if admin:
            users = self.repServer.auth.userAuth.getUserList()
            users.append('--Nobody--')
        else:
            users = [ self.authToken[0] ]

        # build a dict of useful information about each user's OpenPGP Keys
        # xml-rpc calls must be made before kid template is invoked
        openPgpKeys = {}
        for user in users:
            keys = []
            if user == '--Nobody--':
                userLookup = None
            else:
                userLookup = user

            for fingerprint in self.repServer.listUsersMainKeys(self.authToken, 0, userLookup):
                keyPacket = {}
                keyPacket['fingerprint'] = fingerprint
                keyPacket['subKeys'] = self.repServer.listSubkeys(self.authToken, 0, fingerprint)
                keyPacket['uids'] = self.repServer.getOpenPGPKeyUserIds(self.authToken, 0, fingerprint)
                keys.append(keyPacket)
            openPgpKeys[user] = keys

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
