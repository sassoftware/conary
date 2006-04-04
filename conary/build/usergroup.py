#
# Copyright (c) 2004-2005 rPath, Inc.
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

# conary imports
from conary.build import build, errors
from conary.lib import util

class User(build.BuildAction):
    """
    Provides information to use if Conary needs to create a user:
    C{r.User('I{name}', I{preferred_uid}, group='I{maingroupname}', groupid=I{preferred_gid}, homedir='I{/home/dir}', comment='I{comment}', shell='I{/path/to/shell}',  {supplemental=[I{group}, ...]}, {saltedPassword='I{saltedPassword}')}

    The defaults are::
      - C{group}: same name as the user
      - C{groupid}: same id as the user
      - C{homedir}: None
      - C{comment}: None
      - C{saltedPassword}: None
      - C{shell}: C{'/sbin/nologin'}
      - C{supplemental}: None (list of supplemental groups for this user)

    The easiest way to get a salted password is to set that password for a 
    user on your system and then cut and paste the salted value from the
    /etc/shadow file.

    NOTE: Pre-setting a salted password should be done with caution.  Anyone
    who is able to access the repository where this info file will be stored
    will have the salted password, and given enough time will be able to
    recover the original password.  Trust the security of this password as
    far as you trust the security of the repository it is stored in.
    """
    def __init__(self, recipe, *args, **keywords):
        if recipe.type != 'user':
            raise UserGroupError, 'User() allowed only in UserInfoRecipe'
        args=list(args)
        args.extend([None] * (9 - len(args)))
	(self.infoname, self.preferred_uid, self.group,
         self.groupid, self.homedir, self.comment, self.shell,
         self.supplemental, self.saltedPassword) = args
        if self.shell is None: self.shell = '/sbin/nologin'
	build.BuildAction.__init__(self, recipe, [], **keywords)

    def do(self, macros):
        if self.recipe.infofilename:
            raise UserGroupError, 'Only one instance of User per recipe'
        # interpolate macros
        self.infoname = self.infoname %macros
        self.recipe.infoname = self.infoname
        if self.recipe.name != 'info-%s' %self.infoname:
            raise UserGroupError, 'User name must be the same as package name'
        d = '%(destdir)s%(userinfodir)s/' %macros
        util.mkdirChain(d)
        self.recipe.infofilename='%s/%s' %(macros.userinfodir, self.infoname)
        self.recipe.realfilename='%s%s' %(
            macros.destdir, self.recipe.infofilename)
        f = file(self.recipe.realfilename, 'w')
        f.write('PREFERRED_UID=%d\n' %self.preferred_uid)
        if self.group:
            self.group = self.group %macros
            f.write('GROUP=%s\n' %self.group)
            self.recipe.groupname = self.group
        else:
            self.recipe.groupname = self.infoname
        if self.groupid:
            f.write('GROUPID=%d\n' %self.groupid)
        if self.homedir:
            self.homedir = self.homedir %macros
            f.write('HOMEDIR=%s\n' %self.homedir)
        if self.comment:
            self.comment = self.comment %macros
            f.write('COMMENT=%s\n' %self.comment)
        if self.shell:
            self.shell = self.shell %macros
            f.write('SHELL=%s\n' %self.shell)
        if self.supplemental:
            self.supplemental = [ x %macros for x in self.supplemental ]
            f.write('SUPPLEMENTAL=%s\n' %(','.join(self.supplemental)))
            for group in self.supplemental:
                self.recipe.requiresGroup(group)
        if self.saltedPassword:
            if self.saltedPassword[0] != '$' or len(self.saltedPassword) != 34:
                raise UserGroupError('"%s" is not a valid md5 salted password.'
                                     ' Use md5pw (installed with conary) to '
                                     ' create a valid password.' 
                                     % self.saltedPassword)
            f.write('PASSWORD=%s\n' % self.saltedPassword)
        f.close()


class SupplementalGroup(build.BuildAction):
    """
    Requests the Conary ensure that a user be associated with a
    supplemental group that is not associated with any user::
    C{r.SupplementalGroup('I{user}', 'I{group}', I{preferred_gid})}
    """
    def __init__(self, recipe, *args, **keywords):
        if recipe.type != 'group':
            raise UserGroupError, 'SupplementalGroup() allowed only in GroupInfoRecipe'
	(self.user, self.infoname, self.preferred_gid) = args
	build.BuildAction.__init__(self, recipe, [], **keywords)

    def do(self, macros):
        if self.recipe.infofilename:
            raise UserGroupError, 'Only one Group defined per recipe'
        self.infoname = self.infoname %macros
        self.recipe.infoname = self.infoname
        if self.recipe.name != 'info-%s' %self.infoname:
            raise UserGroupError, 'Group name must be the same as package name'
        d = '%(destdir)s%(groupinfodir)s/' %macros
        util.mkdirChain(d)
        self.recipe.infofilename='%s/%s' %(macros.groupinfodir, self.infoname)
        self.recipe.realfilename='%s%s' %(
            macros.destdir, self.recipe.infofilename)
        f = file(self.recipe.realfilename, 'w')
        f.write('PREFERRED_GID=%d\n' %self.preferred_gid)
        self.user = self.user %macros
        self.recipe.requiresUser(self.user)
        f.write('USER=%s\n' %self.user)
        f.close()



class Group(build.BuildAction):
    """
    Provides information to use if Conary needs to create a group:
    C{r.Group('I{group}', I{preferred_gid})}
    This is used only for groups that exist independently, never
    for a main group created by C{r.User()}
    """
    def __init__(self, recipe, *args, **keywords):
        if recipe.type != 'group':
            raise UserGroupError, 'Group() allowed only in GroupInfoRecipe'
	(self.infoname, self.preferred_gid) = args
	build.BuildAction.__init__(self, recipe, [], **keywords)

    def do(self, macros):
        if self.recipe.infofilename:
            raise UserGroupError, 'Only one Group defined per recipe'
        self.infoname = self.infoname %macros
        self.recipe.infoname = self.infoname
        if self.recipe.name != 'info-%s' %self.infoname:
            raise UserGroupError, 'Group name must be the same as package name'
        d = '%(destdir)s%(groupinfodir)s/' %macros
        util.mkdirChain(d)
        self.recipe.infofilename='%s/%s' %(macros.groupinfodir, self.infoname)
        self.recipe.realfilename='%s%s' %(
            macros.destdir, self.recipe.infofilename)
        f = file(self.recipe.realfilename, 'w')
        f.write('PREFERRED_GID=%d\n' %self.preferred_gid)
        f.close()


class UserGroupError(errors.CookError):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
