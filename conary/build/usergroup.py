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
    NAME
    ====
 
    B{C{r.User()}} - Provides user account creation information

    SYNOPSIS
    ========
    C{r.User('I{name}', I{preferred_uid}, group='I{maingroupname}', groupid=I{preferred_gid}, homedir='I{/home/dir}', comment='I{comment}', shell='I{/path/to/shell}',  {supplemental=[I{group}, ...]}, {saltedPassword='I{saltedPassword}')}}

    DESCRIPTION
    ===========
    The C{r.User} class provides user account information to Conary for the
    purpose of user account creation.

    The easiest way to get a salted password is to use the
    /usr/share/conary/md5pw program installed with Conary.
    Alternatively, set that password for a user on your system
    and then cut and paste the salted value from the /etc/shadow file.

    NOTE: Pre-setting a salted password should be done with caution.  Anyone
    who is able to access the repository where this info file will be stored
    will have the salted password, and given enough time will be able to
    recover the original password.  Trust the security of this password as
    far as you trust the security of the repository it is stored in.
    KEYWORDS
    ========

    The C{r.User} class accepts the following keyword arguments, with
    default values shown in parentheses where applicable.

    B{name} : (None) Specify a user name for the account to be created

    B{preferred_uid} : The preferred user identification number for the
    account

    B{group} : (same as user name) Specify default group for the account

    B{groupid} : (same as UID) Specify default group identification number
    for the account

    B{homedir} : (None) Specify the account home directory

    B{comment} : (None) Add a comment to the account record

    B{saltedPassword} : (None) Specify a salted password for the account
 
    B{shell} : (C{'/sbin/nologin'}) Specify the user account shell

    B{supplemental} : (None) Specify a list of additional group memberships
    for the account.

    EXAMPLES
    ========

    C{r.User('mysql', 27, comment='mysql', homedir='%(localstatedir)s/lib/mysql', shell='%(essentialbindir)s/bash')}

    Uses C{r.User} to define a C{mysql} user with a specific UID value of
    '27', a home directory value of C{/var/lib/mysql}, and the default shell
    value of C{/bin/bash}. 
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
    NAME
    ====

    B{C{r.SupplementalGroup()}} - Ensures a user is associated with a supplemental group

    SYNOPSIS
    ========

    C{r.SupplementalGroup('I{user}', 'I{group}', I{preferred_gid})}

    DESCRIPTION
    ===========
    
    The C{r.SupplementalGroup} class ensures that a user is associated with a
    supplemental group that is not associated with any user.

    KEYWORDS
    ========

    The C{r.SupplementalGroup} class accepts the following keyword arguments,
    with default values shown in parentheses where applicable. 
 
    B{user} : (None) Specify the user name to be associated with a
    supplemental group

    B{group} : (None) Specify the supplemental group name

    B{preferred_gid} : (None) Specify the supplemental group identification
    number

    EXAMPLES
    ========

    C{r.SupplementalGroup('breandon', 'ateam', 560)}

    Uses C{r.SupplementalGroup} to add the user C{breandon} to the
    supplemental group C{ateam}, and specifies the preferred group
    identification number value of '560'.
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
    NAME
    ====

    B{C{r.Group()}} - Provides group creation information

    SYNOPSIS
    ========
    
    C{r.Group('I{group}', I{preferred_gid})}

    DESCRIPTION
    ===========
    
    The C{r.Group} class provides group information to Conary for the purpose    of group creation, and should be used only for groups that exist
    independently, never for a main group created by C{r.User()}.

    KEYWORDS
    ========

    The C{r.User} class accepts the following keyword arguments, with
    default values shown in parentheses where applicable.

    B{group} : (None) Specify a group name

    B{preferred_gid} : (None) Specify the group identification number

    EXAMPLES
    ========

    C{r.Group('mem', 8)}

    Uses C{r.Group} to created a group named C{mem} with a group 
    identification number value of '8'.
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
