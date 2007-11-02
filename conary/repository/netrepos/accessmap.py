#
# Copyright (c) 2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from conary.server import schema
from conary.repository import errors
from conary.repository.netrepos import instances, versionops
from conary.lib.tracelog import logMe

# - Entries in the UserGroupTroves table are processed and flattened
#   into the UserGroupAllTroves table
# - Entries in the Permissions table are processed and flattened into
#   the UserGroupAllPermissions table
# - Then, UserGroupAllTroves and UserGroupAllPermissions are summarized
#   in the UserGroupInstancesCache table

# base class for handling UGAP and UGAT
class UserGroupTable:
    def __init__(self, db):
        self.db = db
    def getWhereArgs(self, cond = "where", **kw):
        where = []
        args = []
        for key, val in kw.items():
            if val is None:
                continue
            where.append("%s = ?" % (key,))
            args.append(val)
        if len(where):
            where = cond + " " + " and ".join(where)
        else:
            where = ""
        return (where, args)
    
# class and methods for handling UserGroupTroves operations
class UserGroupTroves(UserGroupTable):
    # given a list of (n,v,f) tuples, convert them to instanceIds in
    # the tmpInstanceId table
    def _findInstanceIds(self, troveList, checkMissing=True):
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpNVF")
        schema.resetTable(cu, "tmpInstanceId")
        for (n,v,f) in troveList:
            cu.execute("insert into tmpNVF (name, version, flavor) "
                       "values (?,?,?)", (n,v,f), start_transaction=False)
        self.db.analyze("tmpNVF")
        cu.execute("""
        insert into tmpInstanceId (idx, instanceId)
        select tmpNVF.idx, Instances.instanceId
        from tmpNVF
        join Items on tmpNVF.name = Items.item
        join Versions on tmpNVF.version = Versions.version
        join Flavors on tmpNVF.flavor = Flavors.flavor
        join Instances on
            Instances.itemId = Items.itemId and
            Instances.versionId = Versions.versionId and
            Instances.flavorId = Flavors.flavorId
        where
            Instances.isPresent in (%d,%d)
        """ % (instances.INSTANCE_PRESENT_NORMAL,
               instances.INSTANCE_PRESENT_HIDDEN),
                   start_transaction=False)
        self.db.analyze("tmpInstances")
        # check if any troves specified are missing
        cu.execute("""
        select tmpNVF.idx, name, version, flavor
        from tmpNVF
        left join tmpInstanceId using(idx)
        where tmpInstanceId.instanceId is NULL
        """)
        if checkMissing:
            # granting permissions to a !present trove has a fuzzy meaning
            for i, n, v, v in cu.fetchall():
                raise errors.TroveMissing(n,v)
        return True

    # update the UserGroupAllTroves table
    def rebuild(self, cu, ugtId = None, userGroupId = None):
        where = []
        args = {}
        if ugtId is not None:
            where.append("ugtId = :ugtId")
            args["ugtId"] = ugtId
        if userGroupId is not None:
            where.append("userGroupId = :userGroupId")
            args["userGroupId"] = userGroupId
        whereCond = ""
        andCond = ""
        if where:
            whereCond = "where " + " and ".join(where)
            andCond   = "and "   + " and ".join(where)
        # update the UserGroupAllTroves table
        cu.execute("delete from UserGroupAllTroves %s" % (whereCond,), args)
        cu.execute("""
        insert into UserGroupAllTroves (ugtId, userGroupId, instanceId)
        select ugtId, userGroupId, instanceId from UserGroupTroves %s
        union
        select ugtId, userGroupId, TroveTroves.includedId
        from UserGroupTroves join TroveTroves using (instanceId)
        where UserGroupTroves.recursive = 1 %s
        """ %(whereCond, andCond), args)
        if ugtId is None and userGroupId is None:
            # this was a full rebuild
            self.db.analyze("UserGroupAllTroves")
        return True
    
    # grant access on a troveList to userGroup
    def add(self, userGroupId, troveList, recursive=True):
        """grant access on a troveList to a userGroup. If recursive = True,
        then access is also granted to all the children of the troves passed
        """
        self._findInstanceIds(troveList)
        recursive = int(bool(recursive))
        # we have now the list of instanceIds in the tmpInstanceId table.
        # avoid inserting duplicates
        cu = self.db.cursor()
        cu.execute("""
        select distinct tmpInstanceId.instanceId, ugt.ugtId, ugt.recursive
        from tmpInstanceId
        left join UserGroupTroves as ugt using(instanceId)
        where ugt.userGroupId is NULL or ugt.userGroupId = ?
        """, userGroupId)
        # record the new permissions
        ugtList = []
        for instanceId, ugtId, recflag in cu.fetchall():
            if ugtId is None: # new instanceId, left join returned a NULL ugt record
                cu.execute("insert into UserGroupTroves(userGroupId, instanceId, recursive) "
                           "values (?,?,?)", (userGroupId, instanceId, recursive))
                ugtId = cu.lastrowid
            elif recursive and not recflag:
                # granting recursive access to something that wasn't recursive before
                cu.execute("update UserGroupTroves set recursive = ? where ugtId = ?",
                           (recursive, ugtId))
            else: # not worth bothering with a rebuild
                ugtId = None
            if ugtId: # we have a new (or changed) acl
                self.rebuild(cu, ugtId, userGroupId)
                ugtList.append(ugtId)
        return ugtList

    # remove trove access grants
    def delete(self, userGroupId, troveList):
        """remove group access to troves passed in the (n,v,f) troveList"""
        self._findInstanceIds(troveList, checkMissing=False)
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpId")
        cu.execute("""
        insert into tmpId(id)
        select ugtId from UserGroupTroves
        where userGroupId = ?
        and instanceId in (select instanceId from tmpInstanceId)
        """, userGroupId, start_transaction=False)
        self.db.analyze("tmpId")
        # save what instanceIds will be affected by this delete
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct ugat.instanceId
        from UserGroupAllTroves as ugat
        where ugat.userGroupId = ?
          and ugat.ugtId in (select id from tmpId)
        """, userGroupId, start_transaction = False)
        cu.execute("delete from UserGroupAllTroves where ugtId in (select id from tmpId)")
        cu.execute("delete from UserGroupTroves where ugtId in (select id from tmpId)")
        # filter out the ones that are still allowed based on other permissions
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllTroves as ugat
            where userGroupId = ?
              and ugat.instanceId = tmpInstances.instanceId )
        """, userGroupId, start_transaction=False)
        return True

    # list what we have in the repository for a userGroupId
    def list(self, userGroupId):
        """return a list of the troves this usergroup is granted special access"""
        cu = self.db.cursor()
        cu.execute("""
        select Items.item, Versions.version, Flavors.flavor, ugt.recursive
        from UserGroupTroves as ugt
        join Instances using(instanceId)
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        where ugt.userGroupId = ? """, userGroupId)
        return [ ((n,v,f),r) for n,v,f,r in cu.fetchall()]

# class and methods for handling UserGroupAllPermissions operations
class UserGroupPermissions(UserGroupTable):
    # adds into the UserGroupAllPermissions table new entries
    # triggered by one or more recordIds
    def addId(self, cu, permissionId = None, userGroupId = None,
              instanceId = None):
        where = []
        args = []
        if permissionId is not None:
            where.append("Permissions.permissionId = ?")
            args.append(permissionId)
        if instanceId is not None:
            where.append("Instances.instanceId = ?")
            args.append(instanceId)
        if userGroupId is not None:
            where.append("Permissions.userGroupId = ?")
            args.append(userGroupId)
        whereStr = ""
        if len(where):
            whereStr = "where %s" % (' and '.join(where),)
        cu.execute("""
        insert into UserGroupAllPermissions
            (permissionId, userGroupId, instanceId, canWrite)
        select
            Permissions.permissionId as permissionId,
            Permissions.userGroupId as userGroupId,
            Instances.instanceId as instanceId,
            Permissions.canWrite as canWrite
        from Instances
        join Nodes using(itemId, versionId)
        join LabelMap using(itemId, branchId)
        join Permissions on
            Permissions.labelId = 0 or
            Permissions.labelId = LabelMap.labelId
        join CheckTroveCache on
            Permissions.itemId = CheckTroveCache.patternId and
            Instances.itemId = CheckTroveCache.itemId
        %s """ % (whereStr,), args)
        return True

    def deleteId(self, cu, permissionId = None, userGroupId = None,
                 instanceId = None):
        where, args = self.getWhereArgs("where", permissionId=permissionId,
            userGroupId=userGroupId, instanceId=instanceId)
        cu.execute("delete from UserGroupAllPermissions %s" % (where,), args)
        return True

    def rebuild(self, cu, permissionId = None, userGroupId = None, instanceId = None):
        self.deleteId(cu, permissionId, userGroupId, instanceId)
        self.addId(cu, permissionId, userGroupId, instanceId)
        if permissionId is None and userGroupId is None and instanceId is None:
            # this was a full rebuild
            self.db.analyze("UserGroupAllPermissions")
        return True

# this class takes care of the UserGroupInstancesCache table, which is a summary
# of rows present in UserGroupAllTroves and UserGroupAllPermissions tables
class UserGroupInstances(UserGroupTable):
    def __init__(self, db):
        UserGroupTable.__init__(self, db)
        self.ugt = UserGroupTroves(db)
        self.ugp = UserGroupPermissions(db)
        self.latest = versionops.LatestTable(db)

    def _getGroupId(self, userGroup):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
                   userGroup)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        raise errors.GroupNotFound

    def addTroveAccess(self, userGroup, troveList, recursive=True):
        userGroupId = self._getGroupId(userGroup)
        ugtList = self.ugt.add(userGroupId, troveList, recursive)
        # we now know the ids of the new acls added. They're useful in
        # updating the UGIC table
        cu = self.db.cursor()
        # grab the list of instanceIds we are adding to the UGIC table;
        # we need those for a faster recomputation of the LatestCache table
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct ugat.instanceId
        from UserGroupAllTroves as ugat
        where ugat.ugtId in (%s)
          and ugat.userGroupId = ?
          and not exists (
              select 1 from UserGroupInstancesCache as ugi
              where ugi.userGroupId = ?
                and ugi.instanceId = ugat.instanceId )
        """ % (",".join("%d" % x for x in ugtList),),
                   (userGroupId, userGroupId), start_transaction=False)
        # insert into UGIC and recompute the latest table
        cu.execute("""
        insert into UserGroupInstancesCache (userGroupId, instanceId)
        select %d, instanceId from tmpInstances """ %(userGroupId,))
        # tmpInstances has instanceIds for which Latest needs to be recomputed
        self.db.analyze("tmpInstances")
        self.latest.updateUserGroupId(cu, userGroupId, tmpInstances=True)

    def deleteTroveAccess(self, userGroup, troveList):
        userGroupId = self._getGroupId(userGroup)
        # remove the UserGroupTrove access
        cu = self.db.cursor()
        self.ugt.delete(userGroupId, troveList)
        # instanceIds that were removed from UGAT are in tmpInstances now
        # UGAP might still grant permissions to some, so we filter those out
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllPermissions as ugap
            where ugap.userGroupId = ?
              and ugap.instanceId = tmpInstances.instanceId )
        """, userGroupId, start_transaction = False)
        self.db.analyze("tmpInstances")
        # now we should have in tmpInstances the instanceIds of the
        # troves this user can no longer access.
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
          and instanceId in (select instanceId from tmpInstances)
        """, userGroupId)
        # tmpInstances has instanceIds for which Latest needs to be recomputed
        self.latest.updateUserGroupId(cu, userGroupId, tmpInstances=True)

    def listTroveAccess(self, userGroup):
        userGroupId = self._getGroupId(userGroup)
        return self.ugt.list(userGroupId)

    # changes in the Permissions table
    def addPermissionId(self, permissionId, userGroupId):
        cu = self.db.cursor()
        self.ugp.addId(cu, permissionId = permissionId)
        # figure out newly accessible troves. We keep track separately
        # to speed up the Latest update
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select instanceId from UserGroupAllPermissions as ugap
        where permissionId = ?
          and not exists (
              select instanceId from UserGroupInstancesCache as ugi
              where ugi.userGroupId = ?
              and ugi.instanceId = ugap.instanceId ) """,
                   (permissionId, userGroupId),
                   start_transaction = False)
        # update UsergroupInstancesCache
        cu.execute("""
        insert into UserGroupInstancesCache (userGroupId, instanceId, canWrite)
        select userGroupId, instanceId,
               case when sum(canWrite) = 0 then 0 else 1 end as canWrite
        from UserGroupAllPermissions
        where permissionId = ?
          and instanceId in (select instanceId from tmpInstances)
        group by userGroupId, instanceId
        """, permissionId)
        # update Latest
        self.latest.updateUserGroupId(cu, userGroupId, tmpInstances=True)

    def updatePermissionId(self, permissionId, userGroupId):
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpInstances")
        # figure out how the access is changing
        cu.execute("""
        insert into tmpInstances(instanceId)
        select instanceId from UserGroupAllPermissions
        where permissionId = ? """, permissionId, start_transaction=False)
        # re-add 
        self.ugp.deleteId(cu, permissionId = permissionId)
        self.ugp.addId(cu, permissionId = permissionId)
        # remove from consideration troves for which we still have access
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllPermissions as ugap
            where ugap.userGroupId = ?
              and ugap.instanceId = tmpInstances.instanceId )
        or exists (
            select 1 from UserGroupAllTroves as ugat
            where ugat.userGroupId = ?
              and ugat.instanceId = tmpInstances.instanceId )
        """, (userGroupId, userGroupId), start_transaction=False)
        self.db.analyze("tmpInstances")
        # remove trove access from troves that are left
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
          and instanceId in (select instanceId from tmpInstances)
          and not exists (
              select 1 from UserGroupAllTroves as ugat
              where ugat.userGroupId = UserGroupInstancesCache.userGroupId
                and ugat.instanceId = UserGroupInstancesCache.instanceId )
        """, userGroupId)
        # add the new troves now
        cu.execute("""
        insert into UserGroupInstancesCache(userGroupId, instanceId, canWrite)
        select userGroupId, instanceId,
               case when sum(canWrite) = 0 then 0 else 1 end as canWrite
        from UserGroupAllPermissions as ugap
        where ugap.permissionId = ?
          and not exists (
              select 1 from UserGroupInstancesCache as ugi
              where ugi.instanceId = ugap.instanceId
                and ugi.userGroupId = ugap.userGroupId )
        group by userGroupId, instanceId
        """, permissionId)
        self.latest.updateUserGroupId(cu, userGroupId)
        return True
    # updates the canWrite flag for an acl change
    def updateCanWrite(self, permissionId, userGroupId):
        cu = self.db.cursor()
        # update the flattened table first
        cu.execute("""
        update UserGroupAllPermissions set canWrite = (
            select canWrite from Permissions where permissionId = ? )
        where permissionId = ? """, (permissionId, permissionId))
        # update the UserGroupInstancesCache now. hopefully we won't
        # do too many of these...
        cu.execute("""
        update UserGroupInstancesCache set canWrite = (
            select case when sum(canWrite) = 0 then 0 else 1 end
            from UserGroupAllPermissions as ugap
            where ugap.userGroupId = UserGroupInstancesCache.userGroupId
              and ugap.instanceId = UserGroupInstancesCache.instanceId )
        where userGroupId = ? and instanceId in (
            select instanceId from UserGroupAllPermissions as ugap2
            where ugap2.permissionId = ? )
        """, (userGroupId, permissionId))
        return True
        
    def deletePermissionId(self, permissionId, userGroupId):
        cu = self.db.cursor()
        # compute the list of troves for which no other UGAP/UGAT access exists
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select ugi.instanceId from UserGroupInstancesCache as ugi
        where ugi.userGroupId = ?
          and not exists (
              select 1 from UserGroupAllPermissions as ugap
              where ugap.userGroupId = ? 
                and ugap.instanceId = ugi.instanceId
                and ugap.permissionId != ? )
          and not exists (
              select 1 from UserGroupAllTroves as ugat
              where ugat.instanceId = ugi.instanceId
                and ugat.userGroupId = ? )""",
                   (userGroupId, userGroupId, permissionId, userGroupId),
                   start_transaction = False)
        # clean up the flattened table
        cu.execute("delete from UserGroupAllPermissions where permissionId = ?",
                   permissionId)
        # now we have only the troves which need to be erased out of UGIC
        self.db.analyze("tmpInstances")
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
        and instanceId in (select instanceId from tmpInstances)""", userGroupId)
        # update Latest
        self.latest.updateUserGroupId(cu, userGroupId, tmpInstances=True)

    # a new trove has been comitted to the system
    def addInstanceId(self, instanceId):
        cu = self.db.cursor()
        self.ugp.addId(cu, instanceId = instanceId)
        cu.execute("""
        insert into UserGroupInstancesCache(userGroupId, instanceId, canWrite)
        select userGroupId, instanceId,
            case when sum(canWrite) = 0 then 0 else 1 end as canWrite
        from UserGroupAllPermissions as ugap
        where ugap.instanceId = ?
          and not exists (
              select 1 from UserGroupInstancesCache as ugi
              where ugi.instanceId = ugap.instanceId
                and ugi.userGroupId = ugap.userGroupId )
        group by userGroupId, instanceId
        """, instanceId)
        self.latest.updateInstanceId(cu, instanceId)

    # these used used primarily by the markRemoved code
    def deleteInstanceId(self, instanceId):
        cu = self.db.cursor()
        for t in [ "UserGroupInstancesCache", "UserGroupAllTroves",
                   "UserGroupAllPermissions"]:
            cu.execute("delete from %s where instanceId = ?" % (t,),
                       instanceId)
        self.latest.updateInstanceId(cu, instanceId)
    def deleteInstanceIds(self, idTableName):
        cu = self.db.cursor()
        for t in [ "UserGroupInstancesCache", "UserGroupAllTroves",
                   "UserGroupAllPermissions"]:
            cu.execute("delete from %s where instanceId in (select instanceId from %s)"%(
                t, idTableName))
        # this case usually does not require recomputing the
        # LatestCache since we only remove !present troves in bulk
        return True
    
    # rebuild the UGIC table entries
    def rebuild(self, userGroupId = None, cu = None):
        if cu is None:
            cu = self.db.cursor()
        where, args = self.getWhereArgs("where", userGroupId = userGroupId)
        cu.execute("delete from UserGroupInstancesCache %s" % (where,), args)
        # first, rebuild the flattened tables
        self.ugt.rebuild(cu, userGroupId = userGroupId)
        self.ugp.rebuild(cu, userGroupId = userGroupId)
        # and now sum it up. The union keeps the values distinct
        cu.execute("""
        insert into UserGroupInstancesCache(userGroupId, instanceId, canWrite)
        select userGroupId, instanceId, case when sum(canWrite) = 0 then 0 else 1 end
        from UserGroupAllPermissions %s
        group by userGroupId, instanceId
        """ % (where,), args)
        cond, args = self.getWhereArgs("and", userGroupId = userGroupId)
        cu.execute("""
        insert into UserGroupInstancesCache(userGroupId, instanceId, canWrite)
        select distinct userGroupId, instanceId, 0 as canWrite
        from UserGroupAllTroves as ugat
        where not exists (
            select 1 from UserGroupInstancesCache as ugi
            where ugat.instanceId = ugi.instanceId
              and ugat.userGroupId = ugi.userGroupId )
        %s """ % (cond,), args)
        self.db.analyze("UserGroupInstancesCache")
        # need to rebuild the latest as well
        if userGroupId is not None:
            self.latest.updateUserGroupId(cu, userGroupId)
        else: # this is a full rebuild
            self.latest.rebuild()
        return True


