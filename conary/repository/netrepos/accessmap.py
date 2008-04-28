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

# - Entries in the RoleTroves table are processed and flattened
#   into the RoleAllTroves table
# - Entries in the Permissions table are processed and flattened into
#   the RoleAllPermissions table
# - Then, RoleAllTroves and RoleAllPermissions are summarized
#   in the RoleInstancesCache table

# base class for handling RAP and GAT
class RoleTable:
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


# class and methods for handling RoleTroves operations
class RoleTroves(RoleTable):
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
            for i, n, v, f in cu.fetchall():
                raise errors.TroveMissing(n,v)
        return True

    # update the RoleAllTroves table
    def rebuild(self, cu = None, rtId = None, roleId = None):
        where = []
        args = {}
        if rtId is not None:
            where.append("ugtId = :rtId")
            args["rtId"] = rtId
        if roleId is not None:
            where.append("userGroupId = :roleId")
            args["roleId"] = roleId
        whereCond = ""
        andCond = ""
        if where:
            whereCond = "where " + " and ".join(where)
            andCond   = "and "   + " and ".join(where)
        if cu is None:
            cu = self.db.cursor()
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
        if rtId is None and roleId is None:
            # this was a full rebuild
            self.db.analyze("UserGroupAllTroves")
        return True
    
    # grant access on a troveList to role
    def add(self, roleId, troveList, recursive=True):
        """
        grant access on a troveList to a Role. If recursive = True,
        then access is also granted to all the children of the troves
        passed
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
        """)
        # record the new permissions
        rtList = []
        for instanceId, rtId, recflag in cu.fetchall():
            # new instanceId, left join returned a NULL rt record, or
            # another role has access to this instanceId
            if rtId != roleId:
                cu.execute("insert into UserGroupTroves(userGroupId, instanceId, recursive) "
                           "values (?,?,?)", (roleId, instanceId, recursive))
                rtId = cu.lastrowid
            elif recursive and not recflag:
                # granting recursive access to something that wasn't recursive before
                cu.execute("update UserGroupTroves set recursive = ? where rtId = ?",
                           (recursive, rtId))
            else: # not worth bothering with a rebuild
                rtId = None
            if rtId: # we have a new (or changed) acl
                self.rebuild(cu, rtId, roleId)
                rtList.append(rtId)
        return rtList

    # remove trove access grants
    def delete(self, roleId, troveList):
        """remove group access to troves passed in the (n,v,f) troveList"""
        self._findInstanceIds(troveList, checkMissing=False)
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpId")
        cu.execute("""
        insert into tmpId(id)
        select ugtId from UserGroupTroves
        where userGroupId = ?
        and instanceId in (select instanceId from tmpInstanceId)
        """, roleId, start_transaction=False)
        self.db.analyze("tmpId")
        # save what instanceIds will be affected by this delete
        schema.resetTable(cu, "tmpInstances")
        cu.execute("""
        insert into tmpInstances(instanceId)
        select distinct ugat.instanceId
        from UserGroupAllTroves as ugat
        where ugat.userGroupId = ?
          and ugat.ugtId in (select id from tmpId)
        """, roleId, start_transaction = False)
        cu.execute("delete from UserGroupAllTroves where ugtId in (select id from tmpId)")
        cu.execute("delete from UserGroupTroves where ugtId in (select id from tmpId)")
        # filter out the ones that are still allowed based on other permissions
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllTroves as ugat
            where userGroupId = ?
              and ugat.instanceId = tmpInstances.instanceId )
        """, roleId, start_transaction=False)
        return True

    # list what we have in the repository for a roleId
    def list(self, roleId):
        """return a list of the troves this usergroup is granted special access"""
        cu = self.db.cursor()
        cu.execute("""
        select Items.item, Versions.version, Flavors.flavor, ugt.recursive
        from UserGroupTroves as ugt
        join Instances using(instanceId)
        join Items on Instances.itemId = Items.itemId
        join Versions on Instances.versionId = Versions.versionId
        join Flavors on Instances.flavorId = Flavors.flavorId
        where ugt.userGroupId = ? """, roleId)
        return [ ((n,v,f),r) for n,v,f,r in cu.fetchall()]

# class and methods for handling RoleAllPermissions operations
class RolePermissions(RoleTable):
    # adds into the RoleAllPermissions table new entries
    # triggered by one or more recordIds
    def addId(self, cu = None, permissionId = None, roleId = None, instanceId = None):
        where = []
        args = []
        if permissionId is not None:
            where.append("Permissions.permissionId = ?")
            args.append(permissionId)
        if instanceId is not None:
            where.append("Instances.instanceId = ?")
            args.append(instanceId)
        if roleId is not None:
            where.append("Permissions.userGroupId = ?")
            args.append(roleId)
        whereStr = ""
        if len(where):
            whereStr = "where %s" % (' and '.join(where),)
        if cu is None:
            cu = self.db.cursor()
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

    def deleteId(self, cu = None, permissionId = None, roleId = None,
                 instanceId = None):
        where, args = self.getWhereArgs("where", permissionId=permissionId,
            userGroupId=roleId, instanceId=instanceId)
        if cu is None:
            cu = self.db.cursor()
        cu.execute("delete from UserGroupAllPermissions %s" % (where,), args)
        return True

    def rebuild(self, cu = None, permissionId = None, roleId = None,
                instanceId = None):
        if cu is None:
            cu = self.db.cursor()
        self.deleteId(cu, permissionId, roleId, instanceId)
        self.addId(cu, permissionId, roleId, instanceId)
        if permissionId is None and roleId is None and instanceId is None:
            # this was a full rebuild
            self.db.analyze("UserGroupAllPermissions")
        return True

# this class takes care of the RoleInstancesCache table, which is
# a summary of rows present in RoleAllTroves and RoleAllPermissions tables
class RoleInstances(RoleTable):
    def __init__(self, db):
        RoleTable.__init__(self, db)
        self.rt = RoleTroves(db)
        self.rp = RolePermissions(db)
        self.latest = versionops.LatestTable(db)

    def _getRoleId(self, role):
        cu = self.db.cursor()
        cu.execute("SELECT userGroupId FROM UserGroups WHERE userGroup=?",
                   role)
        ret = cu.fetchall()
        if len(ret):
            return ret[0][0]
        raise errors.RoleNotFound

    def addTroveAccess(self, role, troveList, recursive=True):
        roleId = self._getRoleId(role)
        rtList = self.rt.add(roleId, troveList, recursive)
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
        """ % (",".join("%d" % x for x in rtList),),
                   (roleId, roleId), start_transaction=False)
        # insert into UGIC and recompute the latest table
        cu.execute("""
        insert into UserGroupInstancesCache (userGroupId, instanceId)
        select %d, instanceId from tmpInstances """ %(roleId,))
        # tmpInstances has instanceIds for which Latest needs to be recomputed
        self.db.analyze("tmpInstances")
        self.latest.updateRoleId(cu, roleId, tmpInstances=True)

    def deleteTroveAccess(self, role, troveList):
        roleId = self._getRoleId(role)
        # remove the RoleTrove access
        cu = self.db.cursor()
        self.rt.delete(roleId, troveList)
        # instanceIds that were removed from RAT are in tmpInstances now
        # RAP might still grant permissions to some, so we filter those out
        cu.execute("""
        delete from tmpInstances
        where exists (
            select 1 from UserGroupAllPermissions as ugap
            where ugap.userGroupId = ?
              and ugap.instanceId = tmpInstances.instanceId )
        """, roleId, start_transaction = False)
        self.db.analyze("tmpInstances")
        # now we should have in tmpInstances the instanceIds of the
        # troves this user can no longer access.
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
          and instanceId in (select instanceId from tmpInstances)
        """, roleId)
        # tmpInstances has instanceIds for which Latest needs to be recomputed
        self.latest.updateRoleId(cu, roleId, tmpInstances=True)

    def listTroveAccess(self, role):
        roleId = self._getRoleId(role)
        return self.rt.list(roleId)

    # changes in the Permissions table
    def addPermissionId(self, permissionId, roleId):
        cu = self.db.cursor()
        self.rp.addId(cu, permissionId = permissionId)
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
                   (permissionId, roleId),
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
        self.latest.updateRoleId(cu, roleId, tmpInstances=True)

    def updatePermissionId(self, permissionId, roleId):
        cu = self.db.cursor()
        schema.resetTable(cu, "tmpInstances")
        # figure out how the access is changing
        cu.execute("""
        insert into tmpInstances(instanceId)
        select instanceId from UserGroupAllPermissions
        where permissionId = ? """, permissionId, start_transaction=False)
        # re-add 
        self.rp.deleteId(cu, permissionId = permissionId)
        self.rp.addId(cu, permissionId = permissionId)
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
        """, (roleId, roleId), start_transaction=False)
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
        """, roleId)
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
        self.latest.updateRoleId(cu, roleId)
        return True

    # updates the canWrite flag for an acl change
    def updateCanWrite(self, permissionId, roleId):
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
        """, (roleId, permissionId))
        return True

    def deletePermissionId(self, permissionId, roleId):
        cu = self.db.cursor()
        # compute the list of troves for which no other RAP/RAT access exists
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
                   (roleId, roleId, permissionId, roleId),
                   start_transaction = False)
        # clean up the flattened table
        cu.execute("delete from UserGroupAllPermissions where permissionId = ?",
                   permissionId)
        # now we have only the troves which need to be erased out of UGIC
        self.db.analyze("tmpInstances")
        cu.execute("""
        delete from UserGroupInstancesCache
        where userGroupId = ?
        and instanceId in (select instanceId from tmpInstances)""", roleId)
        # update Latest
        self.latest.updateRoleId(cu, roleId, tmpInstances=True)

    # a new trove has been comitted to the system
    def addInstanceId(self, instanceId):
        cu = self.db.cursor()
        self.rp.addId(cu, instanceId = instanceId)
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
    def rebuild(self, roleId = None, cu = None):
        if cu is None:
            cu = self.db.cursor()
        where, args = self.getWhereArgs("where", userGroupId = roleId)
        cu.execute("delete from UserGroupInstancesCache %s" % (where,), args)
        # first, rebuild the flattened tables
        logMe(3, "rebuilding UserGroupAllTroves", "roleId=%s" % roleId)
        self.rt.rebuild(cu, roleId = roleId)
        logMe(3, "rebuilding UserGroupAllPermissions", "roleId=%s" % roleId)
        self.rp.rebuild(cu, roleId = roleId)
        # and now sum it up
        logMe(3, "updating UserGroupInstancesCache from UserGroupAllPermissions")
        cu.execute("""
        insert into UserGroupInstancesCache(userGroupId, instanceId, canWrite)
        select userGroupId, instanceId, case when sum(canWrite) = 0 then 0 else 1 end
        from UserGroupAllPermissions %s
        group by userGroupId, instanceId
        """ % (where,), args)
        cond, args = self.getWhereArgs("and", userGroupId = roleId)
        logMe(3, "updating UserGroupInstancesCache from UserGroupAllTroves")
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
        logMe(3, "rebuilding the LatestCache rows", "roleId=%s"%roleId)
        if roleId is not None:
            self.latest.updateRoleId(cu, roleId)
        else: # this is a full rebuild
            self.latest.rebuild()
        return True


