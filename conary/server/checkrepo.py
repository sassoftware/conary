#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2004-2008 rPath, Inc.
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

import os
import sys

thisFile = sys.modules[__name__].__file__
thisPath = os.path.dirname(thisFile)
if thisPath:
    mainPath = thisPath + "/../.."
else:
    mainPath = "../.."
mainPath = os.path.realpath(mainPath)
sys.path.insert(0, mainPath)

from conary.lib import options
from conary.server import schema
from conary.local import schema as depSchema
from conary.lib.cfg import CfgBool, CfgInt, CfgPath
from conary.lib import log
from conary.repository.netrepos import netserver, instances, items
from conary import dbstore
from conary.dbstore import sqlerrors

class Checker:
    """ base class for checking/fixing an issue """
    def __init__(self, cfg, fix = False):
        self.cfg = cfg
        self._fix = fix
        self._db = None
        self._status = None
        self._alwaysfix = False
        self._postinit()
    def _postinit(self):
        pass
    
    def getDB(self):
        if self._db:
            return self._db
        (driver, database) = self.cfg.repositoryDB
        self._db = dbstore.connect(database, driver)
        schema.setupTempTables(self._db)
        depSchema.setupTempDepTables(self._db)
        return self._db
    
    def commit(self, db = None):
        if db is None:
            db = self._db
        assert(db)
        db.commit()

    def check(self):
        return True
    def repair(self):
        if not self._status:
            log.debug("no errors detected on check run, nothing to fix")
            return True
        return self.fix()
    def fix(self):
        return True
    
    def run(self):
        log.info("Running:  %s", self.__doc__)
        ret = self.check()
        if self._alwaysfix or (not ret and self._fix):
            ret = self.repair()
        log.info("%-7s: %s\n", ["FAIL", "Success"][int(bool(ret))], self.__doc__)
        return ret

class CheckAcls(Checker):
    """ acls and permission caches checks """
    def check(self):
        db = self.getDB()
        cu = db.cursor()
        log.info("checking existing Permissions cache")
        cu.execute("""
        select p.permissionId, p.userGroupId, ug.userGroup, i.item, l.label, coalesce(ugap.c,0)
        from Permissions as p
        join UserGroups as ug using (userGroupId)
        join Items as i on p.itemId = i.itemId
        join Labels as l on p.labelId = l.labelId
        left join (
            select permissionId, count(*) as c
            from UserGroupAllPermissions
            join Instances using(instanceId)
            where Instances.isPresent != ?
            group by permissionId ) as ugap on p.permissionId = ugap.permissionId
        """, instances.INSTANCE_PRESENT_MISSING)
        info = {}
        existing = {}
        for permissionId, roleId, role, item, label, count in cu:
            info[permissionId] = (roleId, role, item, label)
            existing[permissionId] = count
        log.info("checking for missing Permissions caches...")
        cu.execute("""
        select p.permissionId, coalesce(checker.c,0)
        from Permissions as p
        left join (
            select permissionId, count(*) as c from (
                select Permissions.permissionId as permissionId,
                       Instances.instanceId as instanceId
                from Instances
                join Nodes using(itemId, versionId)
                join LabelMap using(itemId, branchId)
                join Permissions on
                    Permissions.labelId = 0 or Permissions.labelId = LabelMap.labelId
                join CheckTroveCache on
                    Permissions.itemId = CheckTroveCache.patternId and
                    Instances.itemId = CheckTroveCache.itemId
                where Instances.isPresent != ?
                ) as perms
             group by permissionId ) as checker using (permissionId)
        """, instances.INSTANCE_PRESENT_MISSING)
        self._status = set()
        ret = True
        for permissionId, newCounter in cu:
            crtCounter = existing.get(permissionId, 0)
            if crtCounter == newCounter:
                continue
            roleId, role, item, label = info[permissionId]
            log.warning("acl(%d) (%s %s %s) caches %d entries instead of %d entries",
                        permissionId, role, label, item, crtCounter, newCounter)
            self._status.add((permissionId, roleId, role))
            ret = False
        if not ret:
            log.info("check fails with %d errors found", len(self._status))
        return ret

    def fix(self):
        from conary.repository.netrepos import accessmap
        db = self.getDB()
        ri = accessmap.RoleInstances(db)
        for (permissionId, roleId, role) in self._status:
            log.info("fixing permission cache for %s...", role)
            ri.updatePermissionId(permissionId, roleId)
        log.info("checking again to verify changes...")
        self._status = set()
        if not self.check():
            log.error("FAILED to fix the permissions cache. Unhandled error - contact rPath")
            db.rollback()
            return False
        self.commit()
        return True

class CheckLatest(Checker):
    """ LatestCache table rebuilding """
    def check(self):
        db = self.getDB()
        cu = db.cursor()
        # determine what entries (if any) are visible from LatestView
        # but aren't cached into LatestCache
        log.info("checking if the LatestCache table is current...")
        cu.execute("""
        select userGroupId, itemId, branchId, flavorId, versionId, latestType, count(*) as c
        from ( select userGroupId, itemId, branchId, flavorId, versionId, latestType from latestview
               union all
               select userGroupId, itemId, branchId, flavorId, versionId, latestType from latestcache
        ) as duplicates
        group by userGroupId, itemId, branchId, flavorId, versionId, latestType
        having count(*) != 2 """)
        # any entry that does not appear twice is cached wrong
        self._status = set()
        for userGroupId, itemId, branchId, flavorId, versionId, latestType, c in cu:
            # record what needs rebuilding
            self._status.add((itemId, branchId, flavorId))
        if self._status:
            log.info("detected %d LatestCache entries that need correction" % (
                len(self._status),))
            return False
        return True
    def fix(self):
        from conary.repository.netrepos import versionops
        db = self.getDB()
        cu = db.cursor()
        latest = versionops.LatestTable(db)
        log.info("updating LatestCache table")
        for itemId, branchId, flavorId in self._status:
            latest.update(cu, itemId, branchId, flavorId)
        log.info("update completed for LatestCache")
        self.commit()
        return True

    
class CheckTroveInfo(Checker):
    """ checks for extra/erroneous troveinfo records """
    def check(self):
        db = self.getDB()
        cu = db.cursor()
        log.info("checking for extraneous troveinfo records")
        cu.execute(""" select instanceId, count(*)
            from Instances join TroveInfo using(instanceId)
            where Instances.isPresent = ?
            group by instanceId having count(*) > 0 """, instances.INSTANCE_PRESENT_MISSING)
        self._status = cu.fetchall()
        if self._status:
            log.warning("found %d non-present troves with troveinfo records", len(self._status))
            return False
        return True
    def fix(self):
        db = self.getDB()
        cu = db.cursor()
        log.info("removing troveinfo records for non-prsent troves...")
        schema.resetTable(cu, "tmpId")
        cu.execute(""" insert into tmpId(id)
            select distinct instanceId from Instances join TroveInfo using(instanceId)
            where Instances.isPresent = ? """, instances.INSTANCE_PRESENT_MISSING)
        cu.execute("delete from TroveInfo where instanceId in (select id from tmpId)")
        self.commit()
        return self.check()

class CheckSchema(Checker):
    """ checks for schema version """
    def _postinit(self):
        self._alwaysfix = self._fix
    def check(self):
        db = self.getDB()
        dbVersion = db.getVersion()
        if dbVersion.major == schema.VERSION.major:
            log.info("schema is compatible with this codebase")
            return True
        log.error("codebase requires schema %s, repository has %s",
                  schema.VERSION, dbVersion)
        return False
    def fix(self):
        db = self.getDB()
        dbVersion = db.getVersion()
        try:
            log.info("performing a schema migration...")
            newVersion = schema.loadSchema(db, doMigrate=True)
        except sqlerrors.SchemaVersionError, e:
            log.error(e.msg)
            return False
        if newVersion < dbVersion():
            log.error("schema migration failed from %s to %s" % (
                dbVersion, schema.VERSION))
            return False
        if newVersion == dbVersion: # did a big whoop noop
            log.info("schema check complete")
        else:
            log.info("schema migration from %s to %s completed" %(
                dbVersion, newVersion))
        self.commit()
        return True

class CheckCTC(Checker):
    """ checks if the CheckTroveCache table is correctly built """
    def check(self):
        db = self.getDB()
        log.info("checking the state of the CheckTroveCache table")
        cu = db.cursor()
        cu.execute("select patternId, itemId from CheckTroveCache")
        existing = set([(x[0],x[1]) for x in cu.fetchall()])
        required = []
        cu.execute("select distinct i.itemId, i.item from Permissions as p "
                   "join Items as i using(itemId)")
        patterns = set([(x[0], x[1]) for x in cu.fetchall()])
        cu.execute("select itemId, item from Items")
        troveNames = set([(x[0], x[1]) for x in cu.fetchall()])
        for patternId, pattern in patterns:
            for itemId, item in troveNames:
                if items.checkTrove(pattern, item):
                    required.append((patternId, itemId))
        required = set(required)
        self._status = required.difference(existing)
        if len(self._status):
            log.warning("found %d entries that are missing from CheckTroveCache", len(self._status))
            return False
        return True
    def fix(self):
        db = self.getDB()
        cu = db.cursor()
        log.info("adding missing entries to CheckTroveCache")
        cu.executemany("insert into CheckTroveCache(patternId, itemId) values (?,?)",
                       ((p,i) for (p,i) in self._status))
        self.commit()
        return self.check()
    
# main program
class ServerConfig(netserver.ServerConfig):
    port                    = (CfgInt,  8000)
    sslCert                 = CfgPath
    sslKey                  = CfgPath
    useSSL                  = CfgBool
    def __init__(self, path="serverrc"):
	netserver.ServerConfig.__init__(self)
	self.read(path, exception=False)
        if self.tmpDir.endswith('/'):
            self.tmpDir = self.tmpDir[:-1]
    def check(self):
        if not self.contentsDir:
            log.error("contentsDir needs to be specified")
            return False
        if not self.tmpDir:
            log.error("tmpDir needs to be specified")
            return False
        if not os.path.isdir(self.tmpDir):
            log.error("%s needs to be a directory", self.tmpDir)
            return False
        if not os.access(self.tmpDir, os.R_OK | os.W_OK | os.X_OK):
            log.error("%s needs to allow full read/write access", self.tmpDir)
            return False
        if os.path.realpath(self.tmpDir) != self.tmpDir:
            log.error("tmpDir cannot include symbolic links")
            return False
        return True
    
def startLogging():
    import logging
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt = "%m-%d %H:%M")
    # tell the handler to use this format
    log.logger.handlers[0].setFormatter(formatter)
    log.setVerbosity(log.DEBUG)
    log.info("Logging system started")
    
def usage(name = sys.argv[0]):
    print """checks repository for data consistency
    Usage:
    %s [--fix] [--config-file repo.cnr] [--config 'name param'] checkname [checkname...]
    Valid check names are: ALL
        acls latest troveinfo schema ctc
    """ % (name,)

def getServer(opts = {}, argv = sys.argv, cfgMap = {}):
    cfg = ServerConfig()
    cfgMap.update({
        'contents-dir'  : 'contentsDir',
	'db'	        : 'repositoryDB',
	'tmp-dir'       : 'tmpDir',
        'server-name'   : 'serverName'
        })
    
    opts["config"] = options.MULT_PARAM
    opts["config-file"] = options.ONE_PARAM

    try:
        argSet, otherArgs = options.processArgs(opts, cfgMap, cfg, usage, argv = argv)
    except options.OptionError, msg:
        print >> sys.stderr, msg
        sys.exit(1)

    if "help" in argSet:
        usage(argv[0])
        sys.exit(0)
        
    startLogging()

    if not cfg.check():
        raise RuntimeError("configuration file is invalid")

    (driver, database) = cfg.repositoryDB
    db = dbstore.connect(database, driver)
    # if there is no schema or we're asked to migrate, loadSchema
    dbVersion = db.getVersion()
    # a more recent major is not compatible
    if dbVersion.major > schema.VERSION.major:
        log.error("code base too old for this repository database")
        log.error("repo=%s code=%s", dbVersion, schema.VERSION)
        sys.exit(-1)
    db.close()
    return (cfg, argSet, otherArgs[1:])

def main():
    opts =  {}
    opts["fix"] = options.NO_PARAM
    cfg, opts, args = getServer(opts)
    
    doFix = opts.has_key("fix")
    if not args:
        usage()
        sys.exit(-1)
    log.info("Starting tests\n")

    ret = {}
    all = False
    if "ALL" in args:
        all = True
    # XXX: fixme - we should probably do something smarter and more automatic here...
    if all or "schema" in args: # schema (migration) happens first
        ret["schema"] = CheckSchema(cfg, doFix).run()
    if all or "acls" in args:
        ret["acls"] = CheckAcls(cfg, doFix).run()
    if all or "latest" in args:
        ret["latest"] = CheckLatest(cfg, doFix).run()
    if all or "troveinfo" in args:
        ret["troveinfo"] = CheckTroveInfo(cfg, doFix).run()
    if all or "ctc" in args:
        ret["ctc"] = CheckCTC(cfg, doFix).run()
    if False in ret.values():
        return False
    return True

if __name__ == '__main__':
    ret = main()
    if not ret:
        sys.exit(1)
