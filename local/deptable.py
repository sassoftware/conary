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

from deps import deps

NO_FLAG_MAGIC = '-*none*-'

def createDepTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""

    cu.execute("""CREATE %s TABLE %s(depId integer primary key,
                                  class integer,
                                  name str,
                                  flag str
                                 )""" % (tmp, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx ON %s(class, name, flag)" % 
               (name, name), start_transaction = (not tmp))

def createDepUserTable(cu, name, isTemp):
    if isTemp:
        tmp = "TEMPORARY"
    else:
        tmp = ""

    cu.execute("""CREATE %s TABLE %s(instanceId integer,
                                  depId integer
                                 )""" % (tmp, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx ON %s(instanceId)" % (name, name),
               start_transaction = (not isTemp))
    cu.execute("CREATE INDEX %sIdx2 ON %s(depId)" % (name, name),
               start_transaction = (not isTemp))

class DepTable:
    def __init__(self, db, name):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Dependencies' not in tables:
            createDepTable(cu, name, False)

class DepUser:
    def __init__(self, db, name):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if name not in tables:
            createDepUserTable(cu, name, False)

class DependencyTables:

    def _createTmpTable(self, cu, name):
        cu.execute("""CREATE TEMPORARY TABLE %s(
                                              troveId INT,
                                              depNum INT,
                                              flagCount INT,
                                              isProvides BOOL,
                                              class INTEGER,
                                              name STRING,
                                              flag STRING)""" % name,
                   start_transaction = False)

    def _populateTmpTable(self, cu, name, depList, troveNum, requires, 
                          provides):
        allDeps = []
        if requires:
            allDeps += [ (False, x) for x in 
                            requires.getDepClasses().iteritems() ]
        if provides:
            allDeps += [ (True,  x) for x in 
                            provides.getDepClasses().iteritems() ]

        for (isProvides, (classId, depClass)) in allDeps:
            for dep in depClass.getDeps():
                for (depName, flags) in zip(dep.getName(), dep.getFlags()):
                    if flags:
                        for flag in flags:
                            cu.execute("INSERT INTO %s VALUES(?, ?, ?, ?, "
                                                "?, ?, ?)" % name,
                                       (troveNum, len(depList), len(flags), 
                                        isProvides, classId, 
                                        depName, flag),
                                       start_transaction = False)
                    else:
                        cu.execute(    "INSERT INTO %s VALUES(?, ?, ?, ?, "
                                                "?, ?, ?)" % name,
                                       (troveNum, len(depList), 1, 
                                        isProvides, classId, 
                                        depName, NO_FLAG_MAGIC),
                                       start_transaction = False)

                if not isProvides:
                    depList.append((troveNum, classId, dep))

    def _mergeTmpTable(self, cu, tmpName, depTable, reqTable, provTable,
                       allDeps):
        substDict = { 'tmpName'   : tmpName,
                      'depTable'  : depTable,
                      'reqTable'  : reqTable,
                      'provTable' : provTable,
                      'allDeps'   : allDeps   }

        cu.execute("""INSERT INTO %(depTable)s 
                        SELECT DISTINCT
                            NULL,
                            %(tmpName)s.class,
                            %(tmpName)s.name,
                            %(tmpName)s.flag
                        FROM %(tmpName)s LEFT OUTER JOIN Dependencies ON
                            %(tmpName)s.class == Dependencies.class AND
                            %(tmpName)s.name == Dependencies.name AND
                            %(tmpName)s.flag == Dependencies.flag
                        WHERE
                            Dependencies.depId is NULL
                    """ % substDict, start_transaction = False)

        cu.execute("""INSERT INTO %(reqTable)s 
                    SELECT %(tmpName)s.troveId, depId FROM
                        %(tmpName)s JOIN %(allDeps)s ON
                            %(tmpName)s.class == %(allDeps)s.class AND
                            %(tmpName)s.name == %(allDeps)s.name AND
                            %(tmpName)s.flag == %(allDeps)s.flag
                        WHERE
                            %(tmpName)s.isProvides == 0""" % substDict,
                   start_transaction = False)

        if provTable is None:   
            return

        cu.execute("""INSERT INTO %(provTable)s SELECT 
                            %(tmpName)s.troveId, depId FROM
                        %(tmpName)s JOIN %(allDeps)s ON
                            %(tmpName)s.class == %(allDeps)s.class AND
                            %(tmpName)s.name == %(allDeps)s.name AND
                            %(tmpName)s.flag == %(allDeps)s.flag
                        WHERE
                            %(tmpName)s.isProvides == 1""" % substDict,
                   start_transaction = False)

    def get(self, cu, trv, troveId):
        for (tblName, setFn) in (('Requires', trv.setRequires),
                                 ('Provides', trv.setProvides)):
            cu.execute("SELECT class, name, flag FROM %s NATURAL JOIN "
                       "Dependencies WHERE instanceId=? ORDER BY class, name"
                    % tblName, troveId)

            last = None
            flags = []
            depSet = deps.DependencySet()
            for (classId, name, flag) in cu:
                if (classId, name) == last:
                    flags.append(flag)
                else:
                    if last:
                        depSet.addDep(deps.dependencyClasses[last[0]],
                                      deps.Dependency(last[1], flags))
                        
                    last = (classId, name)
                    flags = []
                    if flag != NO_FLAG_MAGIC:
                        flags.append(flag)
                    
            if last:
                depSet.addDep(deps.dependencyClasses[last[0]],
                              deps.Dependency(last[1], flags))
                setFn(depSet)

    def add(self, cu, trove, troveId):
        assert(cu.con.inTransaction)
        self._createTmpTable(cu, "NeededDeps")
        self._populateTmpTable(cu, "NeededDeps", [], troveId, 
                               trove.getRequires(), trove.getProvides())
        self._mergeTmpTable(cu, "NeededDeps", "Dependencies", "Requires", 
                            "Provides", "Dependencies")

        cu.execute("DROP TABLE NeededDeps", start_transaction = False)

    def delete(self, cu, troveId):
        cu.execute("CREATE TEMPORARY TABLE suspectDepsOrig(depId integer)")
        for tbl in ('Requires', 'Provides'):
            cu.execute("INSERT INTO suspectDepsOrig SELECT depId "
                       "FROM %s WHERE instanceId=%d" % (tbl, troveId))
            cu.execute("DELETE FROM %s WHERE instanceId=%d" % (tbl, troveId))

        cu.execute("CREATE TEMPORARY TABLE suspectDeps(depId integer)")
        cu.execute("INSERT INTO suspectDeps SELECT DISTINCT depId "
                   "FROM suspectDepsOrig")
        cu.execute("DROP TABLE suspectDepsOrig")

        cu.execute("""DELETE FROM Dependencies WHERE depId IN 
                (SELECT DISTINCT suspectDeps.depId FROM suspectDeps 
                 LEFT OUTER JOIN 
                    (SELECT depId AS depId1,
                            instanceId AS instanceId1 FROM Requires UNION 
                     SELECT depId AS depId1,
                            instanceId AS instanceId1 FROM Provides)
                    ON suspectDeps.depId = depId1
                 WHERE instanceId1 IS NULL)""")

        cu.execute("DROP TABLE suspectDeps")

    def _resolveStmt(self, providesTable = "Provides", 
                     depTable = "Dependencies",
                     requiresTable = "Requires"):
        substTable = { 'provides' : providesTable,
                       'requires' : requiresTable,
                       'deptable' : depTable }

        return """
                SELECT depCheck.depNum as depNum,
                        %(provides)s.instanceId as rsvInstanceId
                    FROM %(requires)s LEFT OUTER JOIN %(provides)s ON
                        %(requires)s.depId == %(provides)s.depId
                    JOIN %(deptable)s ON
                        %(requires)s.depId == %(deptable)s.depId
                    JOIN DepCheck ON
                        %(requires)s.instanceId == DepCheck.troveId AND
                        %(deptable)s.class == DepCheck.class AND
                        %(deptable)s.name == DepCheck.name AND
                        %(deptable)s.flag == DepCheck.flag
                    WHERE
                        %(requires)s.instanceId < 0 
                        AND %(provides)s.depId is not NULL
                        AND NOT DepCheck.isProvides
                    GROUP BY
                        DepCheck.depNum,
                        %(provides)s.instanceId
                    HAVING
                        COUNT(DepCheck.troveId) == DepCheck.flagCount
                """ % substTable

    def check(self, changeSet):
        cu = self.db.cursor()

        self._createTmpTable(cu, "DepCheck")
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        cu.execute("CREATE TEMPORARY VIEW AllDeps AS SELECT * FROM "
                   "Dependencies UNION SELECT * FROM TmpDependencies",
                   start_transaction = False)
        createDepUserTable(cu, 'TmpProvides', isTemp = True)
        createDepUserTable(cu, 'TmpRequires', isTemp = True)
        cu.execute("""CREATE TEMPORARY VIEW AllProvides AS
                        SELECT * FROM Provides UNION
                        SELECT * FROM TmpProvides""",
                   start_transaction = False)

        depList = []
        failedSets = []
        for i, trvCs in enumerate(changeSet.iterNewPackageList()):
            failedSets.append((trvCs.getName(), None, None))
            self._populateTmpTable(cu, "DepCheck", depList, -i - 1, 
                                   trvCs.getRequires(), 
                                   trvCs.getProvides())

            cu.execute("INSERT INTO DepCheck VALUES(?, ?, ?, ?, ?, ?, ?)",
                       (-i - 1, 0, 1, True, deps.DEP_CLASS_TROVES, 
                        trvCs.getName(), NO_FLAG_MAGIC), 
                       start_transaction = False)

        if not failedSets:
            self.db.rollback()
            return (False, [])

        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            "TmpProvides", "AllDeps")
        cu.execute(self._resolveStmt(depTable = "AllDeps",
                                     requiresTable = "TmpRequires",
                                     providesTable = "AllProvides"), 
                   start_transaction = False)

        for (depNum, instanceId) in cu:
            depList[depNum] = None

        missingDeps = False
        for depInfo in depList:
            if depInfo is not None:
                (troveIndex, classId, dep) = depInfo

                if classId in [ deps.DEP_CLASS_ABI, deps.DEP_CLASS_FILES ]:
                    continue

                missingDeps = True
                troveIndex = -(troveIndex + 1)

                if failedSets[troveIndex][2] is None:
                    failedSets[troveIndex] = (failedSets[troveIndex][0],
                                              failedSets[troveIndex][1],
                                              deps.DependencySet())
                failedSets[troveIndex][2].addDep(
                                deps.dependencyClasses[classId], dep)

        failedList = []
        for (name, classId, depSet) in failedSets:
            if depSet is not None:
                failedList.append((name, depSet))

        # no need to drop the DepCheck table since we're rolling this whole
        # transaction back anyway
        cu.execute("DROP VIEW AllDeps", start_transaction = False)
        cu.execute("DROP TABLE TmpDependencies", start_transaction= False)
        cu.execute("DROP TABLE TmpRequires", start_transaction= False)
        cu.execute("DROP TABLE TmpProvides", start_transaction= False)
        cu.execute("DROP VIEW AllProvides", start_transaction= False)
        cu.execute("DROP TABLE DepCheck", start_transaction = False)

        assert(not self.db.inTransaction)

        return (missingDeps, failedList)

    def resolve(self, label, depSetList):
        cu = self.db.cursor()

        self._createTmpTable(cu, "DepCheck")
        createDepTable(cu, 'TmpDependencies', isTemp = True)
        cu.execute("CREATE TEMPORARY VIEW AllDeps AS SELECT * FROM "
                   "Dependencies UNION SELECT * FROM TmpDependencies",
                   start_transaction = False)
        createDepUserTable(cu, 'TmpRequires', isTemp = True)

        cu.execute("""
                CREATE TEMPORARY VIEW providesBranch AS 
                    SELECT provides.depId AS depId,
                           provides.instanceId AS instanceId FROM 
                    LabelMap JOIN Nodes ON
                        LabelMap.itemId == Nodes.itemId AND
                        LabelMap.branchId == Nodes.branchId
                    JOIN Instances ON
                        Instances.itemId == Nodes.itemId AND
                        Instances.versionId == Nodes.versionId
                    JOIN Provides ON
                        Provides.instanceId == Instances.instanceId 
                    WHERE
                        LabelMap.labelId == 
                            (SELECT labelId FROM Labels WHERE Label='%s')
        """ % label.asString(), start_transaction = False)

        depList = []
        for i, depSet in enumerate(depSetList):
            self._populateTmpTable(cu, "DepCheck", depList, -i - 1, 
                                   depSet, None)


        self._mergeTmpTable(cu, "DepCheck", "TmpDependencies", "TmpRequires",
                            None, "AllDeps")

        cu.execute("""SELECT depNum, Items.item, Versions.version FROM 
                        (%s)
                      JOIN Instances ON
                        rsvInstanceId == Instances.instanceId
                      JOIN Items ON
                        Instances.itemId == Items.itemId
                      JOIN Versions ON
                        Instances.versionId == Versions.versionId
                      JOIN Nodes ON
                        Instances.itemId == Nodes.itemId AND
                        Instances.versionId == Nodes.versionId
                      ORDER BY
                        Nodes.finalTimestamp DESC
                    """ % self._resolveStmt(providesTable = "providesBranch",
                                            requiresTable = "TmpRequires",
                                            depTable = "AllDeps"),
                    start_transaction = False)
        result = {}
        handled = {}
        for (depId, troveName, versionStr) in cu:
            if handled.has_key(depId):
                continue

            handled[depId] = True
            
            depNum = depList[depId][0]
            depSet = depSetList[depNum]
            l = result.get(depSet, None)
            if not l:
                result[depSet] = [ (troveName, versionStr) ]
            elif (troveName, versionStr) not in l:
                l.append((troveName, versionStr))

        cu.execute("DROP VIEW AllDeps", start_transaction = False)
        cu.execute("DROP TABLE TmpDependencies", start_transaction= False)
        cu.execute("DROP TABLE TmpRequires", start_transaction= False)
        cu.execute("DROP TABLE DepCheck", start_transaction = False)
        cu.execute("DROP VIEW providesBranch", start_transaction = False)

        assert(not self.db.inTransaction)

        return result

    def __init__(self, db):
        self.db = db
        DepTable(db, "Dependencies")
        DepUser(db, 'Provides')
        DepUser(db, 'Requires')
