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

class DepTable:
    def __init__(self, db):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if 'Dependencies' not in tables:
            cu.execute("""CREATE TABLE Dependencies(depId integer primary key,
                                                    class integer,
                                                    name str,
                                                    flag str
                                                    )""")
            cu.execute("""CREATE INDEX DependenciesIdx ON Dependencies(
                                                    class, name, flag)""")

class DepUser:
    def __init__(self, db, name):
        cu = db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if name not in tables:
            cu.execute("""CREATE TABLE %s(instanceId integer,
                                          depId integer
                                         )""" % name)
            cu.execute("CREATE INDEX %sIdx ON %s(instanceId)" % (name, name))
            cu.execute("CREATE INDEX %sIdx2 ON %s(depId)" % (name, name))

class DependencyTables:

    def _createTmpTable(self, cu, name):
        cu.execute("""CREATE TEMPORARY TABLE %s(
                                              troveId INT,
                                              depNum INT,
                                              flagCount INT,
                                              isProvides BOOL,
                                              class INTEGER,
                                              name STRING,
                                              flag STRING)""" % name)

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
                                        depName, flag))
                    else:
                        cu.execute(    "INSERT INTO %s VALUES(?, ?, ?, ?, "
                                                "?, ?, ?)" % name,
                                       (troveNum, len(depList), 1, 
                                        isProvides, classId, 
                                        depName, NO_FLAG_MAGIC))

                if not isProvides:
                    depList.append((troveNum, classId, dep))

    def _mergeTmpTable(self, cu, name):
        substDict = { 'name' : name }

        cu.execute("""INSERT INTO Dependencies 
                        SELECT DISTINCT
                            NULL,
                            %(name)s.class,
                            %(name)s.name,
                            %(name)s.flag
                        FROM %(name)s LEFT OUTER JOIN Dependencies ON
                            %(name)s.class == Dependencies.class AND
                            %(name)s.name == Dependencies.name AND
                            %(name)s.flag == Dependencies.flag
                        WHERE
                            Dependencies.depId is NULL
                    """ % substDict)

        cu.execute("""INSERT INTO Requires SELECT %(name)s.troveId, depId FROM
                        %(name)s JOIN Dependencies ON
                            %(name)s.class == Dependencies.class AND
                            %(name)s.name == Dependencies.name AND
                            %(name)s.flag == Dependencies.flag
                        WHERE
                            %(name)s.isProvides == 0""" % substDict)

        cu.execute("""INSERT INTO Provides SELECT %(name)s.troveId, depId FROM
                        %(name)s JOIN Dependencies ON
                            %(name)s.class == Dependencies.class AND
                            %(name)s.name == Dependencies.name AND
                            %(name)s.flag == Dependencies.flag
                        WHERE
                            %(name)s.isProvides == 1""" % substDict)

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
        self._createTmpTable(cu, "NeededDeps")
        self._populateTmpTable(cu, "NeededDeps", [], troveId, 
                               trove.getRequires(), trove.getProvides())
        self._mergeTmpTable(cu, "NeededDeps")

        cu.execute("DROP TABLE NeededDeps")

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

    def _resolve(self, cu):
        cu.execute("""
            SELECT depNum, instanceId FROM
                (SELECT COUNT(DepCheck.troveId) as matchCount,
                        DepCheck.flagCount as neededCount,
                        DepCheck.depNum as depNum,
                        Provides.instanceId as instanceId
                    FROM Requires LEFT OUTER JOIN Provides ON
                        Requires.depId == Provides.depId
                    JOIN Dependencies ON
                        Requires.depId == Dependencies.depId
                    JOIN DepCheck ON
                        Requires.instanceId == DepCheck.troveId AND
                        Dependencies.class == DepCheck.class AND
                        Dependencies.name == DepCheck.name AND
                        Dependencies.flag == DepCheck.flag
                    WHERE
                        Requires.instanceId < 0 AND Provides.depId is not NULL
                        AND NOT DepCheck.isProvides
                    GROUP BY
                        DepCheck.depNum,
                        Provides.instanceId
                 )
                WHERE 
                    matchCount == neededCount
                """)


    def check(self, changeSet):
        # XXX
        # this ignores file dependencies for now

        self.db._begin()
        cu = self.db.cursor()

        self._createTmpTable(cu, "DepCheck")
        depList = []
        failedSets = []
        for i, trvCs in enumerate(changeSet.iterNewPackageList()):
            failedSets.append((trvCs.getName(), None, None))
            self._populateTmpTable(cu, "DepCheck", depList, -i - 1, 
                                   trvCs.getRequires(), 
                                   trvCs.getProvides())

            cu.execute("INSERT INTO DepCheck VALUES(?, ?, ?, ?, ?, ?, ?)",
                       (-i - 1, 0, 1, True, deps.DEP_CLASS_TROVES, 
                        trvCs.getName(), NO_FLAG_MAGIC))

        if not failedSets:
            self.db.rollback()
            return (False, [])

        self._mergeTmpTable(cu, "DepCheck")
        self._resolve(cu)

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
        self.db.rollback()

        return (missingDeps, failedList)

    def __init__(self, db):
        self.db = db
        DepTable(db)
        DepUser(db, 'Provides')
        DepUser(db, 'Requires')
