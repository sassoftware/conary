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

class DependencyTables:

    def _createTmpTable(self, cu, name):
        cu.execute("""CREATE TEMPORARY TABLE %s(
                                              troveId int,
                                              isProvides bool,
                                              class integer,
                                              name string,
                                              flag string)""" % name)

    def _populateTmpTable(self, cu, name, troveNum, requires, provides):
        allDeps = []
        if requires:
            allDeps += [ (False, x) for x in 
                            requires.getDepClasses().iteritems() ]
        if provides:
            allDeps += [ (True,  x) for x in 
                            provides.getDepClasses().iteritems() ]

        for (isProvides, (classId, depClass)) in allDeps:
            for dep in depClass.getDeps():
                flags = dep.getFlags()
                if flags:
                    for flag in flags:
                        cu.execute("INSERT INTO %s VALUES(?, ?, ?, ?, ?)"
                                        % name,
                                   (troveNum, isProvides, classId, 
                                    dep.getName(), flag))
                else:
                    cu.execute("INSERT INTO %s VALUES(?, ?, ?, ?, '')"
                                        % name, 
                               (troveNum, isProvides, classId, dep.getName()))

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
                    if flag:
                        flags.append(flag)
                    
            if last:
                depSet.addDep(deps.dependencyClasses[last[0]],
                              deps.Dependency(last[1], flags))
                setFn(depSet)

    def add(self, cu, trove, troveId):
        self._createTmpTable(cu, "NeededDeps")
        self._populateTmpTable(cu, "NeededDeps", troveId, trove.getRequires(), 
                               trove.getProvides())
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
                (SELECT suspectDeps.depId FROM suspectDeps LEFT OUTER JOIN 
                    (SELECT depId AS depId1,
                            instanceId as instanceId1 FROM Requires UNION 
                     SELECT depId AS depId1,
                            instanceId as instanceId1 FROM Provides)
                    ON suspectDeps.depId = depId1
                 WHERE instanceId1 is NULL)""")

        cu.execute("DROP TABLE suspectDeps")

    def check(self, changeSet):
        # XXX
        # this ignores file dependencies for now

        self.db._begin()
        cu = self.db.cursor()

        self._createTmpTable(cu, "DepCheck")
        i = None
        for i, trvCs in enumerate(changeSet.iterNewPackageList()):
            self._populateTmpTable(cu, "DepCheck", -i - 1, trvCs.getRequires(), 
                                   trvCs.getProvides())
            cu.execute("INSERT INTO DepCheck VALUES(?, 1, ?, ?, '')",
                       (i, deps.DEP_CLASS_TROVES, trvCs.getName()))
        if i is None:
            self.db.rollback()
            return (False, [])

        failedSets = [ None ] * (i + 1)

        self._mergeTmpTable(cu, "DepCheck")

        cu.execute("""SELECT Requires.instanceId,
                             Dependencies.class,
                             Dependencies.name,
                             Dependencies.flag
                FROM Requires LEFT OUTER JOIN Provides ON
                    Requires.depId == Provides.depId
                JOIN Dependencies ON
                    Requires.depId == Dependencies.depId
                WHERE
                    Provides.depId is NULL AND Requires.instanceId < 0
                ORDER BY 
                    Requires.instanceId DESC,
                    Dependencies.class ASC,
                    Dependencies.name ASC""")

        last = None
        for (instanceId, classId, name, flag) in cu:
            if classId == deps.DEP_CLASS_ABI or \
               classId == deps.DEP_CLASS_FILES:
                continue

            instanceId = -instanceId

            flags = []
            depSet = deps.DependencySet()

            if (instanceId, classId, name) == last:
                flags.append(flag)
            else:
                if last:
                    lastIdx = last[0] - 1
                    if failedSets[lastIdx] is None:
                        failedSets[lastIdx] = deps.DependencySet()

                    failedSets[lastIdx].addDep(deps.dependencyClasses[last[1]],
                                  deps.Dependency(last[2], flags))
                    
                last = (instanceId, classId, name)
                flags = []
                if flag:
                    flags.append(flag)
                    
            if last:
                lastIdx = last[0] - 1

                if failedSets[lastIdx] is None:
                    failedSets[lastIdx] = deps.DependencySet()

                failedSets[lastIdx].addDep(deps.dependencyClasses[last[1]],
                              deps.Dependency(last[2], flags))

        missingDeps = False
        failedList = []
        for i, trvCs in enumerate(changeSet.iterNewPackageList()):
            if failedSets[i] is not None:
                missingDeps = True
                failedList.append((trvCs.getName(), failedSets[i]))

        # no need to drop the DepCheck table since we're rolling this whole
        # transaction back anyway
        self.db.rollback()

        return (missingDeps, failedList)

    def __init__(self, db):
        self.db = db
        DepTable(db)
        DepUser(db, 'Provides')
        DepUser(db, 'Requires')
