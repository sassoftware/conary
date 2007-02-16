#!/usr/bin/python
#
# saved for reference

# these are yet unused...
def createViews(db):
    commit = False
    cu = db.cursor()
    if "UsersView" not in db.views:
        cu.execute("""
        CREATE VIEW
            UsersView AS
        SELECT
            Users.userName as userName,
            Items.item as item,
            Labels.label as label,
            Permissions.canWrite as W,
            Permissions.admin as A,
            Permissions.capId as C
        FROM
            Users
        JOIN UserGroupMembers using (userId)
        JOIN Permissions using (userGroupId)
        JOIN Items using (itemId)
        JOIN Labels on Permissions.labelId = Labels.labelId
        """)
        commit = True
    if "InstancesView" not in db.views:
        cu.execute("""
        CREATE VIEW
            InstancesView AS
        SELECT
            Instances.instanceId as instanceId,
            Items.item as item,
            Versions.version as version,
            Flavors.flavor as flavor
        FROM
            Instances
        JOIN Items on Instances.itemId = Items.itemId
        JOIN Versions on Instances.versionId = Versions.versionId
        JOIN Flavors on Instances.flavorId = Flavors.flavorId
        """)
        commit = True
    if 'NodesView' not in db.views:
        cu.execute("""
        CREATE VIEW
            NodesView AS
        SELECT
            Nodes.nodeId as nodeId,
            Items.item as item,
            Branches.branch as branch,
            Versions.version as version,
            Nodes.timestamps as timestamps,
            Nodes.finalTimestamp as finalTimestamp
        FROM
            Nodes
        JOIN Items on Nodes.itemId = Items.itemId
        JOIN Branches on Nodes.branchId = Branches.branchId
        JOIN Versions on Nodes.versionId = Versions.versionId
        """)
        commit = True
    if 'LatestView' not in db.views:
        cu.execute("""
        CREATE VIEW
            LatestView AS
        SELECT
            Items.item as item,
            Branches.branch as branch,
            Versions.version as version,
            Flavors.flavor as flavor
        FROM
            Latest
        JOIN Items on Latest.itemId = Items.itemId
        JOIN Branches on Latest.branchId = Branches.branchId
        JOIN Versions on Latest.versionId = Versions.versionId
        JOIN Flavors on Latest.flavorId = Flavors.flavorId
        """)
        commit = True
    if commit:
        db.commit()
        db.loadSchema()
