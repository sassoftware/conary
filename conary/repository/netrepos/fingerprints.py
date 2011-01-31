#
# Copyright (c) 2010 rPath, Inc.
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

import base64

from conary import trove
from conary.lib import sha1helper
from conary.server import schema

def _troveFp(troveTup, sig, meta):
    if not sig and not meta:
        # we don't have sig or metadata info; just use the trove tuple
        # itself
        t = troveTup
    else:
        (sigPresent, sigBlock) = sig
        l = []
        if sigPresent >= 1:
            l.append(base64.decodestring(sigBlock))
        (metaPresent, metaBlock) = meta
        if metaPresent >= 1:
            l.append(base64.decodestring(metaBlock))
        if sigPresent or metaPresent:
            t = tuple(l)
        else:
            t = ("missing", ) + troveTup

    return sha1helper.sha1String("\0".join(t))

def expandJobList(db, chgSetList, recurse):
    """
    For each job in the list, find the set of troves which are recursively
    included in it. The reutnr value is list parallel to chgSetList, each
    item of which is a sorted list of those troves which are included in the
    recursive changeset.
    """
    # We mark old groups (ones without weak references) as uncachable
    # because they're expensive to flatten (and so old that it
    # hardly matters).

    if not recurse:
        return [ [ job ] for job in chgSetList ]

    cu = db.cursor()
    schema.resetTable(cu, "tmpNVF")

    foundGroups = set()
    foundWeak = set()
    foundCollections = set()

    insertList = []

    for jobId, job in enumerate(chgSetList):
        if trove.troveIsGroup(job[0]):
            foundGroups.add(jobId)

        insertList.append((jobId, job[0], job[2][0], job[2][1]))

    db.bulkload("tmpNvf", insertList,
                     [ "idx", "name", "version", "flavor" ],
                     start_transaction = False)

    db.analyze("tmpNVF")

    newJobList = [ [ job ] for job in chgSetList ]

    cu.execute("""SELECT
            tmpNVF.idx, I_Items.item, I_Versions.version,
            I_Flavors.flavor, TroveTroves.flags
        FROM tmpNVF JOIN Items ON tmpNVF.name = Items.item
        JOIN Versions ON (tmpNVF.version = Versions.version)
        JOIN Flavors ON (tmpNVF.flavor = Flavors.flavor)
        JOIN Instances ON
            Items.itemId = Instances.itemId AND
            Versions.versionId = Instances.versionId AND
            Flavors.flavorId = Instances.flavorId
        JOIN TroveTroves USING (instanceId)
        JOIN Instances AS I_Instances ON
            TroveTroves.includedId = I_Instances.instanceId
        JOIN Items AS I_Items ON
            I_Instances.itemId = I_Items.itemId
        JOIN Versions AS I_Versions ON
            I_Instances.versionId = I_Versions.versionId
        JOIN Flavors AS I_Flavors ON
            I_Instances.flavorId = I_Flavors.flavorId
        WHERE
            I_Instances.isPresent = 1
        ORDER BY
            I_Items.item, I_Versions.version, I_Flavors.flavor
    """)

    for (idx, name, version, flavor, flags) in cu:
        idx = abs(idx) - 1

        newJobList[idx].append( (name, (None, None),
                                       (version, flavor), True) )
        if flags & schema.TROVE_TROVES_WEAKREF > 0:
            foundWeak.add(idx)
        if trove.troveIsCollection(name):
            foundCollections.add(idx)

    for idx in ((foundGroups & foundCollections) - foundWeak):
        # groups which contain collections but no weak refs
        # are uncachable
        newJobList[idx] = None

    return newJobList
