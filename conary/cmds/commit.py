#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


import os
import tempfile

from conary import callbacks
from conary import conaryclient
from conary import versions
from conary.cmds import updatecmd
from conary.lib import log
from conary.local import database
from conary.repository import changeset
from conary.repository import errors
from conary.repository import filecontainer

class CheckinCallback(updatecmd.UpdateCallback, callbacks.ChangesetCallback):
    def __init__(self, cfg=None):
        updatecmd.UpdateCallback.__init__(self, cfg)
        callbacks.ChangesetCallback.__init__(self)

    def missingFiles(self, missingFiles):
        print "Warning: The following files are missing:"
        for mp in missingFiles:
            print mp[4]
        return True

def doCommit(cfg, changeSetFile, targetLabel):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()
    callback = CheckinCallback()

    try:
        cs = changeset.ChangeSetFromFile(changeSetFile)
    except filecontainer.BadContainer:
        log.error("invalid changeset %s", changeSetFile)
        return 1

    if cs.isLocal():
        if not targetLabel:
            log.error("committing local changesets requires a targetLabel")
        label = versions.Label(targetLabel)
        cs.setTargetShadow(repos, label)
        commitCs = cs.makeAbsolute(repos)

        (fd, changeSetFile) = tempfile.mkstemp()

        os.close(fd)
        commitCs.writeToFile(changeSetFile)

    try:
        # hopefully the file hasn't changed underneath us since we
        # did the check at the top of doCommit().  We should probably
        # add commitChangeSet method that takes a fd.
        try:
            repos.commitChangeSetFile(changeSetFile, callback=callback)
        except errors.CommitError, e:
            print e
    finally:
        if targetLabel:
            os.unlink(changeSetFile)

def doLocalCommit(db, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    if not cs.isLocal():
        log.error("repository changesets must be applied with update instead")
    else:
        db.commitChangeSet(cs, database.UpdateJob(db),
                           rollbackPhase = db.ROLLBACK_PHASE_LOCAL,
                           updateDatabase = False)
