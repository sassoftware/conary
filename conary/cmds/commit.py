#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
