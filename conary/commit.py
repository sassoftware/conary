#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import os
import tempfile

from conary import conaryclient
from conary import versions
from conary.lib import log
from conary.repository import changeset
from conary.repository import errors
from conary.repository import filecontainer

def doCommit(cfg, changeSetFile, targetLabel):
    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

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
            repos.commitChangeSetFile(changeSetFile)
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
        db.commitChangeSet(cs, set(), isRollback = True, updateDatabase = False)


