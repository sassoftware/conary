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
from repository import changeset
from repository import repository
from repository import filecontainer
from lib import log
import os
import tempfile
import versions

def doCommit(repos, changeSetFile, targetBranch):
    try:
	cs = changeset.ChangeSetFromFile(changeSetFile)
    except filecontainer.BadContainer:
	log.error("invalid changeset %s", changeSetFile)
	return 1

    if targetBranch:
	if cs.isAbsolute():
	    # we can't do this -- where would we branch from?
	    log.error("absolute change sets cannot be retargeted")
	    return 1
	label = versions.Label(targetBranch)
	cs.setTargetBranch(repos, label)

        (fd, changeSetFile) = tempfile.mkstemp()
        os.close(fd)
        cs.writeToFile(changeSetFile)

    if cs.isLocal():
	log.error("commits of local change sets require branch overrides")
        return 1

    try:
        # hopefully the file hasn't changed underneath us since we
        # did the check at the top of doCommit().  We should probably
        # add commitChangeSet method that takes a fd.
        try:
            repos.commitChangeSetFile(changeSetFile)
        except repository.CommitError, e:
            print e
    finally:
        if targetBranch:
            os.unlink(changeSetFile)
	
def doLocalCommit(db, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    if not cs.isLocal():
	log.error("repository changesets must be applied with update instead")
    db.commitChangeSet(cs, isRollback = True, toStash = False)
    

