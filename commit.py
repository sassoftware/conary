#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
from repository import changeset
from repository import repository
import log
import versions

def doCommit(repos, changeSetFile, targetBranch):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    if targetBranch:
	if cs.isAbsolute():
	    # we can't do this -- where would we branch from?
	    log.error("absolute change sets cannot be retargeted")
	    return
	label = versions.BranchName(targetBranch)
	cs.setTargetBranch(repos, label)

    if cs.isLocal():
	log.error("local change sets cannot be applied to a repository "
		  "without a branch override")

    try:
	repos.commitChangeSet(cs)
    except repository.CommitError, e:
	print e
	
def doLocalCommit(db, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    if not cs.isLocal():
	log.error("repository changesets must be applied with update instead")
    db.commitChangeSet(cs, isRollback = True, toStash = False)
    

