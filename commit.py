#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import log
import repository
import versions

def doCommit(repos, changeSetFile, targetBranch):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    if targetBranch:
	if cs.isAbstract():
	    # we can't do this -- where would we branch from?
	    log.error("abstract change sets cannot be retargeted")
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
    

