#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import repository

def doCommit(repos, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
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
    db.commitChangeSet(cs, isRollback = True, toDatabase = False)
    

