#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import repository

def doCommit(repos, cfg, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)

    try:
	repos.commitChangeSet(cs)
    except repository.CommitError, e:
	print e
	
