#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset

def doCommit(repos, cfg, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    repos.commitChangeSet(cs)
