#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#
import changeset
import copy
import files
import package
import string
import sys

def doCommit(repos, cfg, changeSetFile):
    cs = changeset.ChangeSetFromFile(changeSetFile)
    repos.commitChangeSet(cfg.sourcepath, cs)
