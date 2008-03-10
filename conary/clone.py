#
# Copyright (c) 2005-2007 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
import itertools

from conary import callbacks
from conary import errors
from conary import versions
from conary import conaryclient
from conary.conaryclient import ConaryClient, cmdline
from conary.build.cook import signAbsoluteChangesetByConfig
from conary.conarycfg import selectSignatureKey
from conary.deps import deps

def displayCloneJob(cs):
    indent = '   '
    def _sortTroveNameKey(x):
        name = x.getName()
        return (not name.endswith(':source'), x.getNewFlavor(), name)
    csTroves = sorted(cs.iterNewTroveList(), key=_sortTroveNameKey)

    for csTrove in csTroves:
        newInfo = str(csTrove.getNewVersion())
        flavor = csTrove.getNewFlavor()
        if not flavor.isEmpty():
            newInfo += '[%s]' % flavor

        print "%sClone  %-20s (%s)" % (indent, csTrove.getName(), newInfo)

def CloneTrove(cfg, targetBranch, troveSpecList, updateBuildInfo = True,
               info = False, cloneSources = False, message = None, 
               test = False, fullRecurse = False, ignoreConflicts = False):
    client = ConaryClient(cfg)
    repos = client.getRepos()

    targetBranch = versions.VersionFromString(targetBranch)
    if not isinstance(targetBranch, versions.Branch):
        raise errors.ParseError('Cannot specify full version "%s" to clone to - must specify target branch' % targetBranch)

    troveSpecs = [ cmdline.parseTroveSpec(x) for x in troveSpecList]

    componentSpecs = [ x[0] for x in troveSpecs 
                       if ':' in x[0] and x[0].split(':')[1] != 'source']
    if componentSpecs:
        raise errors.ParseError('Cannot clone components: %s' % ', '.join(componentSpecs))


    trovesToClone = repos.findTroves(cfg.installLabelPath, 
                                    troveSpecs, cfg.flavor)
    trovesToClone = list(set(itertools.chain(*trovesToClone.itervalues())))

    if not client.cfg.quiet:
        callback = conaryclient.callbacks.CloneCallback(client.cfg, message)
    else:
        callback = callbacks.CloneCallback()

    okay, cs = client.createCloneChangeSet(targetBranch, trovesToClone,
                                           updateBuildInfo=updateBuildInfo,
                                           infoOnly=info, callback=callback,
                                           fullRecurse=fullRecurse,
                                           cloneSources=cloneSources)
    if not okay:
        return
    return _finishClone(client, cfg, cs, callback, info=info,
                        test=test, ignoreConflicts=ignoreConflicts)

def _convertLabelOrBranch(lblStr, template):
    try:
        if not lblStr:
            return None
        if lblStr[0] == '/':
            v = versions.VersionFromString(lblStr)
            if isinstance(v, versions.Branch):
                return v
            # Some day we could lift this restriction if its useful.
            raise errors.ParseError('Cannot specify version to promote'
                                    ' - must specify branch or label')


        hostName = template.getHost()
        nameSpace = template.getNamespace()
        tag = template.branch

        if lblStr[0] == ':':
            lblStr = '%s@%s%s' % (hostName, nameSpace, lblStr)
        elif lblStr[0] == '@':
            lblStr = '%s%s' % (hostName, lblStr)
        elif lblStr[-1] == '@':
            lblStr = '%s%s:%s' % (lblStr, nameSpace, tag)
        return versions.Label(lblStr)
    except Exception, msg:
        raise errors.ParseError('Error parsing %r: %s' % (lblStr, msg))

def promoteTroves(cfg, troveSpecs, targetList, skipBuildInfo=False,
                  info=False, message=None, test=False,
                  ignoreConflicts=False, cloneOnlyByDefaultTroves=False,
                  cloneSources = False, allFlavors = False, client=None, 
                  targetFile = None):
    targetMap = {}
    for fromLoc, toLoc in targetList:
        context = cfg.buildLabel
        fromLoc = _convertLabelOrBranch(fromLoc, context)
        if fromLoc is not None:
            if isinstance(fromLoc, versions.Branch):
                context = fromLoc.label()
            else:
                context = fromLoc
        toLoc = _convertLabelOrBranch(toLoc, context)
        targetMap[fromLoc] = toLoc

    troveSpecs = [ cmdline.parseTroveSpec(x, False) for x in troveSpecs ]

    if allFlavors:
        cfg.flavor = []
    client = ConaryClient(cfg)
    searchSource = client.getSearchSource()
    results = searchSource.findTroves(troveSpecs,
                                      bestFlavor=not allFlavors)
    if allFlavors:
        trovesToClone = []
        # we only clone the latest version for all troves.
        # bestFlavor=False resturns the leaves for all flavors, so 
        # we may need to cut some out.
        for troveSpec, troveTups in results.items():
            latest = max([x[1] for x in troveTups])
            troveTups = [ x for x in troveTups if x[1] == latest ]
            trovesToClone.extend(troveTups)
    else:
        trovesToClone = itertools.chain(*results.itervalues())
    trovesToClone = list(set(trovesToClone))

    if not client.cfg.quiet:
        callback = conaryclient.callbacks.CloneCallback(client.cfg, message)
    else:
        callback = callbacks.CloneCallback()

    okay, cs = client.createSiblingCloneChangeSet(
                           targetMap, trovesToClone,
                           updateBuildInfo=not skipBuildInfo,
                           infoOnly=info, callback=callback,
                           cloneOnlyByDefaultTroves=cloneOnlyByDefaultTroves,
                           cloneSources=cloneSources)
    if not okay:
        return False
    return _finishClone(client, cfg, cs, callback, info=info,
                        test=test, ignoreConflicts=ignoreConflicts,
                        targetFile=targetFile)

def _finishClone(client, cfg, cs, callback, info=False, test=False, 
                 ignoreConflicts=False, targetFile=None):
    repos = client.repos
    if cfg.interactive or info:
        print 'The following clones will be created:'
        displayCloneJob(cs)

    labelConflicts = client._checkChangeSetForLabelConflicts(cs)
    if labelConflicts and not ignoreConflicts:
        print
        print 'WARNING: performing this clone will create label conflicts:'
        for troveTups in labelConflicts:
            print
            print '%s=%s[%s]' % (troveTups[0])
            print '  conflicts with %s=%s[%s]' % (troveTups[1])

        if not cfg.interactive and not info:
            print
            print 'error: interactive mode is required for when creating label conflicts'
            return

    if info:
        return

    if cfg.interactive:
        print
        okay = cmdline.askYn('continue with clone? [y/N]', default=False)
        if not okay:
            return

    signAbsoluteChangesetByConfig(cs, cfg)

    if targetFile:
        cs.writeToFile(targetFile)
    elif not test:
        repos.commitChangeSet(cs, callback=callback)
    return cs


