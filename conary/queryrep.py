#
# Copyright (c) 2004-2005 rPath, Inc.
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
"""
Provides the output for the "conary repquery" command
"""
import itertools
import sys
import time

from conary import conaryclient
from conary.conaryclient import cmdline
from conary import display
from conary.deps import deps
from conary.lib import log
from conary import versions


def displayTroves(cfg, troveSpecs = [], all = False, ls = False, 
                  ids = False, sha1s = False, leaves = False, 
                  info = False, tags = False, deps = False,
                  showBuildReqs = False, digSigs = False):
    """
       Displays information about troves found in repositories

       @param repos: a network repository client 
       @type repos: repository.netclient.NetworkRepositoryClient
       @param cfg: conary config
       @type cfg: conarycfg.ConaryConfiguration
       @param troveSpecs: troves to search for
       @type troveSpecs: list of troveSpecs (n[=v][[f]])
       @param all: If true, find all versions of the specified troves, 
                   not just the leaves
       @type all: bool
       @param leaves: If true, find all leaves of the specified troves,
                      regardless of whether they match the cfg flavor 
       @type leaves: bool
       @param ls: If true, list files in the trove
       @type ls: bool
       @param ids: If true, list pathIds for files in the troves
       @type ids: bool
       @param sha1s: If true, list sha1s for files in the troves
       @type sha1s: bool
       @param tags: If true, list tags for files in the troves
       @type tags: bool
       @param info: If true, display general information about the trove
       @type info: bool
       @param deps: If true, display provides and requires information for the
                    trove.
       @type deps: bool
       @param showDiff: If true, display the difference between the local and
                        pristine versions of the trove
       @type showDiff: bool
       @param digSigs: If true, list digital signatures for the troves
       @type digSigs: bool
       @rtype: None
    """

    client = conaryclient.ConaryClient(cfg)
    repos = client.getRepos()

    troveTups, namesOnly, primary  = getTrovesToDisplay(repos, cfg, troveSpecs, 
                                                        all, leaves)
    iterChildren = not namesOnly

    dcfg = display.DisplayConfig(repos, ls, ids, sha1s, digSigs,
                                 cfg.fullVersions, tags, info, deps,
                                 showBuildReqs, cfg.fullFlavors, iterChildren,
                                 cfg.showComponents)

    if primary:
        dcfg.setPrimaryTroves(set(troveTups))

    if dcfg.needFiles() and all:
        log.error('cannot use "all" with commands that require file lists')
        sys.exit(1)

    formatter = display.TroveFormatter(dcfg)

    display.displayTroves(dcfg, formatter, troveTups)


def getTrovesToDisplay(repos, cfg, troveSpecs, all, leaves):
    """ Finds troves that match the given trove specifiers, using the
        current configuration, and parameters

        @param repos: a network repository client
        @type repos: repository.netclient.NetworkRepositoryClient
        @param cfg: conary config
        @type cfg: conarycfg.ConaryConfiguration
        @param troveSpecs: troves to search for
        @type troveSpecs: list of troveSpecs (n[=v][[f]])
        @param all: If true, find all versions of the specified troves, 
                   not just the leaves
        @type all: bool
        @param leaves: If true, find all leaves of the specified troves,
                       regardless of whether they match the cfg flavor 
        @type leaves: bool

        @rtype: troveTupleList (list of (name, version, flavor) tuples)
                and a boolean that is true if all troveSpecs passed in do not 
                specify version or flavor
    """

    namesOnly = True

    if troveSpecs:
        primary = True
        troveSpecs = [ cmdline.parseTroveSpec(x) for x in troveSpecs ]
    else:
        primary = False
        troveSpecs = []

    for troveSpec in troveSpecs:
        if troveSpec[1:] != (None, None):
            namesOnly = False

    if not (all or leaves) and not troveSpecs:
        for label in cfg.installLabelPath:
            troveSpecs += [ (x, None, None) for x in repos.troveNames(label) ]
            troveSpecs.sort()
        allowMissing = True
    else:
        allowMissing = False

    troveTups = []
    if all or leaves:
        if troveSpecs:
            for (n, vS, fS) in troveSpecs:
                hostList = None
                if vS:
                    try:
                        label = versions.Label(vS)
                        hostList = [label.getHost()]
                    except versions.ParseError:
                        pass
                    if not hostList:
                        try:
                            ver = versions.VersionFromString(vS)
                            host = ver.getHost()
                        except versions.ParseError:
                            pass
                if not hostList:
                    hostList =  [ x.getHost() for x in cfg.installLabelPath ]

                repositories = {}
                for host in hostList:
                    d = repositories.setdefault(host, {})
                    l = d.setdefault(n, [])
                    l.append(fS)
        else:
            repositories = dict.fromkeys((x.getHost() for x in cfg.installLabelPath), {})

        if all:
            fn = repos.getTroveVersionList
        else:
            fn = repos.getAllTroveLeaves

        troveDict = {}
        for host, names in repositories.iteritems():
            d = fn(host, names)
            repos.queryMerge(troveDict, d)

        for n, verDict in troveDict.iteritems():
            for v, flavors in reversed(sorted(verDict.iteritems())):
                for f in flavors:
                    troveTups.append((n, v, f))
    else:
        results = repos.findTroves(cfg.installLabelPath, 
                                   troveSpecs, cfg.flavor, 
                                   acrossLabels = True,
                                   acrossFlavors = True,
                                   allowMissing = allowMissing)
        for troveSpec in troveSpecs:
            # make latest items be at top of list
            troveTups.extend(sorted(reversed(results.get(troveSpec, []))))

    return troveTups, namesOnly, primary
