#!/usr/bin/env python
#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import os
import sys

if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/conary/scripts")
            
import shlex
import imp

from conary import conarycfg
from conary.lib import options, util
from conary.conaryclient import ConaryClient
from conary import versions

usageMessage = "\n".join((
     "Usage: commitaction --repmap <repmap> --build-label <buildLabel>",
     "                    [--username <repository username> --password <password>]",
     "                    [--config-file <path>] [--config 'item value']",
     "                    --module '/path/to/module module options'",
     r"Takes triplets of name\nversion\nflavor\n on standard input",
     ""
    ))

def usage(exitcode=1):
    sys.stderr.write(usageMessage)
    sys.exit(exitcode)

def main(argv):
    if not len(argv) > 1:
        usage()

    sys.excepthook = util.genExcepthook(prefix='commitaction-stack-')

    argDef = {
        'module': options.MULT_PARAM,
        'config-file': options.ONE_PARAM,
        'config': options.MULT_PARAM,
        'username': options.OPT_PARAM,
        'password': options.OPT_PARAM,
        'repmap': options.OPT_PARAM,
    }

    cfgMap = {
        'build-label': 'buildLabel',
    }

    cfg = conarycfg.ConaryConfiguration()
    cfg.root = ":memory:"
    cfg.dbPath = ":memory:"
    argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usageMessage,
                                            argv=argv)


    # remove argv[0]
    otherArgs = otherArgs[1:]

    if 'module' not in argSet:
        usage()

    for line in argSet.pop('config', []):
        cfg.configLine(line)

    if 'repmap' in argSet:
        # this is ONLY for accessing the committing repository
        host, url = argSet['repmap'].split(" ")
        cfg.repositoryMap.update({host: url})
        
        if 'username' in argSet and 'password' in argSet:
            cfg.user.addServerGlob(host, argSet['username'], argSet['password'])

    repos = ConaryClient(cfg).getRepos()

    data = [x[:-1] for x in sys.stdin.readlines()]

    # { 'pkg:source': [(version, shortversion), ...] }
    srcMap = {}
    # { 'pkg' { version: { flavor: [ component, ...] } } }
    pkgMap = {}
    # { 'group-foo' { version: set(flavor, ...) } }
    grpMap = {}

    # [1,2,3,4,5,6,...] -> [(1,2,3), (4,5,6), ...]
    commitList = zip(data, data[1:], data[2:])[::3]

    for name, version, flavor in commitList:
        if name[-7:] == ':source':
            # get full trailing version
            trailingVersion = versions.VersionFromString(
                version).trailingRevision().asString()
            # sources are not flavored
            l = srcMap.setdefault(name, [])
            l.append((version, trailingVersion))
        elif ':' in name:
            package, component = name.split(':')
            d = pkgMap.setdefault(package, {})
            d = d.setdefault(version, {})
            l = d.setdefault(flavor, [])
            l.append(component)
        elif name.startswith('group-'):
            d = grpMap.setdefault(name, {})
            s = d.setdefault(version, set())
            s.add(flavor)

    ret = 0
    for module in argSet['module']:
        argv = shlex.split(module)
        path = argv[0]
        dirname, name = os.path.split(path)
        if name.endswith('.py'):
            name = name[:-3]
        if dirname:
            searchPath = [dirname]
            try:
                f, pathName, description = imp.find_module(name, searchPath)
            except:
                break
            try:
                mod = imp.load_module(name, f, pathName, description)
            finally:
                f.close()
        else:
            try:
                mod = __import__(name)
                names = name.split('.')[1:]
                for subname in names:
                    mod = getattr(mod, subname)
            except:
                break

        # pass repos, cfg, and all otherArgs to all modules
        if 'process' in mod.__dict__:
            ret |= mod.process(repos, cfg, commitList, srcMap, pkgMap, grpMap,
                    argv[1:], otherArgs)

    return ret

if __name__ == "__main__":
    sys.exit(main(sys.argv))
