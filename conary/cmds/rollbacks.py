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

from conary.lib import log
from conary.local import database
from conary.conaryclient import cmdline
from conary.repository import changeset, filecontainer

def listRollbacks(db, cfg):
    return formatRollbacks(cfg, db.getRollbackStack().iter(), stream=sys.stdout)

def versionFormat(cfg, version, defaultLabel = None):
    """Format the version according to the options in the cfg object"""
    if cfg.fullVersions:
        return str(version)

    if cfg.showLabels:
        ret = "%s/%s" % (version.branch().label(), version.trailingRevision())
        return ret

    if defaultLabel and (version.branch().label() == defaultLabel):
        return str(version.trailingRevision())

    ret = "%s/%s" % (version.branch().label(), version.trailingRevision())
    return ret

def verStr(cfg, version, flavor, defaultLabel = None):
    if defaultLabel is None:
        defaultLabel = cfg.installLabel

    ret = versionFormat(cfg, version, defaultLabel = defaultLabel)
    if cfg.fullFlavors:
        return "%s[%s]" % (ret, str(flavor))
    return ret

def formatRollbacks(cfg, rollbacks, stream=None):
    # Formatter function

    if stream is None:
        stream = sys.stdout

    # Display template
    templ = "\t%9s: %s %s\n"

    # Shortcut
    w_ = stream.write

    for (rollbackName, rb) in rollbacks:
        w_("%s:\n" % rollbackName)

        for cs in rb.iterChangeSets():
            newList = []
            for pkg in cs.iterNewTroveList():
                newList.append((pkg.getName(),
                                pkg.getOldVersion(), pkg.getOldFlavor(),
                                pkg.getNewVersion(), pkg.getNewFlavor()))
            oldList = [ x[0:3] for x in cs.getOldTroveList() ]

            newList.sort()

            # looks for components-of-packages and collapse those into the
            # package itself (just like update does)
            compByPkg = {}

            for info in newList:
                name = info[0]
                if ':' in name:
                    pkg, component = name.split(':')
                    pkgInfo = (pkg,) + info[1:]
                else:
                    pkgInfo = info
                    component = None
                l = compByPkg.setdefault(pkgInfo, [])
                l.append(component)

            oldList.sort()
            for info in newList:
                (name, oldVersion, oldFlavor, newVersion, newFlavor) = info
                if ':' in name:
                    pkgInfo = (name.split(':')[0],) + info[1:]
                    if None in compByPkg[pkgInfo]:
                        # this component was displayed with its package
                        continue

                if info in compByPkg:
                    comps = [":" + x for x in compByPkg[info] if x is not None]
                    if comps:
                        name += '(%s)' % " ".join(comps)

                if newVersion.onLocalLabel():
                    # Don't display changes to local branch
                    continue
                if not oldVersion:
                    w_(templ % ('erased', name,
                                verStr(cfg, newVersion, newFlavor)))
                else:
                    ov = oldVersion.trailingRevision()
                    nv = newVersion.trailingRevision()
                    if newVersion.onRollbackLabel() and ov == nv:
                        # Avoid displaying changes to rollback branch
                        continue
                    pn = "%s -> %s" % (verStr(cfg, newVersion, newFlavor),
                                       verStr(cfg, oldVersion, oldFlavor,
                                              defaultLabel =
                                                newVersion.branch().label()))
                    w_(templ % ('updated', name, pn))

            compByPkg = {}

            for name, version, flavor in oldList:
                if ':' in name:
                    pkg, component = name.split(':')
                else:
                    pkg = name
                    component = None
                l = compByPkg.setdefault((pkg, version, flavor), [])
                l.append(component)

            for (name, version, flavor) in oldList:
                if ':' in name:
                    pkgInfo = (name.split(':')[0], version, flavor)
                    if None in compByPkg[pkgInfo]:
                        # this component was displayed with its package
                        continue

                if (name, version, flavor) in compByPkg:
                    comps = [ ":" + x
                                for x in compByPkg[(name, version, flavor)]
                                if x is not None ]
                    if comps:
                        name += '(%s)' % " ".join(comps)
                w_(templ % ('installed', name, verStr(cfg, version, flavor)))

        w_('\n')

def formatRollbacksAsUpdate(cfg, rollbackList):
    updateTempl = "    %-7s %s %s"
    templ = "    %-7s %s=%s"
    print 'The following actions will be performed:'

    for idx, rb in enumerate(rollbackList):
        print 'Job %s of %s' % (idx + 1, len(rollbackList))

        newList = []
        oldList = []
        for cs in rb.iterChangeSets():
            for pkg in cs.iterNewTroveList():
                newList.append((pkg.getName(),
                                pkg.getOldVersion(), pkg.getOldFlavor(),
                                pkg.getNewVersion(), pkg.getNewFlavor()))
            oldList += [ x[0:3] for x in cs.getOldTroveList() ]
        newList.sort()

        # looks for components-of-packages and collapse those into the
        # package itself (just like update does)
        compByPkg = {}

        for info in newList:
            name = info[0]
            if ':' in name:
                pkg, component = name.split(':')
                pkgInfo = (pkg,) + info[1:]
            else:
                pkgInfo = info
                component = None
            l = compByPkg.setdefault(pkgInfo, [])
            l.append(component)

        oldList.sort()
        for info in newList:
            (name, oldVersion, oldFlavor, newVersion, newFlavor) = info
            if ':' in name:
                pkgInfo = (name.split(':')[0],) + info[1:]
                if None in compByPkg[pkgInfo]:
                    # this component was displayed with its package
                    continue

            if info in compByPkg:
                comps = [":" + x for x in compByPkg[info] if x is not None]
                if comps:
                    name += '(%s)' % " ".join(comps)

            if newVersion.onLocalLabel():
                # Don't display changes to local branch
                continue

            if not oldVersion:
                print(templ % ('Install', name,
                            verStr(cfg, newVersion, newFlavor)))
            else:
                ov = oldVersion.trailingRevision()
                nv = newVersion.trailingRevision()
                if newVersion.onRollbackLabel() and ov == nv:
                    # Avoid displaying changes to rollback branch
                    continue
                pn = "(%s -> %s)" % (verStr(cfg, oldVersion, newFlavor),
                                   verStr(cfg, newVersion, oldFlavor,
                                          defaultLabel =
                                            newVersion.branch().label()))
                print(updateTempl % ('Update', name, pn))

        compByPkg = {}

        for name, version, flavor in oldList:
            if ':' in name:
                pkg, component = name.split(':')
            else:
                pkg = name
                component = None
            l = compByPkg.setdefault((pkg, version, flavor), [])
            l.append(component)

        for (name, version, flavor) in oldList:
            if ':' in name:
                pkgInfo = (name.split(':')[0], version, flavor)
                if None in compByPkg[pkgInfo]:
                    # this component was displayed with its package
                    continue

            if (name, version, flavor) in compByPkg:
                comps = [ ":" + x
                            for x in compByPkg[(name, version, flavor)]
                            if x is not None ]
                if comps:
                    name += '(%s)' % " ".join(comps)
            print(templ % ('Erase', name, verStr(cfg, version, flavor)))

    return 0


def applyRollback(client, rollbackSpec, returnOnError = False, **kwargs):
    """
    Apply a rollback.

    See L{conary.conaryclient.ConaryClient.applyRollback} for a description of
    the arguments for this function.
    """
    client.checkWriteableRoot()
    # Record the transaction counter, to make sure the state of the database
    # didn't change while we were computing the rollback list.
    transactionCounter = client.db.getTransactionCounter()

    log.syslog.command()
    showInfoOnly = kwargs.pop('showInfoOnly', False)

    defaults = dict(replaceFiles = False,
                    transactionCounter = transactionCounter,
                    lazyCache = client.lzCache)
    defaults.update(kwargs)

    rollbackStack = client.db.getRollbackStack()
    rollbackList = rollbackStack.getList()

    if rollbackSpec.startswith('r.'):
        try:
            i = rollbackList.index(rollbackSpec)
        except ValueError:
            log.error("rollback '%s' not present" % rollbackSpec)
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)

        rollbacks = rollbackList[i:]
        rollbacks.reverse()
    else:
        try:
            rollbackCount = int(rollbackSpec)
        except ValueError:
            log.error("integer rollback count expected instead of '%s'" %
                    rollbackSpec)
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)

        if rollbackCount < 1:
            log.error("rollback count must be positive")
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)
        elif rollbackCount > len(rollbackList):
            log.error("rollback count higher then number of rollbacks "
                      "available")
            if returnOnError:
                return 1
            raise database.RollbackDoesNotExist(rollbackSpec)

        rollbacks = rollbackList[-rollbackCount:]
        rollbacks.reverse()

    capsuleChangeSet = changeset.ReadOnlyChangeSet()
    for path in defaults.pop('capsuleChangesets', []):
        if os.path.isdir(path):
            pathList = [ os.path.join(path, x) for x in os.listdir(path) ]
        else:
            pathList = [ path ]

        for p in pathList:
            if not os.path.isfile(p):
                continue

            try:
                cs = changeset.ChangeSetFromFile(p)
            except filecontainer.BadContainer:
                continue

            capsuleChangeSet.merge(cs)

    defaults['capsuleChangeSet'] = capsuleChangeSet

    #-- Show only information and return
    if showInfoOnly or client.cfg.interactive:
        rollbackList = [ rollbackStack.getRollback(x) for x in rollbacks if rollbackStack.hasRollback(x) ]
        formatRollbacksAsUpdate(client.cfg, rollbackList)

    if showInfoOnly:
        return 0

    #-- Interactive input (default behaviour)
    if client.cfg.interactive:
        okay = cmdline.askYn('continue with rollback? [y/N]', default=False)
        if not okay:
            return 1

    try:
        client.db.applyRollbackList(client.getRepos(), rollbacks, **defaults)
    except database.RollbackError, e:
        log.error("%s", e)
        if returnOnError:
            return 1
        raise

    log.syslog.commandComplete()

    return 0

def removeRollbacks(db, rollbackSpec):
    rollbackStack = db.getRollbackStack()
    rollbackList = rollbackStack.getList()

    if rollbackSpec.startswith('r.'):
        try:
            i = rollbackList.index(rollbackSpec)
        except:
            log.error("rollback '%s' not present" % rollbackSpec)
            return 1

        rollbacks = rollbackList[:i + 1]
    else:
        try:
            rollbackCount = int(rollbackSpec)
        except:
            log.error("integer rollback count expected instead of '%s'" %
                    rollbackSpec)
            return 1

        if rollbackCount < 1:
            log.error("rollback count must be positive")
            return 1
        elif rollbackCount > len(rollbackList):
            log.error("rollback count higher then number of rollbacks "
                      "available")
            return 1

        rollbacks = rollbackList[:rollbackCount]

    for rb in rollbacks:
        rollbackStack.remove(rb)

    return 0

#{ Classes used for the serialization of postrollback scripts.
class RollbackScriptsError(Exception):
    "Generic class for rollback scripts exceptions"

class _RollbackScripts(object):
    _KEY_JOB = 'job'
    _KEY_INDEX = 'index'
    _KEY_OLD_COMPAT_CLASS = 'oldCompatibilityClass'
    _KEY_NEW_COMPAT_CLASS = 'newCompatibilityClass'
    _KEYS = set([_KEY_JOB, _KEY_INDEX, _KEY_OLD_COMPAT_CLASS,
                 _KEY_NEW_COMPAT_CLASS])

    _metaFileNameTemplate = 'post-scripts.meta'
    _scriptFileNameTemplate = 'post-script.%d'

    def __init__(self):
        # Each item is a tuple (job, script, oldCompatClass, newCompatClass)
        self._items = []

    def add(self, job, script, oldCompatClass, newCompatClass, index=None):
        if index is None:
            index = len(self._items)
        self._items.append((index, job, script, oldCompatClass, newCompatClass))
        return self

    def __iter__(self):
        return iter(self._items)

    def getCreatedFiles(self, dir):
        "Returns the files that will be created on save"
        ret = set()
        ret.add(self._getMDFileName(dir))
        for idx, job, script, oldCompatClass, newCompatClass in self:
            fname = self._getScriptFileName(dir, idx)
            ret.add(fname)
        return ret

    def save(self, dir):
        # Save metadata
        stream = self._openFile(self._getMDFileName(dir))
        self.saveMeta(stream)
        stream.close()
        for idx, job, script, oldCompatClass, newCompatClass in self:
            # Save individual scripts
            fname = self._getScriptFileName(dir, idx)
            self._openFile(fname).write(script)

    def saveMeta(self, stream):
        for idx, job, script, oldCompatClass, newCompatClass in self:
            if idx > 0:
                # Add the double-newline as a group separator
                stream.write('\n')

            lines = self._serializeMeta(idx, job, oldCompatClass,
                                        newCompatClass)
            for line in lines:
                stream.write(line)
                stream.write('\n')

    @classmethod
    def load(cls, dir):
        ret = cls()
        group = []

        try:
            stream = file(cls._getMDFileName(dir))
        except IOError, e:
            raise RollbackScriptsError("Open error: %s: %s: %s" %
                (e.errno, e.filename, e.strerror))

        while 1:
            line = stream.readline()
            sline = line.strip()
            if not sline:
                # Empty line (either from a double-newline or from EOF)
                if group:
                    cls._finalize(dir, group, ret)
                if line:
                    # Double-newline
                    continue
                # EOF
                break
            group.append(sline)
        return ret

    @classmethod
    def _finalize(cls, dir, group, rbs):
        idx, g = cls._parseMeta(group)
        del group[:]
        if g is not None:
            try:
                scfile = file(cls._getScriptFileName(dir, idx))
            except IOError:
                # If a script is missing, oh well...
                return
        rbs.add(g[0], scfile.read(), g[1], g[2], index=idx)

    @classmethod
    def _serializeVF(cls, version, flavor):
        if version is None:
            return ''
        if flavor is None or not str(flavor):
            return str(version)
        return "%s[%s]" % (version, flavor)

    @classmethod
    def _serializeJob(cls, job):
        return "%s=%s--%s" % (job[0],
                              cls._serializeVF(*job[1]),
                              cls._serializeVF(*job[2]))

    @classmethod
    def _serializeMeta(cls, idx, job, oldCompatClass, newCompatClass):
        lines = []
        lines.append('%s: %d' % (cls._KEY_INDEX, idx))
        lines.append('%s: %s' % (cls._KEY_JOB, cls._serializeJob(job)))
        lines.append('%s: %s' % (cls._KEY_OLD_COMPAT_CLASS, oldCompatClass))
        lines.append('%s: %s' % (cls._KEY_NEW_COMPAT_CLASS, newCompatClass))
        return lines

    @classmethod
    def _parseMeta(cls, lines):
        ret = {}
        for line in lines:
            arr = line.split(': ', 1)
            if len(arr) != 2:
                continue
            if arr[0] not in cls._KEYS:
                continue
            ret[arr[0]] = arr[1]
        if cls._KEYS.difference(ret.keys()):
            # Missing key
            return None
        job = cmdline.parseChangeList([ret[cls._KEY_JOB]])[0]
        oldCompatClass = cls._toInt(ret[cls._KEY_OLD_COMPAT_CLASS])
        newCompatClass = cls._toInt(ret[cls._KEY_NEW_COMPAT_CLASS])
        try:
            idx = int(ret[cls._KEY_INDEX])
        except ValueError:
            return None
        return idx, (job, oldCompatClass, newCompatClass)

    @classmethod
    def _toInt(cls, value):
        if value == 'None':
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @classmethod
    def _openFile(cls, fileName):
        flags = os.O_WRONLY | os.O_CREAT
        try:
            fd = os.open(fileName, flags, 0600)
        except OSError, e:
            raise RollbackScriptsError("Open error: %s: %s: %s" %
                (e.errno, e.filename, e.strerror))

        return os.fdopen(fd, "w")

    @classmethod
    def _getMDFileName(cls, dir):
        return os.path.join(dir, cls._metaFileNameTemplate)

    @classmethod
    def _getScriptFileName(cls, dir, idx):
        return os.path.join(dir, cls._scriptFileNameTemplate % idx)

#}
