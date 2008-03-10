# -*- mode: python -*-
#
# Copyright (c) 2006-2007 rPath, Inc.
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


# Generally useful callbacks for client work...

from conary import callbacks
from conary import changelog

class FetchCallback(callbacks.LineOutput, callbacks.FetchCallback):
    def fetch(self, got, need):
        if need == 0:
            self._message("Downloading source (%dKB at %dKB/sec)..." \
                          % (got/1024, self.rate/1024))
        else:
            self._message("Downloading source (%dKB (%d%%) of %dKB at %dKB/sec)..." \
                          % (got/1024, (got*100)/need , need/1024, self.rate/1024))

    def __init__(self, *args, **kw):
        callbacks.LineOutput.__init__(self, *args, **kw)
        callbacks.FetchCallback.__init__(self, *args, **kw)

class ChangesetCallback(callbacks.LineOutput, callbacks.ChangesetCallback):

    def preparingChangeSet(self):
        self.updateMsg("Preparing changeset request")

    def requestingFileContents(self, count=0):
        if not count:
            self._message("Requesting file...")
        else:
            self._message("Requesting %s files...")

    def downloadingFileContents(self, got, need):
        if need == 0:
            self._message("Downloading file (%dKB at %dKB/sec)..." \
                          % (got/1024, self.rate/1024))
        else:
            self._message("Downloading file (%dKB (%d%%) of %dKB at %dKB/sec)..." \
                          % (got/1024, (got*100)/need , need/1024, self.rate/1024))
    def downloadingChangeSet(self, got, need):
        self._downloading('Downloading', got, self.rate, need)

    def _downloading(self, msg, got, rate, need):
        if got == need:
            self.csText = None
        elif need != 0:
            if self.csHunk[1] < 2 or not self.updateText:
                self.csMsg("%s %dKB (%d%%) of %dKB at %dKB/sec"
                           % (msg, got/1024, (got*100)/need, need/1024, rate/1024))
            else:
                self.csMsg("%s %d of %d: %dKB (%d%%) of %dKB at %dKB/sec"
                           % ((msg,) + self.csHunk + \
                              (got/1024, (got*100)/need, need/1024, rate/1024)))
        else: # no idea how much we need, just keep on counting...
            self.csMsg("%s (got %dKB at %dKB/s so far)" % (msg, got/1024, rate/1024))

        self.update()

    def csMsg(self, text):
        self.csText = text
        self.update()

    def sendingChangeset(self, got, need):
        if need != 0:
            self._message("Committing changeset "
                          "(%dKB (%d%%) of %dKB at %dKB/sec)..."
                          % (got/1024, (got*100)/need, need/1024, self.rate/1024))
        else:
            self._message("Committing changeset "
                          "(%dKB at %dKB/sec)..." % (got/1024, self.rate/1024))


    def update(self):
        t = self.csText
        if t:
            self._message(t)
        else:
            self._message('')

    def done(self):
        self._message('')

    def _message(self, txt, usePrefix=True):
        if txt and usePrefix:
            return callbacks.LineOutput._message(self, self.prefix + txt)
        else:
            return callbacks.LineOutput._message(self, txt)

    def setPrefix(self, txt):
        self.prefix = txt

    def clearPrefix(self):
        self.prefix = ''

    def __init__(self, *args, **kw):
        self.csHunk = (0, 0)
        self.csText = None
        self.prefix = ''
        callbacks.LineOutput.__init__(self, *args, **kw)
        callbacks.ChangesetCallback.__init__(self, *args, **kw)

class CloneCallback(ChangesetCallback, callbacks.CloneCallback):
    def __init__(self, cfg, defaultMessage=None):
        self.cfg = cfg
        if defaultMessage and defaultMessage[:-1] != '\n':
            defaultMessage += '\n'
        self.defaultMessage = defaultMessage
        callbacks.CloneCallback.__init__(self, cfg)
        ChangesetCallback.__init__(self)

    @callbacks.passExceptions
    def getCloneChangeLog(self, trv):
        if self.cfg.name is None or self.cfg.contact is None:
            raise ValueError, \
                    "name and contact information must be set for clone"

        message = self.defaultMessage
        cl = changelog.ChangeLog(self.cfg.name, self.cfg.contact, message)
        prompt = ('Please enter the clone message'
                  ' for\n %s=%s.' % (trv.getName(), trv.getVersion()))
        if not message and not cl.getMessageFromUser(prompt=prompt):
            return None
        return cl

    def determiningCloneTroves(self, current=0, total=0):
        if total:
            self._message('Step 1/5: Determining items to clone...(%s/%s)' % (current, total))
        else:
            self._message('Step 1/5: Determining items to clone...')

    def determiningTargets(self):
        self._message('Step 2/5: Determining target versions...')

    def targetSources(self, current=0, total=0):
        self.prefix = 'Step 2/5: '
        if total:
            self._message('Targeting Sources (%s/%s)' % (current, total))
        else:
            self._message('Targeting Sources' % (current, total))

    def targetBinaries(self, current=0, total=0):
        self.prefix = 'Step 2/5: '
        if total:
            self._message('Targeting Binaries (%s/%s)' % (current,
                                                                    total))
        else:
            self._message('Targeting Binaries')

    def checkNeedsFulfilled(self, current=0, total=0):
        self.prefix = 'Step 3/5: '
        if total:
            self._message('Making sure clone is complete (%s/%s)' % (
                                                                    current,
                                                                    total))
        else:
            self._message('Making sure clone is complete ')

    def rewriteTrove(self, current=0, total=0):
        self.prefix = 'Step 4/5:'
        if total:
            self._message('Rewriting trove information (%s/%s)' % (current, total))
        else:
            self._message('Rewriting trove information')

    def rewritingFileVersions(self, current=0, total=0):
        if total:
            percent = (current * 1000 / total) / 10.0
            self.prefix = 'Step 5/5 (%s%%): ' % (percent)
            self._message('Rewriting file versions')
        else:
            self.prefix = 'Step 5/5: '
            self._message('Rewriting file versions ')

    def requestingFiles(self, number):
        self._message('Requesting file info for %s files...' % (number))

    def requestingFileContentsWithCount(self, count):
        self._message("Requesting file contents %s files..." % count)

    def gettingCloneData(self):
        self._message('Getting file contents for clone...')

