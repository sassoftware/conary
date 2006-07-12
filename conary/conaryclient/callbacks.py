# -*- mode: python -*-
#
# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#


# Generally useful callbacks for client work...

from conary import callbacks

class FetchCallback(callbacks.LineOutput, callbacks.FetchCallback):
    def fetch(self, got, need):
        if need == 0:
            self._message("Downloading source (%dKb at %dKb/sec)..." \
                          % (got/1024, self.rate/1024))
        else:
            self._message("Downloading source (%dKb (%d%%) of %dKb at %dKb/sec)..." \
                          % (got/1024, (got*100)/need , need/1024, self.rate/1024))

    def __init__(self, *args, **kw):
        callbacks.LineOutput.__init__(self, *args, **kw)
        callbacks.FetchCallback.__init__(self, *args, **kw)

class ChangesetCallback(callbacks.LineOutput, callbacks.ChangesetCallback):

    def preparingChangeSet(self):
        self.updateMsg("Preparing changeset request")

    def requestingFileContents(self):
        self._message("Requesting file...")

    def downloadingFileContents(self, got, need):
        if need == 0:
            self._message("Downloading file (%dKb at %dKb/sec)..." \
                          % (got/1024, self.rate/1024))
        else:
            self._message("Downloading file (%dKb (%d%%) of %dKb at %dKb/sec)..." \
                          % (got/1024, (got*100)/need , need/1024, self.rate/1024))
    def downloadingChangeSet(self, got, need):
        self._downloading('Downloading', got, self.rate, need)

    def _downloading(self, msg, got, rate, need):
        if got == need:
            self.csText = None
        elif need != 0:
            if self.csHunk[1] < 2 or not self.updateText:
                self.csMsg("%s %dKb (%d%%) of %dKb at %dKb/sec"
                           % (msg, got/1024, (got*100)/need, need/1024, rate/1024))
            else:
                self.csMsg("%s %d of %d: %dKb (%d%%) of %dKb at %dKb/sec"
                           % ((msg,) + self.csHunk + \
                              (got/1024, (got*100)/need, need/1024, rate/1024)))
        else: # no idea how much we need, just keep on counting...
            self.csMsg("%s (got %dKb at %dKb/s so far)" % (msg, got/1024, rate/1024))

        self.update()

    def csMsg(self, text):
        self.csText = text
        self.update()

    def sendingChangeset(self, got, need):
        if need != 0:
            self._message("Committing changeset "
                          "(%dKb (%d%%) of %dKb at %dKb/sec)..."
                          % (got/1024, (got*100)/need, need/1024, self.rate/1024))
        else:
            self._message("Committing changeset "
                          "(%dKb at %dKb/sec)..." % (got/1024, self.rate/1024))


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
