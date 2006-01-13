#!/usr/bin/python
#
# Copyright (c) 2006 rPath, Inc.
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

import cmd
import sys
from conary import dbstore
import sqlerrors
import os

try:
    import readline
except ImportError:
    hasReadline = False
else:
    hasReadline = True

class DbSql(cmd.Cmd):
    _historyPath = os.path.expanduser('~/.dbsqlhistory')

    def __init__(self, db = None, driver = None, path = None):
        cmd.Cmd.__init__(self)
        self.prompt = 'dbsql> '
        self.doc_header = "Documented commands (type .help <topic>):"
        self.intro = 'dbstore sql shell.  type ".quit" to exit'
        if driver and path:
            self.db = dbstore.connect(path, driver=driver)
        elif db:
            self.db = db
        else:
            raise RuntimeError, 'driver and path OR db must be given'
        self.cu = self.db.cursor()

    def onecmd(self, cmd):
        # remove any trailing whitespace
        cmd = cmd.strip()

        if cmd.startswith('.'):
            cmd = cmd[1:]
            cmd, arg, line = self.parseline(cmd)
            try:
                func = getattr(self, 'do_' + cmd)
            except AttributeError:
                return self.default(line)
            return func(arg)

        if not cmd:
            return
        if ';' in cmd:
            onecmd, rest = cmd.split(';')
            self.onecmd(onecmd)
            self.onecmd(rest)
            return
        try:
            self.cu.execute(cmd)
        except sqlerrors.DatabaseError, e:
            print 'Error:', str(e.args[0])
        fields = None
        for row in self.cu:
            if not fields:
                fields = self.cu.fields()
                print '|'.join(fields)
            print '|'.join(repr(x) for x in row)

    def cmdloop(self):
        self.read_history()
        rc = cmd.Cmd.cmdloop(self)
        self.save_history()
        return rc

    def read_history(self):
        if hasReadline and self._historyPath:
            try:
                readline.read_history_file(self._historyPath)
            except:
                pass

    def save_history(self):
        if hasReadline and self._historyPath:
            readline.set_history_length(1000)
            try:
                readline.write_history_file(self._historyPath)
            except:
                pass

    def do_quit(self, arg):
        # ask to stop
        return True

    def help_quit(self):
        print """quit
quit the shell"""

    def help_help(self):
        print """quit
display help"""


def shell(db):
    'invokes a dbstore sql shell on an existing db connection'
    shell = DbSql(db)
    return shell.cmdloop()
