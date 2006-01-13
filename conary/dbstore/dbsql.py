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

class DbSql(cmd.Cmd):
    def __init__(self, driver=None, path=None, db=None):
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

    def do_quit(self, arg):
        sys.exit()

    def help_quit(self):
        print """quit
quit the shell"""

    def help_help(self):
        print """quit
display help"""
