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

        self.intro = """dbstore sql shell.
type ".quit" to exit, ".help" for help"""

        self.showHeaders = False

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

        if cmd == 'EOF':
            # on EOF, ask to stop
            print
            return True

        if not cmd:
            # no command, noop
            return False

        if ';' in cmd:
            # split up the command and execute each part
            for onecmd in cmd.split(';', 1):
                self.onecmd(onecmd)
            return False

        # execute the SQL command
        try:
            self.cu.execute(cmd)
        except sqlerrors.DatabaseError, e:
            print 'Error:', str(e.args[0])
            return False

        # print the results (if any)
        fields = None
        for row in self.cu:
            if self.showHeaders and not fields:
                fields = self.cu.fields()
                print '|'.join(fields)
            print '|'.join(str(x) for x in row)

        # reload the schema, in case there was a change
        self.db.loadSchema()

    def cmdloop(self):
        self.read_history()
        while 1:
            try:
                rc = cmd.Cmd.cmdloop(self)
            except KeyboardInterrupt:
                # let Ctrl+C just cancel input, like in a shell
                self.intro = None
                print
                continue
            break
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

    def completenames(self, text, *ignored):
        # override completenames to append the . at the start
        if text.startswith('.'):
            text = text[1:]
        dotext = 'do_'+text
        return ['.' + a[3:] for a in self.get_names() if a.startswith(dotext)]

    def do_show(self, arg):
        schemaBits = ('tables', 'triggers', 'functions', 'sequences',
                      'triggers')
        if arg in schemaBits:
            d = self.db.__dict__[arg]
            print '\n'.join(sorted(d.keys()))
        else:
            print 'unknown argument', arg
        return False

    def help_show(self):
        print """show [tables/triggers/functions/sequences/triggers]
display database information"""

    def do_headers(self, arg):
        if arg in ('on', 'yes'):
            self.showHeaders = True
        elif arg in ('off', 'no'):
            self.showHeaders = False
        else:
            print 'unknown argument', arg
        return False

    def help_headers(self):
        print """headers [on/off]
turn the display of headers on or off"""

    do_head = do_headers
    help_head = do_head

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
