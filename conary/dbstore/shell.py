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
import itertools

try:
    import readline
except ImportError:
    hasReadline = False
else:
    hasReadline = True

class DbShell(cmd.Cmd):
    _historyPath = os.path.expanduser('~/.dbshellhistory')
    yesArgs = ('on', 'yes')
    noArgs = ('off', 'no')
    prompt = 'dbsh> '
    multilinePrompt = ' ...> '
    doc_header = "Documented commands (type .help <topic>):"
    intro = """dbstore sql shell.
type ".quit" to exit, ".help" for help"""

    def __init__(self, db = None, driver = None, path = None):
        cmd.Cmd.__init__(self)

        # default to .head off
        self.showHeaders = False
        # default to .mode list
        self.display = self.display_list
        # a dictionary of column number: width for manual setting
        self.manual_widths = {}

        if driver and path:
            self.db = dbstore.connect(path, driver=driver)
        elif db:
            self.db = db
        else:
            raise RuntimeError, 'driver and path OR db must be given'
        self.cu = self.db.cursor()

    def multiline(self, firstline=''):
        full_input = []
        # keep a list of the entries that we've made in history
        old_hist = []
        if firstline:
            full_input.append(firstline)
        while True:
            if hasReadline:
                # add the current readline position
                old_hist.append(readline.get_current_history_length())
            if self.use_rawinput:
                try:
                    line = raw_input(self.multilinePrompt)
                except EOFError:
                    line = 'EOF'
            else:
                self.stdout.write(self.multilinePrompt)
                self.stdout.flush()
                line = self.stdin.readline()
                if not len(line):
                    line = 'EOF'
                else:
                    line = line[:-1] # chop \n
            if line == 'EOF':
                print
                break
            full_input.append(line)
            if ';' in line:
                break

        # add the final readline history position
        if hasReadline:
            old_hist.append(readline.get_current_history_length())

        cmd = ' '.join(full_input)
        if hasReadline:
            # remove the old, individual readline history entries.

            # first remove any duplicate entries
            old_hist = sorted(set(old_hist))

            # Make sure you do this in reversed order so you move from
            # the end of the history up.
            for pos in reversed(old_hist):
                # get_current_history_length returns pos + 1
                readline.remove_history_item(pos - 1)
            # now add the full line
            readline.add_history(cmd)

        return cmd

    def parseline(self, line):
        # knock off any leading .
        line = line.strip()
        if line.startswith('.'):
            line = line[1:]
        return cmd.Cmd.parseline(self, line)

    def set_mode(self, mode):
        self.display = getattr(self, 'display_' + mode)

    def display_column(self, cu):
        fields = None
        widths = [ len(s) + 2 for s in self.cu.fields() ]
        # override widths if they are set manually
        for col, width in enumerate(widths):
            if col in self.manual_widths:
                widths[col] = self.manual_widths[col]
        # the total width is the sum of widths plus | for each col
        total = sum(widths) + len(widths) - 1
        # draw a bar like ---+---+---
        bar = '+'.join('-' * x for x in widths)
        # surround it with + to make +---+---+---+
        bar = '+' + bar + '+'
        # build up a format string like %5.5s|%6.6s
        format = '|'.join(('%%%d.%ds' % (x, x)) for x in widths)
        # and surround it with |
        format = '|' + format + '|'
        print bar
        for row in self.cu:
            if self.showHeaders and not fields:
                fields = self.cu.fields()
                print format % tuple(' %s ' %x for x in fields)
                print bar
            print format % tuple(row)
        print bar

    def display_list(self, cu):
        fields = None
        for row in self.cu:
            if self.showHeaders and not fields:
                fields = self.cu.fields()
                print '|'.join(fields)
            print '|'.join(str(x) for x in row)

    def default(self, cmd):
        cmd = cmd.strip()

        if cmd == 'EOF':
            # EOF means exit.  print a new line to clean up
            print
            return True

        if not cmd.endswith(';'):
            cmd = self.multiline(cmd).strip()

        # if there are multiple statements on one line, split them up
        if ';' in cmd:
            # split up the command and execute each part
            complete, partial = cmd.split(';', 1)
            if partial:
                # if there are two or more commands, run the first
                self.default(complete + ';')
                return self.default(partial)

        if not cmd:
            # no sql, noop
            return False

        # execute the SQL command
        try:
            self.cu.execute(cmd)
        except sqlerrors.DatabaseError, e:
            print 'Error:', str(e.args[0])
            return False

        # display the results (if any)
        self.display(self.cu)

        # reload the schema, in case there was a change
        self.db.loadSchema()
        return False

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
        if text.startswith('.'):
            text = text[1:]
        dotext = 'do_'+text
        return ['.' + a[3:] for a in self.get_names() if a.startswith(dotext)]

    # funtions defined below
    schemaBits = ('tables', 'triggers', 'functions', 'sequences',
                  'triggers')
    def do_show(self, arg):
        if arg in self.schemaBits:
            d = getattr(self.db, arg)
            print '\n'.join(sorted(d.keys()))
        else:
            print 'unknown argument', arg
        return False

    def help_show(self):
        print """show [tables/triggers/functions/sequences/triggers]
display database information"""

    def complete_show(self, text, *ignored):
        return [x for x in self.schemaBits if x.startswith(text)]

    # headers
    def do_headers(self, arg):
        if arg in self.yesArgs:
            self.showHeaders = True
        elif arg in self.noArgs:
            self.showHeaders = False
        else:
            print 'unknown argument', arg
        return False

    def help_headers(self):
        print """headers [on/off]
turn the display of headers on or off"""

    def complete_headers(self, text, *ignored):
        return [x for x in itertools.chain(self.yesArgs, self.noArgs)
                if x.startswith(text)]

    do_head = do_headers
    help_head = help_headers
    complete_head = complete_headers

    # mode
    modes = ('column', 'list')
    def do_mode(self, arg):
        choices = [x for x in self.modes if x.startswith(arg)]
        if len(choices) != 1:
            print 'unknown argument', arg
            return False
        self.set_mode(choices[0])
        return False

    def help_mode(self):
        print """mode [%s]
change the display mode""" % '/'.join(self.modes)

    def complete_mode(self, text, *ignored):
        return [x for x in self.modes if x.startswith(text)]

    do_head = do_headers
    help_head = help_headers
    complete_head = complete_headers

    # width
    def do_width(self, arg):
        if '=' in arg:
            col, width = arg.split('=', 1)
            col = col.strip()
            width = width.strip()
            try:
                col = int(col)
            except ValueError:
                print 'invalid argument for .width column=width: %s' %col
                return False
            try:
                width = int(width)
            except ValueError:
                print 'invalid argument for .width column=width: %s' %width
                return False
            if col < 0:
                print 'invalid argument for .width column=width: %s' %col
            self.manual_widths[col - 1] = width
        else:
            new_widths = {}
            for col, width in enumerate(arg.split()):
                width = width.strip()
                try:
                    width = int(width)
                except ValueError:
                    print 'invalid argument for .width NUM NUM ...: %s' %width
                    return False
                new_widths[col] = width

            self.manual_widths.update(new_widths)

    def help_width(self):
        print """width [col=width || width width width ...]
set the width of a column manually"""

    # quit
    def do_quit(self, arg):
        # ask to stop
        return True

    def help_quit(self):
        print """quit
quit the shell"""

    # reset
    def do_reset(self, arg):
        # write Ctrl+O
        sys.stdout.write('\017')
        sys.stdout.flush()

    def help_reset(self):
        print """reset
shift the terminal back into mode 1 (like the reset command line tool)"""

    # help (mostly builtin)
    def help_help(self):
        print """quit
display help"""


def shell(db):
    'invokes a dbstore sql shell on an existing db connection'
    shell = DbShell(db)
    return shell.cmdloop()
