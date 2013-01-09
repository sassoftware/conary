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


import cmd
import sys
from conary import dbstore
import sqlerrors
import os
import itertools
import time

try:
    import readline
except ImportError:
    hasReadline = False
else:
    hasReadline = True

class DbShell(cmd.Cmd):
    _history_path = os.path.expanduser('~/.dbsh_history')
    yes_args = ('on', 'yes')
    no_args = ('off', 'no')
    # sql commands that are commonly at the start of a sql command
    sqlstarters = ['alter', 'create', 'delete', 'drop', 'insert into',
                   'select', 'update']
    # additional sql keywords
    sqlkeywords = ['database', 'from', 'left', 'limit', 'index',
                   'join', 'null', 'outer', 'table', 'trigger',
                   'using', 'values', 'view']

    prompt = 'dbsh> '
    multiline_prompt = ' ...> '
    doc_header = "Documented commands (type .help <topic>):"
    intro = """dbstore sql shell.
type ".quit" to exit, ".help" for help"""
    identchars = cmd.IDENTCHARS + '.'
    default_width = 10

    def __init__(self, db = None, driver = None, path = None):
        cmd.Cmd.__init__(self)

        # default to .head off
        self.show_headers = False
        # default to .mode list
        self.format = self.format_list
        # a dictionary of column number: width for manual setting
        self.manual_widths = {}
        # use a pager?
        self.use_pager = False
        # calculate column widths for column view?
        self.auto_width = False
        # display stats: N rows in set (0.00 sec)
        self.show_stats = True

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
                    line = raw_input(self.multiline_prompt)
                except EOFError:
                    line = 'EOF'
            else:
                self.stdout.write(self.multiline_prompt)
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
        line = line.strip()
        # convert . to _, which allows us to differentiate dbsh commands
        # from sql commands (like .show tables versus show variables)
        if line.startswith('.'):
            line = '_' + line[1:]
        return cmd.Cmd.parseline(self, line)

    def calculate_widths(self, cu):
        if self.show_headers:
            widths = [ len(s) + 4 for s in cu.fields() ]
        else:
            widths = [ 0 ] * len(cu.fields())
        rows = []
        for row in cu:
            for pos, col in enumerate(row):
                widths[pos] = max(widths[pos], len(str(col)))
            rows.append(row)
        return widths, rows

    def format_column_rows(self, headers, rows, widths = None):
        if not widths:
            if self.show_headers:
                widths = [ len(s) + 4 for s in headers ]
            else:
                widths = [ self.default_width ] * len(headers)
        # override widths if they are set manually
        for col, width in enumerate(widths):
            if col in self.manual_widths:
                widths[col] = self.manual_widths[col]
        # the total width is the sum of widths plus | for each col
        total = sum(widths) + (len(widths) - 1)
        # draw a bar like ---+---+---
        bar = '+'.join('-' * (x + 2) for x in widths)
        # surround it with + to make +---+---+---+
        bar = '+' + bar + '+'
        # build up a format string like %-5.5s|%-6.6s
        format = '|'.join((' %%-%d.%ds ' % (x, x)) for x in widths)
        # and surround it with |
        format = '|' + format + '|'
        yield 0, bar
        if self.show_headers:
            yield 0, format % tuple(' %s ' %x for x in headers)
            yield 0, bar
        for row in rows:
            yield 1, format % tuple(row)
        yield 0, bar

    def format_column(self, cu):
        if self.auto_width:
            widths, rows = self.calculate_widths(cu)
            return self.format_column_rows(cu.fields(), rows, widths)
        return self.format_column_rows(cu.fields(), cu)

    def format_val(self, col):
        if isinstance(col, float):
            return '%f' %col
        return str(col)

    def format_list(self, cu):
        fields = None
        for row in cu:
            if self.show_headers and not fields:
                fields = cu.fields()
                yield 0, '|'.join(fields)
            yield 1, '|'.join(self.format_val(x) for x in row)

    def display(self, cu):
        lines = self.format(cu)
        if self.use_pager:
            import pydoc
            rows = [ x for x in lines ]
            text = '\n'.join(x[1] for x in rows)
            rows = sum(x[0] for x in rows)
            end = time.time()
            pydoc.pager(text)
        else:
            rows = 0
            for isrow, line in lines:
                if isrow:
                    rows += 1
                print line
            end = time.time()
        return rows, end

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

        start = time.time()
        # execute the SQL command
        try:
            # a weak attempt to see if we have any %(keyword)s to expand
            if r'%(' in cmd:
                cmd = cmd % self.db.keywords
            self.cu.execute(cmd)
        except sqlerrors.DatabaseError, e:
            if len(e.args) > 1:
                print 'Error:', str(e.args[0])
            else:
                print 'Error:', str(e)
            return False
        except Exception, e:
            print 'Error:', str(e)
            return False

        # check for no rows
        if self.cu.fields() is None or not len(self.cu.fields()):
            print "Query OK"
            # reload the schema, in case there was a change
            self.db.loadSchema()
        else: # display the results (if any)
            rows, end = self.display(self.cu)
            if self.show_stats and rows != -1:
                print '%d rows in set (%.2f sec)' %(rows, end - start)
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
        if hasReadline and self._history_path:
            try:
                readline.read_history_file(self._history_path)
            except:
                pass

    def save_history(self):
        if hasReadline and self._history_path:
            readline.set_history_length(1000)
            try:
                readline.write_history_file(self._history_path)
            except:
                pass

    def completedefault(self, text, line, begin, end):
        line = line.strip().lower()
        tables = self.db.tables.keys()
        views = self.db.views.keys()
        if (line.endswith('from') or line.endswith('into')
            or line.endswith('update')):
            candidates = tables + views
        elif line.endswith('table'):
            candidates = tables
        elif line.endswith('view'):
            candidates = views
        else:
            candidates = tables + views + self.sqlkeywords

        text = text.lower()
        return [a + ' ' for a in candidates if a.startswith(text)]

    def completenames(self, text, line, begin, end):
        cmds = ['.' + a[4:] for a in self.get_names()
                if a.startswith('do__')]
        if line.endswith('.help '):
            starters = cmds
        else:
            starters = cmds + self.sqlstarters

        cmd, arg, line = self.parseline(text)
        if not cmd:
            return starters
        text = text.lower()
        return [a + ' ' for a in starters if a.startswith(text)]

    def print_topics(self, header, cmds, cmdlen, maxcol):
        if cmds == ['help']:
            return
        if cmds:
            self.stdout.write("%s\n"%str(header))
            if self.ruler:
                self.stdout.write("%s\n"%str(self.ruler * len(header)))
            newcmds = []
            for cmd in cmds:
                if cmd.startswith('_'):
                    newcmds.append('.' + cmd[1:])
                else:
                    newcmds.append(cmd)
            self.columnize(newcmds, maxcol-1)
            self.stdout.write("\n")

    def complete__yesno(self, text, *ignored):
        return [x for x in itertools.chain(self.yes_args, self.no_args)
                if x.startswith(text)]

    def complete__noop(self, *args):
        return []

    # funtions defined below
    schemaBits = ('tables', 'triggers', 'functions', 'sequences',
                  'triggers')
    def do__show(self, arg):
        self.db.loadSchema()
        if arg in self.schemaBits:
            d = getattr(self.db, arg)
            print '\n'.join(sorted(d.keys()))
        else:
            print 'unknown argument', arg
        return False

    def help__show(self):
        print """show %s
display database information""" % ' '.join('[%s]' % x for x in self.schemaBits)

    def complete__show(self, text, *ignored):
        return [x for x in self.schemaBits if x.startswith(text)]

    # headers
    def do__headers(self, arg):
        if arg in self.yes_args:
            self.show_headers = True
        elif arg in self.no_args:
            self.show_headers = False
        else:
            print 'unknown argument', arg
        return False

    def help__headers(self):
        print """headers [on/off]
turn the display of headers on or off"""

    do__head = do__headers
    help__head = help__headers
    complete__headers = complete__yesno
    complete__head = complete__yesno

    # pager
    def do__pager(self, arg):
        if arg in self.yes_args:
            self.use_pager = True
        elif arg in self.no_args:
            self.use_pager = False
        else:
            print 'unknown argument', arg
        return False

    def help__pager(self):
        print """pager [on/off]
turn the use of the pager on or off"""

    complete__pager = complete__yesno

    # stats
    def do__stats(self, arg):
        if arg in self.yes_args:
            self.show_stats = True
        elif arg in self.no_args:
            self.show_stats = False
        else:
            print 'unknown argument', arg
        return False

    def help__stats(self):
        print """stats [on/off]
turn the display of query statistics on or off"""

    complete__stats = complete__yesno

    # mode
    def set_mode(self, mode):
        self.format = getattr(self, 'format_' + mode)

    modes = ('column', 'list')
    def do__mode(self, arg):
        # allow the user to abbreviate, as long as it's enough
        # to be unambiguous
        choices = [x for x in self.modes if x.startswith(arg)]
        if len(choices) != 1:
            print 'unknown argument', arg
            return False
        self.set_mode(choices[0])
        return False

    def help__mode(self):
        print """mode %s
change the display mode""" % ' '.join('[%s]' %x for x in self.modes)

    def complete__mode(self, text, *ignored):
        return [x for x in self.modes if x.startswith(text)]

    do__head = do__headers
    help__head = help__headers
    complete__head = complete__headers

    # width
    def do__width(self, arg):
        if arg == 'auto':
            self.auto_width = True
        elif arg == 'manual':
            self.auto_width = False
        elif '=' in arg:
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

    def help__width(self):
        print """width [col=width] [width width width ...] [auto] [manual]
set the width of a column manually"""

    complete__width = complete__noop

    # quit
    def do__quit(self, arg):
        # ask to stop
        return True

    def help__quit(self):
        print """quit
quit the shell"""

    complete__quit = complete__noop

    # reset
    def do__reset(self, arg):
        # write Ctrl+O
        sys.stdout.write('\017')
        sys.stdout.flush()

    def help__reset(self):
        print """reset
shift the terminal back into mode 1 (like the reset command line tool)"""

    complete__reset = complete__noop

    # help (mostly builtin)
    def do__help(self, arg):
        if arg.startswith('.'):
            arg = '_' + arg[1:]
        return cmd.Cmd.do_help(self, arg)

    def help__help(self):
        print """help
display help"""

    complete__help = cmd.Cmd.complete_help

def shell(db):
    'invokes a dbstore sql shell on an existing db connection'
    shell = DbShell(db)
    return shell.cmdloop()
