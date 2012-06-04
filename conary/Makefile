#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


all: subdirs constants.py stubs

SUBDIRS=build local repository server lib pysqlite3 deps web \
        conaryclient dbstore ScanDeps cmds

# These are stubs that must be maintained for as long as there is
# active code that uses any of these modules that needs to work
# against conary 2.1 or earlier.  If in doubt, leave these here.
compat_modules=commit.py cscmd.py fmtroves.py metadata.py \
	query.py queryrep.py rollbacks.py showchangeset.py \
	updatecmd.py verify.py branch.py clone.py

# constants.py, cvc.py, and compat_modules have to be listed
# explicitly here because they are generated and therefore might
# not exist when the wildcard is run
python_files = $(wildcard *.py) constants.py cvc.py $(compat_modules)

extra_files = \
	Makefile		\
	constants.py.in

dist_files = $(python_files) $(extra_files)

generated_files = *.pyo *.pyc 

.PHONY: clean dist install subdirs stubs

stubs:
	for cmd in $(compat_modules); do \
	  echo "from conary.cmds.$$(basename $${cmd} .py) import *" > $$cmd ; \
	done; \
	echo 'from conary.cmds.cvccmd import *' > cvc.py

subdirs: default-subdirs

constants.py: constants.py.in ../Makefile
	$(SED) -e s,@version@,$(VERSION),g -e s,@changeset@,$(CHANGESET),g $< > $@

install: all install-subdirs pyfiles-install constants.py
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"
	$(PYTHON) -OO -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"

dist: default-dist

clean: clean-subdirs default-clean
	rm -f _sqlite.so _sqlite3.so
	rm -rf sqlite sqlite3
	rm -f constants.py
	rm -rf *.egg-info
	rm -f $(compat_modules) cvc.py

include ../Make.rules
