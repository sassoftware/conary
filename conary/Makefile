#
# Copyright (c) 2004-2010 rPath, Inc.
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
	sed -e s,@version@,$(VERSION),g -e s,@changeset@,$(CHANGESET),g $< > $@

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
