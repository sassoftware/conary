#
# Copyright (c) 2004-2006 rPath, Inc.
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

all: subdirs constants.py

SUBDIRS=build local repository server lib pysqlite3 deps web \
        conaryclient dbstore ScanDeps

python_files = __init__.py	\
	branch.py		\
	callbacks.py		\
	changelog.py		\
	checkin.py		\
	clone.py		\
	commit.py		\
	conarycfg.py		\
	constants.py		\
	cvc.py			\
        cvcdesc.py              \
	cscmd.py		\
	display.py		\
	errors.py		\
	files.py		\
	flavorcfg.py		\
        fmtroves.py             \
        metadata.py             \
	query.py		\
	queryrep.py		\
	rollbacks.py		\
	rpmhelper.py		\
        showchangeset.py        \
	state.py		\
	streams.py		\
	trove.py		\
	updatecmd.py		\
	verify.py		\
	versions.py

extra_files = \
	Makefile		\
	constants.py.in

dist_files = $(python_files) $(extra_files)

generated_files = *.pyo *.pyc 

.PHONY: clean dist install subdirs

subdirs: default-subdirs

constants.py: constants.py.in ../Makefile
	sed s,@version@,$(VERSION),g $< > $@

install: all install-subdirs pyfiles-install
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"
	$(PYTHON) -OO -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"

dist: default-dist

clean: clean-subdirs default-clean
	rm -f _sqlite.so _sqlite3.so
	rm -rf sqlite sqlite3
	rm -f constants.py

include ../Make.rules
