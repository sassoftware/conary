#
# Copyright (c) 2004-2005 rPath, Inc.
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

all: subdirs conary-wrapper constants.py

export VERSION = 0.62.1
export TOPDIR = $(shell pwd)
export DISTDIR = $(TOPDIR)/conary-$(VERSION)
export prefix = /usr
export bindir = $(prefix)/bin
export libdir = $(prefix)/lib
export mandir = $(prefix)/share/man
export conarydir = $(libdir)/python$(PYVERSION)/site-packages/conary
export conarylibdir = $(libdir)/conary

SUBDIRS=build local repository server lib pysqlite3 deps scripts config web

python_files = __init__.py	\
	branch.py		\
	callbacks.py		\
	changelog.py		\
	checkin.py		\
	clone.py		\
	commit.py		\
	conary.py		\
	conarycfg.py		\
	conaryclient.py		\
	constants.py		\
	cvc.py			\
        cvcdesc.py              \
	cscmd.py		\
	datastore.py		\
	display.py		\
	files.py		\
	flavorcfg.py		\
        fmtroves.py             \
        metadata.py             \
	queryrep.py		\
	rollbacks.py		\
	rpmhelper.py		\
        showchangeset.py        \
	streams.py		\
	trove.py		\
	updatecmd.py		\
	verify.py		\
	versions.py

bin_files = conary cvc cvcdesc
extra_files = \
	LICENSE			\
	Make.rules 		\
	Makefile		\
	NEWS			\
	conary-wrapper.in	\
	conary.1		\
	cvc.1			\
        cvcdesc.1               \
	constants.py.in

dist_files = $(python_files) $(bin_files) $(extra_files)

generated_files = conary-wrapper *.pyo *.pyc 

.PHONY: clean dist install subdirs

subdirs: default-subdirs

conary-wrapper: conary-wrapper.in Makefile
	sed s,@conarydir@,$(conarydir),g $< > $@
	chmod 755 $@

constants.py: constants.py.in Makefile
	sed s,@version@,$(VERSION),g $< > $@

install-mkdirs:
	mkdir -p $(DESTDIR)$(bindir)
	mkdir -p $(DESTDIR)$(mandir)/man1

install: all install-mkdirs install-subdirs pyfiles-install
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"
	$(PYTHON) -OO -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"
	install -m 755 conary-wrapper $(DESTDIR)$(bindir)
	for f in $(bin_files) rpm2cpio yuck; do \
		ln -sf conary-wrapper $(DESTDIR)$(bindir)/$$f; \
	done
	install -m 644 conary.1 $(DESTDIR)$(mandir)/man1
	install -m 644 cvc.1 $(DESTDIR)$(mandir)/man1
	install -m 644 cvcdesc.1 $(DESTDIR)$(mandir)/man1

dist: $(dist_files)
	if ! grep "^Changes in $(VERSION)" NEWS > /dev/null 2>&1; then \
		echo "no NEWS entry"; \
		exit 1; \
	fi
	rm -rf $(DISTDIR)
	mkdir $(DISTDIR)
	for d in $(SUBDIRS); do make -C $$d DIR=$$d dist || exit 1; done
	for f in $(dist_files); do \
		mkdir -p $(DISTDIR)/`dirname $$f`; \
		cp -a $$f $(DISTDIR)/$$f; \
	done; \
	tar cjf $(DISTDIR).tar.bz2 `basename $(DISTDIR)`
	@echo "=== sanity building/testing conary ==="; \
	cd $(DISTDIR); \
	make > /dev/null; \
	./conary > /dev/null || echo "CONARY DOES NOT WORK"; \
	cd -; \
	rm -rf $(DISTDIR)

distcheck: dist

clean: clean-subdirs default-clean
	rm -f _sqlite.so _sqlite3.so
	rm -rf sqlite sqlite3

tag:
	cvs tag conary-`echo $(VERSION) | sed 's/\./_/g'`

force-tag:
	cvs tag -F conary-`echo $(VERSION) | sed 's/\./_/g'`

include Make.rules
