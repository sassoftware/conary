#
# Copyright (c) 2004-2006 rPath, Inc.
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

all: subdirs

export VERSION = 0.97
export TOPDIR = $(shell pwd)
export DISTDIR = $(TOPDIR)/conary-$(VERSION)
export prefix = /usr
export bindir = $(prefix)/bin
export libdir = $(prefix)/lib
export libexecdir = $(prefix)/libexec
export datadir = $(prefix)/share
export mandir = $(datadir)/man
export sitedir = $(libdir)/python$(PYVERSION)/site-packages/
export conarydir = $(sitedir)/conary
export conarylibdir = $(libdir)/conary
export conarylibexecdir = $(libexecdir)/conary

SUBDIRS=commands conary config man scripts

extra_files = \
	LICENSE			\
	Make.rules 		\
	Makefile		\
	NEWS			\

dist_files = $(extra_files)

.PHONY: clean dist install subdirs

subdirs: default-subdirs

install: install-subdirs

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
	./bin/conary --version > /dev/null || echo "CONARY DOES NOT WORK"; \
	cd -; \
	rm -rf $(DISTDIR)

tag:
	hg tag conary-$(VERSION)

clean: clean-subdirs default-clean
	rm -f _sqlite.so _sqlite3.so
	rm -rf sqlite sqlite3

include Make.rules
