#
# Copyright (c) 2004-2008 rPath, Inc.
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

all: subdirs

export VERSION = 2.0.24
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

minimal:
	NO_KID=1 $(MAKE) all


SUBDIRS = commands conary config man scripts extra

extra_files = \
	LICENSE			\
	EULA_Conary.txt		\
	Make.rules 		\
	Makefile		\
	NEWS			\

dist_files = $(extra_files)

.PHONY: clean dist install subdirs

subdirs: default-subdirs

install: install-subdirs

dist:
	if ! grep "^Changes in $(VERSION)" NEWS > /dev/null 2>&1; then \
		echo "no NEWS entry"; \
		exit 1; \
	fi
	$(MAKE) forcedist

archive:
	rm -rf $(DISTDIR)
	mkdir $(DISTDIR)
	hg archive -t tbz2 -r conary-$(VERSION) conary-$(VERSION).tar.bz2

version:
	sed -i 's/@NEW@/$(VERSION)/g' NEWS
	$(MAKE) -C extra VERSION=$(VERSION)

show-version:
	@echo $(VERSION)

smoketest: archive
	@echo "=== sanity building/testing conary ==="; \
	tar jxf $(DISTDIR).tar.bz2 ; \
	cd $(DISTDIR); \
	make > /dev/null; \
	tmpdir=$$(mktemp -d); \
	make install DESTDIR=$$tmpdir > /dev/null; \
	PYTHONPATH=$$tmpdir/usr/lib/python$(PYVER)/site-packages $$tmpdir/usr/bin/conary --version > /dev/null || echo "CONARY DOES NOT WORK"; \
	PYTHONPATH=$$tmpdir/usr/lib/python$(PYVER)/site-packages $$tmpdir/usr/bin/cvc --version > /dev/null || echo "CVC DOES NOT WORK"; \
	cd -; \
	rm -rf $(DISTDIR) $$tmpdir

forcedist: $(dist_files) smoketest

tag:
	hg tag -f conary-$(VERSION)

docs:
	cd scripts; ./gendocs

clean: clean-subdirs default-clean

check: check-subdirs

ccs: dist
	cvc co --dir conary-$(VERSION) conary=conary.rpath.com@rpl:devel
	sed -i 's,version = ".*",version = "$(VERSION)",' \
                                        conary-$(VERSION)/conary.recipe;
	sed -i 's,version = '.*',version = "$(VERSION)",' \
                                        conary-$(VERSION)/conary.recipe;
	sed -i 's,r.addArchive(.*),r.addArchive("conary-$(VERSION).tar.bz2"),' \
                                        conary-$(VERSION)/conary.recipe;
	# Assume conary tip always has the patches required to build from the
	# recipe: filter out non-sqlite patches (the sqlite patch spans across
	# two lines)
	sed -i 's,r.addPatch(.*),,' conary-$(VERSION)/conary.recipe;
	cp conary-$(VERSION).tar.bz2 conary-$(VERSION)
	# This is just to prime the cache for the cook from a recipe
	bin/cvc cook --build-label conary.rpath.com@rpl:devel --prep conary=conary.rpath.com@rpl:devel
	bin/cvc cook --build-label conary.rpath.com@rpl:devel conary-$(VERSION)/conary.recipe
	rm -rf conary-$(VERSION)

include Make.rules
