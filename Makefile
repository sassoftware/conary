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


all: subdirs

export TOPDIR = $(shell pwd)
export VERSION = 2.3.9
export CHANGESET = $(shell ./scripts/hg-version.sh)
export DISTDIR = $(TOPDIR)/conary-$(VERSION)
export prefix = /usr
export lib = $(shell uname -m | sed -r '/x86_64|ppc64|s390x|sparc64/{s/.*/lib64/;q};s/.*/lib/')
export bindir = $(prefix)/bin
export libdir = $(prefix)/$(lib)
export libexecdir = $(prefix)/libexec
export datadir = $(prefix)/share
export mandir = $(datadir)/man
export sitedir = $(libdir)/python$(PYVER)/site-packages/
export conarydir = $(sitedir)/conary
export conarylibdir = $(libdir)/conary
export conarylibexecdir = $(libexecdir)/conary

minimal:
	NO_KID=1 $(MAKE) all


SUBDIRS = commands conary config man scripts

extra_files = \
	COPYING			\
	EULA_Conary_Dual_License.txt		\
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
	sed -i 's/@NEW@/$(VERSION)/g' ./doc/PROTOCOL.versions

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

# Build extension (cython) output, which is checked into source control and
# thus not normally built.
ext:
	make -C conary/lib/ext ext

ext-clean:
	make -C conary/lib/ext ext-clean


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
