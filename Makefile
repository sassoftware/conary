#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

all: subdirs srs-wrapper srs.recipe

export VERSION = 0.1
export TOPDIR = $(shell pwd)
export DISTDIR = $(TOPDIR)/srs-$(VERSION)
export prefix = /usr
export srsdir = $(prefix)/share/srs
export bindir = $(prefix)/bin
export PYTHON = python2.3

SUBDIRS=build local repository test lib

subdirs_rule=

python_files = __init__.py	\
	branch.py		\
	checkin.py		\
	commit.py		\
	cook.py			\
	cscmd.py		\
	datastore.py		\
	display.py		\
	enum.py			\
	filecontainer.py	\
	files.py		\
	helper.py		\
	importrpm.py		\
	log.py			\
	package.py		\
	patch.py		\
	rollbacks.py		\
	rpmhelper.py		\
	sha1helper.py		\
	srcctl.py		\
	srscfg.py		\
	srs.py			\
	updatecmd.py		\
	util.py			\
	versioned.py		\
	versions.py

example_files = examples/tmpwatch.recipe
bin_files = srs srs-bootstrap
extra_files = srs.recipe.in srs.recipe srs-wrapper.in Makefile Make.rules
dist_files = $(python_files) $(example_files) $(bin_files) $(extra_files)

generated_files = srs-wrapper srs.recipe *.pyo *.pyc 

.PHONY: clean bootstrap deps.dot pychecker dist install test debug-test subdirs


subdirs:
	for d in $(SUBDIRS); do make -C $$d DIR=$$d || exit 1; done

srs-wrapper: srs-wrapper.in
	sed s,@srsdir@,$(srsdir),g $< > $@
	chmod 755 $@

srs.recipe: srs.recipe.in
	sed s,@VERSION@,$(VERSION),g $< > $@

install-mkdirs:
	mkdir -p $(DESTDIR)$(bindir)

install-subdirs:
	for d in $(SUBDIRS); do make -C $$d DIR=$$d install || exit 1; done

install: all install-mkdirs install-subdirs pyfiles-install
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)$(srsdir)', ddir='$(srsdir)', quiet=1)"
	$(PYTHON) -OO -c "import compileall; compileall.compile_dir('$(DESTDIR)$(srsdir)', ddir='$(srsdir)', quiet=1)"
	install -m 755 srs-wrapper $(DESTDIR)$(bindir)
	for f in $(bin_files); do \
		ln -sf srs-wrapper $(DESTDIR)$(bindir)/$$f; \
	done

dist: $(dist_files)
	rm -rf $(DISTDIR)
	mkdir $(DISTDIR)
	for d in $(SUBDIRS); do make -C $$d DIR=$$d dist || exit 1; done
	for f in $(dist_files); do \
		mkdir -p $(DISTDIR)/`dirname $$f`; \
		cp -a $$f $(DISTDIR)/$$f; \
	done
	tar cjf $(DISTDIR).tar.bz2 srs-$(VERSION)
	rm -rf $(DISTDIR)

distcheck:
	@echo Possible missing files:
	@(ls *py; for f in $(python_files); do echo $$f; done) | sort | uniq -u

clean:
	for d in $(SUBDIRS); do make -C $$d DIR=$$d clean || exit 1; done
	rm -f *~ .#* $(generated_files)

bootstrap:
	@if ! [ -d /opt/ -a -w /opt/ ]; then \
		echo "/opt isn't writable, this won't work"; \
		exit 1; \
	fi
	time $(PYTHON) ./srs-bootstrap --bootstrap group-bootstrap

bootstrap-continue:
	@if ! [ -d /opt/ -a -w /opt/ ]; then \
		echo "/opt isn't writable, this won't work"; \
		exit 1; \
	fi
	time $(PYTHON) ./srs-bootstrap --bootstrap --onlyunbuilt group-bootstrap


deps.dot:
	$(PYTHON) ./srs-bootstrap --dot `find ../recipes/ -name "cross*.recipe" -o -name "bootstrap*.recipe"` > deps.dot

pychecker:
	$(PYTHON) /usr/lib/python2.2/site-packages/pychecker/checker.py *.py

test:
	make -C test $@

debug-test:
	make -C test $@

include Make.rules
