#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

all: subdirs conary-wrapper

export VERSION = 0.2
export TOPDIR = $(shell pwd)
export DISTDIR = $(TOPDIR)/conary-$(VERSION)
export prefix = /usr
export conarydir = $(prefix)/share/conary
export bindir = $(prefix)/bin
export mandir = $(prefix)/share/man

SUBDIRS=build local repository lib pysqlite deps

subdirs_rule=

python_files = __init__.py	\
	branch.py		\
	changelog.py		\
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
	options.py		\
	package.py		\
	patch.py		\
	queryrep.py		\
	rollbacks.py		\
	rpmhelper.py		\
	sha1helper.py		\
	srcctl.py		\
	conarycfg.py		\
	conary.py			\
	updatecmd.py		\
	util.py			\
	magic.py		\
	versions.py		\
	xmlshims.py

example_files = examples/tmpwatch.recipe
bin_files = conary
extra_files = conary-wrapper.in Makefile Make.rules conary.1 LICENSE
dist_files = $(python_files) $(example_files) $(bin_files) $(extra_files)

generated_files = conary-wrapper *.pyo *.pyc 

.PHONY: clean bootstrap deps.dot pychecker dist install subdirs


subdirs:
	for d in $(SUBDIRS); do make -C $$d DIR=$$d || exit 1; done

conary-wrapper: conary-wrapper.in
	sed s,@conarydir@,$(conarydir),g $< > $@
	chmod 755 $@

install-mkdirs:
	mkdir -p $(DESTDIR)$(bindir)
	mkdir -p $(DESTDIR)$(mandir)/man1

install: all install-mkdirs install-subdirs pyfiles-install
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"
	$(PYTHON) -OO -c "import compileall; compileall.compile_dir('$(DESTDIR)$(conarydir)', ddir='$(conarydir)', quiet=1)"
	install -m 755 conary-wrapper $(DESTDIR)$(bindir)
	for f in $(bin_files); do \
		ln -sf conary-wrapper $(DESTDIR)$(bindir)/$$f; \
	done
	install -m 644 conary.1 $(DESTDIR)$(mandir)/man1

dist: $(dist_files)
	rm -rf $(DISTDIR)
	mkdir $(DISTDIR)
	for d in $(SUBDIRS); do make -C $$d DIR=$$d dist || exit 1; done
	for f in $(dist_files); do \
		mkdir -p $(DISTDIR)/`dirname $$f`; \
		cp -a $$f $(DISTDIR)/$$f; \
	done
	tar cjf $(DISTDIR).tar.bz2 conary-$(VERSION)
	rm -rf $(DISTDIR)

clean: clean-subdirs default-clean
	rm -f _sqlite.so
	rm -rf sqlite

include Make.rules
