#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

VERSION = 0.1
distdir = srs-$(VERSION)
prefix = /usr
srsdir = $(prefix)/share/srs
bindir = $(prefix)/bin
PYTHON = python2.3

python_files = __init__.py	\
	branch.py		\
	build.py		\
	buildpackage.py		\
	database.py		\
	checkin.py		\
	changeset.py		\
	commit.py		\
	cook.py			\
	datastore.py		\
	destdirpolicy.py	\
	display.py		\
	enum.py			\
	filecontainer.py	\
	filecontents.py		\
	files.py		\
	fixedglob.py		\
	fsrepos.py		\
	helper.py		\
	importrpm.py		\
	localrep.py		\
	log.py			\
	lookaside.py		\
	package.py		\
	packagepolicy.py	\
	patch.py		\
	policy.py		\
	recipe.py		\
	repository.py		\
	rollbacks.py		\
	rpmhelper.py		\
	sha1helper.py		\
	srcctl.py		\
	srs.py			\
	srscfg.py		\
	trovedb.py		\
	update.py		\
	updatecmd.py		\
	util.py			\
	versioned.py		\
	versions.py		\
	use.py

example_files = examples/tmpwatch.recipe
bin_files = srs srs-bootstrap
extra_files = srs.recipe.in srs.recipe srs-wrapper.in Makefile test/*.py
dist_files = $(python_files) $(example_files) $(bin_files) $(extra_files)

generated_files = srs-wrapper srs.recipe *.pyo *.pyc 

.PHONY: clean bootstrap deps.dot pychecker dist install test debug-test

all: srs-wrapper srs.recipe

srs-wrapper: srs-wrapper.in
	sed s,@srsdir@,$(srsdir),g $< > $@
	chmod 755 $@

srs.recipe: srs.recipe.in
	sed s,@VERSION@,$(VERSION),g $< > $@

install: all
	mkdir -p $(DESTDIR)$(srsdir) $(DESTDIR)$(bindir)
	for f in $(python_files) $(bin_files); do \
		cp -a $$f $(DESTDIR)$(srsdir)/$$f; \
	done
	$(PYTHON) -c "import compileall; compileall.compile_dir('$(DESTDIR)$(srsdir)', ddir='$(srsdir)', quiet=1)"
	$(PYTHON) -OO -c "import compileall; compileall.compile_dir('$(DESTDIR)$(srsdir)', ddir='$(srsdir)', quiet=1)"
	install -m 755 srs-wrapper $(DESTDIR)$(bindir)
	for f in $(bin_files); do \
		ln -sf srs-wrapper $(DESTDIR)$(bindir)/$$f; \
	done

dist: $(dist_files)
	rm -rf $(distdir)
	mkdir $(distdir)
	for f in $(dist_files); do \
		mkdir -p $(distdir)/`dirname $$f`; \
		cp -a $$f $(distdir)/$$f; \
	done
	tar cjf $(distdir).tar.bz2 $(distdir)
	rm -rf $(distdir)

distcheck:
	@echo Possible missing files:
	@(ls *py; for f in $(python_files); do echo $$f; done) | sort | uniq -u

clean:
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
