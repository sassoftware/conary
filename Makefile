#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

VERSION = 0.1
distdir = srs-$(VERSION)
prefix = $(DESTDIR)/usr
srsdir = $(prefix)/share/srs
bindir = $(prefix)/bin

python_files = __init__.py	\
	build.py		\
	buildpackage.py		\
	database.py		\
	changeset.py		\
	commit.py		\
	cook.py			\
	datastore.py		\
	destdirpolicy.py	\
	display.py		\
	filecontainer.py	\
	files.py		\
	fixedglob.py		\
	importrpm.py		\
	lookaside.py		\
	package.py		\
	policy.py		\
	recipe.py		\
	repository.py		\
	rollbacks.py		\
	rpmhelper.py		\
	sha1helper.py		\
	srscfg.py		\
	update.py		\
	util.py			\
	versioned.py		\
	versions.py		\
	use.py

example_files = examples/tmpwatch.recipe
bin_files = srs srs-bootstrap
extra_files = srs.recipe.in srs.recipe srs-wrapper.in Makefile
dist_files = $(python_files) $(example_files) $(bin_files) $(extra_files)

generated_files = srs-wrapper srs.recipe *.pyo *.pyc 

.PHONY: clean bootstrap deps.dot pychecker dist install

all: srs-wrapper srs.recipe

srs-wrapper: srs-wrapper.in
	sed s,@srsdir@,$(srsdir),g $< > $@
	chmod 755 $@

srs.recipe: srs.recipe.in
	sed s,@VERSION@,$(VERSION),g $< > $@

install: all
	mkdir -p $(srsdir) $(bindir)
	for f in $(python_files) $(bin_files); do \
		cp -a $$f $(srsdir)/$$f; \
	done
	install -m 755 srs-wrapper $(bindir)
	for f in $(bin_files); do \
		ln -sf srs-wrapper $(bindir)/$$f; \
	done

dist:
	rm -rf $(distdir)
	mkdir $(distdir)
	for f in $(dist_files); do \
		mkdir -p $(distdir)/`dirname $$f`; \
		cp -a $$f $(distdir)/$$f; \
	done
	tar cjf $(distdir).tar.bz2 $(distdir)
	rm -rf $(distdir)

clean:
	rm -f *~ .#* $(generated_files)

bootstrap:
	@if ! [ -d /opt/ -a -w /opt/ ]; then \
		echo "/opt isn't writable, this won't work"; \
		exit 1; \
	fi
	time ./srs-bootstrap `find ../recipes/ -name "cross*.recipe" -o -name "bootstrap*.recipe"`

bootstrap-continue:
	@if ! [ -d /opt/ -a -w /opt/ ]; then \
		echo "/opt isn't writable, this won't work"; \
		exit 1; \
	fi
	time ./srs-bootstrap `find ../recipes/ -name "cross*.recipe" -o -name "bootstrap*.recipe"` --skip=$$(for i in `./srs replist | cut -d: -f 1 | sort | uniq`; do echo -n $$i,; done)


deps.dot:
	./srs-bootstrap --dot `find ../recipes/ -name "cross*.recipe" -o -name "bootstrap*.recipe"` > deps.dot

pychecker:
	pychecker *.py