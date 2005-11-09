#
# Copyright (c) 2004 rPath, Inc.
# All rights reserved
#

.PHONY: all dist clean install

all:
	$(PYTHON) setup.py build
	$(PYTHON) setup.py install --install-lib=..

install:
	$(PYTHON) setup.py install --install-lib=$(DESTDIR)/$(conarydir)

clean:
	rm -rf build dist
	$(PYTHON) setup.py clean

dist: clean
	mkdir -p $(DISTDIR)/$(DIR)
	tar cSpf - --exclude CVS --exclude build --exclude dist . | tar xSpf - -C $(DISTDIR)/$(DIR) 

include ../../Make.rules
