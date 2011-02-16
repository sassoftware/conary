#
# Copyright (c) 2011 rPath, Inc.
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

.PHONY: all dist clean install

sqlite_dir = sqlite-*
sqlite_lib = $(sqlite_dir)/.libs/libsqlite3.a
sqlite_inc = $(sqlite_dir)/sqlite3*.h

all:
	if [ -f $(sqlite_lib) ]; then \
		rm -rf embed; \
		mkdir embed; \
		cp $(sqlite_lib) $(sqlite_inc) embed/; \
	fi
	$(PYTHON) setup.py build
	$(PYTHON) setup.py install --install-lib=..

install:
	$(PYTHON) setup.py install --install-lib=$(DESTDIR)/$(conarydir)

clean:
	rm -rf build dist embed
	$(PYTHON) setup.py clean

dist: clean
	mkdir -p $(DISTDIR)/$(DIR)
	tar cSpf - --exclude CVS --exclude build --exclude dist . | tar xSpf - -C $(DISTDIR)/$(DIR) 

include ../../Make.rules
