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


.PHONY: all dist clean install

sqlite_dir = $(wildcard sqlite-*)
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
