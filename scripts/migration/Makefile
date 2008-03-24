#
# Copyright (c) 2005-2007 rPath, Inc.
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

python_files = schema.py db2db.py database.py tablelist.py

extra_dist = Makefile

dist_files = $(python_files) $(extra_dist)

SUBDIRS	=

all: default-subdirs default-all

install:
	mkdir -p $(DESTDIR)$(datadir)/conary/migration
	install -m 755 $(python_files) $(DESTDIR)$(datadir)/conary/migration

dist: default-dist

clean: default-clean

include ../../Make.rules
