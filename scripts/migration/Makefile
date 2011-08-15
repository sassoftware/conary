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
