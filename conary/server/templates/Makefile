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


kid_files = $(wildcard *.kid)

extra_dist = Makefile

python_files = __init__.py

dist_files = $(python_files) $(extra_dist) $(kid_files)

all:
	if [ -z "$(NO_KID)" ] ; then kidc --strip-dest-dir= $(kid_files); fi

install: pyfiles-install kidfiles-install

kidfiles-install:
	cp -a $(kid_files) $(DESTDIR)/$(sitedir)/$(DIR)/; \
	kidc --strip-dest-dir $(DESTDIR) $(DESTDIR)/$(sitedir)/$(DIR)/ ||:

dist: default-dist

clean: default-clean

include ../../../Make.rules
