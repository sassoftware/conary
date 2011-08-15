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


template_files = \
	default \
	rpath

dist_files = Makefile $(template_files)

all: default-all

install:
	mkdir -p $(DESTDIR)/etc/conary/recipeTemplates/; \
	for f in $(template_files); do \
		cp -a $$f $(DESTDIR)/etc/conary/recipeTemplates/$$f; \
	done

dist: default-dist

clean: default-clean

include ../../Make.rules
