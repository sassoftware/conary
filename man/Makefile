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


man_files = \
	conary.1	\
	cvc.1		\

extra_dist = Makefile

dist_files = $(man_files) $(extra_dist)

all: default-all

install-mkdirs:
	mkdir -p $(DESTDIR)$(mandir)/man1

install: install-mkdirs
	install -m 644 conary.1 $(DESTDIR)$(mandir)/man1
	install -m 644 cvc.1 $(DESTDIR)$(mandir)/man1

dist: default-dist

clean: default-clean

include ../Make.rules
