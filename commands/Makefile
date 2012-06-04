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


TOPDIR=..

bin_files = \
	conary		\
	cvc

extra_dist = Makefile conary-wrapper

dist_files = $(bin_files) $(extra_dist)

all: default-all
	mkdir -p $(TOPDIR)/bin
	$(SED) -e 's#/usr/bin/python#$(PYTHON)#g' conary-wrapper >$(TOPDIR)/bin/conary-wrapper
	chmod 0755 $(TOPDIR)/bin/conary-wrapper
	for f in $(bin_files); do \
		ln -sf conary-wrapper $(TOPDIR)/bin/$$f; \
	done

install-mkdirs:
	mkdir -p $(DESTDIR)$(bindir)

install: install-mkdirs
	for f in $(bin_files); do \
		install -m755 $$f $(DESTDIR)$(bindir)/$$f; \
	done

dist: default-dist

clean: default-clean
	rm -rf $(TOPDIR)/bin


include $(TOPDIR)/Make.rules
