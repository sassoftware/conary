#
# Copyright (c) 2005 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

python_files = conarybugz.py

extra_dist = Makefile rpm2cpio yuck yuck.1 convertcontents

dist_files = $(python_files) $(extra_dist)

all: default-all

install:
	# see conary-wrapper symlink in ../Makefile to make scripts
	# available in $(bindir)
	install -m 755 rpm2cpio $(DESTDIR)$(conarydir)
	install -m 755 yuck $(DESTDIR)$(conarydir)
	install -m 644 yuck.1 $(DESTDIR)$(mandir)/man1
	install -m 755 convertcontents $(DESTDIR)$(conarydir)
	install -m 755 conarybugz.py $(DESTDIR)$(conarydir)

dist: default-dist

clean: default-clean

include ../Make.rules
