#
# Copyright (c) 2005 rPath, Inc.
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

extra_dist = Makefile rpm2cpio changemail changemail.py commitaction conary-debug perlreqs.pl dbsh dumpcontainer showchangeset mirror

dist_files = $(python_files) $(extra_dist)

SUBDIRS	= migration

all: default-subdirs default-all

install:
	# see conary-wrapper symlink in ../Makefile to make scripts
	# available in $(bindir)
	install -m 755 dbsh $(DESTDIR)$(bindir)
	install -m 755 rpm2cpio $(DESTDIR)$(bindir)
	install -m 755 conary-debug $(DESTDIR)$(bindir)
	install -m 755 conarybugz.py $(DESTDIR)$(conarydir)
	install -m 755 commitaction $(DESTDIR)$(conarydir)
	install -m 755 changemail $(DESTDIR)$(conarydir)
	install -m 755 changemail.py $(DESTDIR)$(conarydir)
	mkdir -p $(DESTDIR)$(datadir)/conary
	install -m 755 dumpcontainer $(DESTDIR)$(datadir)/conary
	install -m 755 mirror $(DESTDIR)$(datadir)/conary
	install -m 755 showchangeset $(DESTDIR)$(datadir)/conary
	mkdir -p $(DESTDIR)$(conarylibexecdir)
	install -m 755 perlreqs.pl $(DESTDIR)$(conarylibexecdir)

dist: default-dist

clean: default-clean

include ../Make.rules
