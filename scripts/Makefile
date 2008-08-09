#
# Copyright (c) 2005,2007 rPath, Inc.
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

python_files = conarybugz.py

extra_dist = Makefile \
             logaction.py changemail.py commitaction \
	     perlreqs.pl findmissingbuildreqs

bin_scripts = rpm2cpio dbsh conary-debug ccs2tar
util_scripts = dumpcontainer localupdateinfo mirror md5pw showchangeset logcat listcachedir

dist_files = $(python_files) $(extra_dist) $(bin_scripts) $(util_scripts)

SUBDIRS	= migration

all: default-subdirs default-all

install: install-subdirs
	# see conary-wrapper symlink in ../Makefile to make scripts
	# available in $(bindir)
	install -m 755 $(bin_scripts) $(DESTDIR)$(bindir)
	mkdir -p $(DESTDIR)$(datadir)/conary
	install -m 755 $(util_scripts) $(DESTDIR)$(datadir)/conary
	install -m 755 conarybugz.py $(DESTDIR)$(conarydir)
	install -m 755 commitaction $(DESTDIR)$(conarydir)
	install -m 755 changemail.py $(DESTDIR)$(conarydir)
	install -m 755 logaction.py $(DESTDIR)$(conarydir)
	mkdir -p $(DESTDIR)$(conarylibexecdir)
	install -m 755 perlreqs.pl $(DESTDIR)$(conarylibexecdir)

dist: default-dist

clean: default-clean

include ../Make.rules
