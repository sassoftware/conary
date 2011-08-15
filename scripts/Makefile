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


extra_dist = Makefile \
             logaction.py changemail.py commitaction \
	     perlreqs.pl findmissingbuildreqs

bin_scripts = rpm2cpio dbsh conary-debug ccs2tar
util_scripts = dumpcontainer localupdateinfo mirror md5pw showchangeset logcat listcachedir recreatedb genmodel promote-redirects

dist_files = $(python_files) $(extra_dist) $(bin_scripts) $(util_scripts)

SUBDIRS	= migration

all: default-subdirs default-all

install: install-subdirs
	# see conary-wrapper symlink in ../Makefile to make scripts
	# available in $(bindir)
	install -m 755 $(bin_scripts) $(DESTDIR)$(bindir)
	mkdir -p $(DESTDIR)$(datadir)/conary
	install -m 755 $(util_scripts) $(DESTDIR)$(datadir)/conary
	install -m 755 commitaction $(DESTDIR)$(conarydir)
	mkdir -p $(DESTDIR)$(conarylibexecdir)
	install -m 755 perlreqs.pl $(DESTDIR)$(conarylibexecdir)

dist: default-dist

clean: default-clean

include ../Make.rules
