#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
