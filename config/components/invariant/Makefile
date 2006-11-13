#
# Copyright (c) 2006 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

filter_files =	\
	test.py \
	debuginfo.py \


dist_files = Makefile $(filter_files)

all: default-all

install:
	mkdir -p $(DESTDIR)/etc/conary/components/invariant/; \
	for f in $(filter_files); do \
		cp -a $$f $(DESTDIR)/etc/conary/components/invariant/$$f; \
	done

dist: default-dist

clean: default-clean

include ../../../Make.rules
