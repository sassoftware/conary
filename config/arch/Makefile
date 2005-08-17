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

arch_files =	alpha	\
		ia64	\
		ppc	\
		ppc64	\
		s390	\
		sparc	\
		sparc64	\
		x86	\
		x86_64

dist_files = Makefile $(arch_files)

all: default-all

install:
	mkdir -p $(DESTDIR)/etc/conary/arch/; \
	for f in $(arch_files); do \
		cp -a $$f $(DESTDIR)/etc/conary/arch/$$f; \
	done

dist: default-dist

clean: default-clean

include ../../Make.rules
