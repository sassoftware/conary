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

dist_files = \
	conary.1	\
	cvc.1		\
	cvcdesc.1

install-mkdirs:
	mkdir -p $(DESTDIR)$(mandir)/man1

install: install-mkdirs
	install -m 644 conary.1 $(DESTDIR)$(mandir)/man1
	install -m 644 cvc.1 $(DESTDIR)$(mandir)/man1
	install -m 644 cvcdesc.1 $(DESTDIR)$(mandir)/man1
