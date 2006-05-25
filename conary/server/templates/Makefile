#
# Copyright (c) 2005 rPath, Inc.
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

kid_files =	add_group.kid		\
		add_user.kid 		\
		browse.kid		\
		change_password.kid	\
		choose_branch.kid	\
		error.kid		\
		files.kid		\
		group_contents.kid	\
		library.kid		\
		metadata.kid		\
		notice.kid		\
		permission.kid		\
		pick_trove.kid		\
		trove_info.kid		\
		user_admin.kid		\
		pgp_admin.kid		\
		pgp_submit_key.kid      \
		pgp_get_key.kid

extra_dist = Makefile

python_files = __init__.py

dist_files = $(python_files) $(extra_dist) $(kid_files)

all:
	if [ -z "$(NO_KID)" ] ; then kidc --strip-dest-dir= $(kid_files); fi

install: pyfiles-install kidfiles-install

kidfiles-install:
	cp -a $(kid_files) $(DESTDIR)/$(sitedir)/$(DIR)/; \
	kidc --strip-dest-dir $(DESTDIR) $(DESTDIR)/$(sitedir)/$(DIR)/ ||:

dist: default-dist

clean: default-clean

include ../../../Make.rules
