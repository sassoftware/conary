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

extra_inst=	add_user.kid 		\
		change_password.kid	\
		choose_branch.kid	\
		error.kid		\
		library.js		\
		library.kid		\
		main_page.kid		\
		metadata.kid		\
		notice.kid		\
		permission.kid		\
		pick_trove.kid		\
		style.css		\
		user_admin.kid

python_files =	__init__.py

dist_files = $(python_files) $(extra_inst)

all: default-all

install: install-subdirs pyfiles-install

dist: default-dist

clean: default-clean

include ../../Make.rules
