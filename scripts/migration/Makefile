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


python_files = schema.py db2db.py database.py tablelist.py

extra_dist = Makefile

dist_files = $(python_files) $(extra_dist)

SUBDIRS	=

all: default-subdirs default-all

install:
	mkdir -p $(DESTDIR)$(datadir)/conary/migration
	install -m 755 $(python_files) $(DESTDIR)$(datadir)/conary/migration

dist: default-dist

clean: default-clean

include ../../Make.rules
