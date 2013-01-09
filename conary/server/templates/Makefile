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


kid_files = $(wildcard *.kid)

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
