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


TOPDIR=..

bin_files = \
	conary		\
	cvc

extra_dist = Makefile conary-wrapper

dist_files = $(bin_files) $(extra_dist)

all: default-all
	mkdir -p $(TOPDIR)/bin
	$(SED) -e 's#/usr/bin/python#$(PYTHON)#g' conary-wrapper >$(TOPDIR)/bin/conary-wrapper
	chmod 0755 $(TOPDIR)/bin/conary-wrapper
	for f in $(bin_files); do \
		ln -sf conary-wrapper $(TOPDIR)/bin/$$f; \
	done

install-mkdirs:
	mkdir -p $(DESTDIR)$(bindir)

install: install-mkdirs
	for f in $(bin_files); do \
		install -m755 $$f $(DESTDIR)$(bindir)/$$f; \
	done

dist: default-dist

clean: default-clean
	rm -rf $(TOPDIR)/bin


include $(TOPDIR)/Make.rules
