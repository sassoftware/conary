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


arch_files =	alpha	\
		ia64	\
		ppc	\
		ppc64	\
		s390	\
		s390x	\
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
