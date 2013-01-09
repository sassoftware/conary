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


site_files =	x86	\
		x86_64	\
		linux	

dist_files = Makefile $(site_files)
# not sizeof.c or sizeof

all: default-all

install:
	mkdir -p $(DESTDIR)/etc/conary/site/; \
	for f in $(site_files); do \
		cp -a $$f $(DESTDIR)/etc/conary/site/$$f; \
	done

dist: default-dist

clean: default-clean
	rm -f sizeof

include ../../Make.rules
