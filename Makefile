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

source_rev=master

default: stage

add_files=*.html *.js *.inv _sources _static revision.txt

stage:
	rm -rf source
	git rm -rf --ignore-unmatch ${add_files}
	rm -rf ${add_files}
	git rev-parse ${source_rev} >revision.txt
	git archive --prefix=source/ ${source_rev} | tar -x
	make -C source/doc html
	mv source/doc/_build/html/* .
	rm -rf source
	git add ${add_files}

publish: stage
	git commit -m "Build revision $(shell cat revision.txt)"
	git push origin HEAD
