#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed with the whole that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import deps
import os

def flags_i686():
    ofInterest = { "cmov"   : True,
		   "mmx"    : True,
		   "sse"    : True,
		   "sse2"   : True,
		   "3dnow"  : True }
    try:
        lines = open("/proc/cpuinfo").read().split("\n")
    except IOError:
        lines=[]
    rc = [ 'i686' ]
    for line in lines:
	if not line.startswith("flags"): continue
	fields = line.split()
	if fields[0] != "flags": continue

	for flag in fields[2:]:
	    if ofInterest.has_key(flag): rc.append(flag)

	return deps.Dependency('x86', rc)

    return deps.Dependency('x86')

def flags_mips64():
    return deps.Dependency('mipseb', [ 'mips64' ])

def current():
    return currentArch

def any():
    return "any"

def canInstall(other):
    return other == "any" or other == currentArch

baseArch = os.uname()[4]
localNamespace = locals()
if localNamespace.has_key("flags_" + baseArch):
    currentArch = localNamespace["flags_" + baseArch]()
else:
    currentArch = deps.Dependency(baseArch)

del localNamespace
