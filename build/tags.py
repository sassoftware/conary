#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

"""
Module implementing tag file handling
"""

import conarycfg
import filter
import os

EXCLUDE, INCLUDE = range(2)

class TagFile(conarycfg.ConfigFile):

    def __init__(self, filename, macros):
	self.defaults = {
	    'file'		: '',
	    'name'		: '', 
	    'description'	: '',
	    'implements'	: [ conarycfg.STRINGLIST, [] ],
	    'exclude'		: [ conarycfg.CALLBACK, self.filterCB ],
	    'include'		: [ conarycfg.CALLBACK, self.filterCB ],
	}
	self.tag = os.path.basename(filename)
	self.macros = macros
	self.filterlist = []
	conarycfg.ConfigFile.__init__(self)
	self.read(filename)

    def filterCB(self, type, key=None, val=None):
	if not self.macros:
	    # empty dictionary passed in from install side, do not
	    # care about callbacks because this is only used on
	    # build side
	    return
	if type == 'display':
	    # I do not think we ever need to display, but if we do we
	    # can fix this
	    return
	elif type == 'set':
	    if key == 'exclude':
		keytype = EXCLUDE
	    elif key == 'include':
		keytype = INCLUDE
	    self.filterlist.append((keytype, filter.Filter(val, self.macros)))

    def match(self, filename):
	for keytype, filter in self.filterlist:
	    if filter.match(filename):
		if keytype == EXCLUDE:
		    return False
		else:
		    return True
	return False
