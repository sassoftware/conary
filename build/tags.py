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

    # lists all legal options for "implements"
    implementsCheck = {'files': ('update', 'preremove', 'remove'),
		       'self':  ('update', 'preremove')}
    # ...and "datasource"
    datasourceCheck = ['args', 'stdin']

    def __init__(self, filename, macros = {}):
	self.defaults = {
	    'file'		: '',
	    'name'		: '', 
	    'description'	: '',
	    'datasource'        : 'args',
	    'implements'	: [ conarycfg.STRINGLIST, [] ],
	    'exclude'		: [ conarycfg.CALLBACK, self.filterCB ],
	    'include'		: [ conarycfg.CALLBACK, self.filterCB ],
	}
	self.tag = os.path.basename(filename)
	self.tagFile = filename
	self.macros = macros
	self.filterlist = []
	conarycfg.ConfigFile.__init__(self)
	self.read(filename, exception=True)
	if 'implements' in self.__dict__:
	    for item in self.__dict__['implements']:
		if item.find(" ") < 0:
		    raise conarycfg.ParseError, \
			'missing type/action in "implements %s"' %item
		key, val = item.split(" ")
		if key not in self.implementsCheck:
		    raise conarycfg.ParseError, \
			'unknown type %s in "implements %s"' %(key, item)
		if val not in self.implementsCheck[key]:
		    raise conarycfg.ParseError, \
			'unknown action %s in "implements %s"' %(val, item)
	if 'datasource' in self.__dict__:
	    if self.__dict__['datasource'] not in self.datasourceCheck:
		raise conarycfg.ParseError, \
		    'unknown datasource option %s: "datasource args|stdin"' \
		    %self.__dict__['datasource']


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

def loadTagDict(dirPath):
    d = {}
    try:
	files = os.listdir(dirPath)
    except OSError:
	return {}

    for path in files:
	c = TagFile(os.path.join(dirPath, path))
	d[c.tag] = c

    return d
