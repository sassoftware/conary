#
# Copyright (c) 2004-2005 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Module implementing tag file handling
"""
import os

from conary.build import filter
from conary.lib.cfg import *

EXCLUDE, INCLUDE = range(2)

class CfgImplementsItem(CfgEnum):
    validValueDict = {'files':   ('update', 'preremove', 'remove',
                                  'preupdate'),
                      'handler': ('update', 'preremove'),
		      'description':  ('update', 'preremove')}

    def __init__(self):
        validValues = []
        for fileType, actionList in self.validValueDict.iteritems():
            validValues.extend(' '.join((fileType, x)) for x in actionList)
        self.validValues = validValues
        CfgEnum.__init__(self)

    def checkEntry(self, val):
	if val.find(" ") < 0:
            raise ParseError, \
                'missing type/action in "implements %s"' %val
        CfgEnum.checkEntry(self, val)
        # XXX missing check for description here

CfgImplements = CfgList(CfgImplementsItem)
        

class CfgDataSource(CfgEnum):
    validValues = ['args', 'stdin', 'multitag' ]


class TagFile(ConfigFile):
    def filterCB(self, val, key):
        if not self.macros:
            return
        if key == 'exclude':
            keytype = EXCLUDE
        elif key == 'include':
            keytype = INCLUDE
        self.filterlist.append((keytype, filter.Filter(val, self.macros)))


    file              = CfgString
    name              = CfgString
    description       = CfgString
    datasource        = (CfgDataSource, 'args')
    implements        = CfgImplements
  
    def __init__(self, filename, macros = {}, warn=False):
	ConfigFile.__init__(self)
        self.addConfigOption('include', CfgCallBack(self.filterCB, 'include'))
        self.addConfigOption('exclude', CfgCallBack(self.filterCB, 'exclude'))

	self.tag = os.path.basename(filename)
	self.tagFile = filename
	self.macros = macros
	self.filterlist = []
	self.read(filename, exception=True)
	if 'implements' in self.__dict__:
	    for item in self.__dict__['implements']:
		if item.find(" ") < 0:
		    raise ParseError, \
			'missing type/action in "implements %s"' %item
		key, val = item.split(" ")
                # deal with self->handler protocol change
                if key == 'description':
                    if warn:
                        # at cook time
                        raise ParseError, \
                            'change "implements %s" to "implements handler" in %s' % (key, filename)
                    # throw this away
                    continue


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
        # ignore hidden files
        if path.startswith('.'):
            continue
	c = TagFile(os.path.join(dirPath, path))
	d[c.tag] = c

    return d
