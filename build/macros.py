#
# Copyright (c) 2004 Specifix, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

"""
Module implementing the "macro" dictionary class
"""

class Macros(dict):
    def __init__(self, macros={}, shadow=False):
	self.__tracked = {}
	self.__track = False
	self.__overrides = {}
	if shadow:
	    self.__macros = macros
	else:
	    self.__macros = {}
	    self.update(macros)

    def update(self, other):
        for key, item in other.iteritems():
            self[key] = item
    
    def __setitem__(self, name, value):
	if name[:7] == '_Macros':
	    dict.__setitem__(self, name, value)
	    return
	if self.__track:
	    self.__tracked[name] = 1 
        # only expand references to ourself
        d = {name: self.get(name)}
        # escape any macros in the new value
        value = value.replace('%', '%%')
        # unescape references to ourself
        value = value.replace('%%%%(%s)s' %name, '%%(%s)s'%name)
        # expand our old value when defining the new value
 	dict.__setitem__(self, name, value % d)

    # overrides allow you to set a macro value at the command line
    # or in a config file and use it despite the value being 
    # set subsequently within the recipe
    
    def _override(self, key, value):
	self.__overrides[key] = value

    def __setattr__(self, name, value):
	self.__setitem__(name, value)

    def __getitem__(self, name):
	if name[:7] == '_Macros':
	    return dict.__getitem__(self, name)
	if name in self.__overrides:
	    return self.__overrides[name]
	if not name in self:
	    # update on access
	    # okay for this to fail bc of no __macros
	    # -- equivalent to missing dict value
	    value = self.__macros[name]
	    self[name] = value
	    return value
	else:
	    return dict.__getitem__(self, name) % self
    
    def __getattr__(self, name):
	return self.__getitem__(name)

    def trackChanges(self, flag=True):
	self.__track = flag

    def getTrackedChanges(self):
	return self.__tracked.keys()
    
    def copy(self, shadow=True):
	# shadow saves initial copying cost for a higher search cost
	return Macros(self, shadow)
