#
# Copyright (c) 2004-2005 rPath, Inc.
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

from conary.lib import util

class Macros(dict):
    def __init__(self, macros={}, shadow=False):
	self.__tracked = {}
	self.__track = False
	self.__overrides = {}
        self.__callbacks = {}
	if shadow:
	    self.__macros = macros
	else:
	    self.__macros = {}
	    self.update(macros)

    def _get(self, key):
        return dict.__getitem__(self, key)
            
    def update(self, other):
        for key, item in other.iteritems():
            self[key] = item

    def setCallback(self, name, callback):
        """ Add a callback to a particular macros.  When that macro is 
            accessed, the callback function will be called with that macro's
            name as an argument 
        """
        self.__callbacks[name] = callback

    def unsetCallback(self, name):
        del self.__callbacks[name]
    
    def __setitem__(self, name, value):
	if name.startswith('_Macros'):
	    dict.__setitem__(self, name, value)
	    return
        # '.' in name reserved for getting alternative representations
        if '.' in name:
            raise MacroError, 'name "%s" contains illegal character: "."' % name
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
	if name.startswith('_Macros'):
	    return dict.__getitem__(self, name)
        repmethod = None
        parts = name.split('.', 1)
        if len(parts) > 1:
            repmethod = parts[1]
            name = parts[0]
        if name in self.__callbacks:
            self.__callbacks[name](name)
	if name in self.__overrides:
	    return self.__repmethod(self.__overrides[name], repmethod)
	if not name in self:
	    # update on access
	    # okay for this to fail bc of no __macros
	    # -- equivalent to missing dict value
            if name not in self.__macros:
                raise KeyError, 'Unknown macro "%s" - check for spelling mistakes' % name
	    value = self.__macros[name]
	    self[name] = value
	    return self.__repmethod(value, repmethod)
	else:
	    return self.__repmethod(dict.__getitem__(self, name) % self, repmethod)

    def __repmethod(self, name, repmethod):
        if repmethod is None:
            return name
        if repmethod == 'literalRegex':
            return util.literalRegex(name)
        # should not be reached
        raise MacroError, 'unknown representation method %s for %s' %(repmethod, name)
    
    def __getattr__(self, name):
	return self.__getitem__(name)

    def trackChanges(self, flag=True):
	self.__track = flag

    def getTrackedChanges(self):
	return self.__tracked.keys()
    
    def copy(self, shadow=True):
	# shadow saves initial copying cost for a higher search cost
	return Macros(self, shadow)

    
    # occasionally it may be desirable to switch from shadowing
    # to a flattened representation
    def _flatten(self):
        if self.__macros:
            # just accessing the element will copy it to this
            # macro
            for key in self.__macros.keys():
                dummy = self[key]
            self.__macros = {}

    def __iter__(self):
        # since we are accessing every element in the parent anyway
        # just flatten hierarchy first, which greatly simplifies iterating 
        self._flatten()
        # iter over self and parents
        for key in dict.__iter__(self):
            if not key.startswith('_Macros'):
                yield key

    def iterkeys(self):
        for key in self.__iter__():
            yield key

    def iteritems(self):
        for key in self.__iter__():
            yield (key, self[key])

    def keys(self):
        return [ x for x in self.__iter__() ]


class MacroError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __repr__(self):
	return self.msg

    def __str__(self):
	return repr(self)
