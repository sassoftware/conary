# Copyright (c) 2006 rPath, Inc.
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
""" Defines an on-demand importer that only actually loads modules when their
    attributes are accessed.  NOTE: if the ondemand module is viewed using
    introspection, like dir(), isinstance, etc, it will appear as a 
    ModuleProxy, not a module, and will not have the correct attributes.
    Barring introspection, however, the module will behave as normal.
"""
import sys
import imp
import os
import types

def makeImportedModule(name, data, scope):
    """ Returns a ModuleProxy that has access to a closure w/ 
        information about the module to load, but is otherwise 
        empty.  On an attempted access of any member of the module,
        the module is loaded.
    """
        
    def _loadModule():
        """ Load the given module, and insert it into the parent
            scope, and also the original importing scope.
        """
        mod = sys.modules.get(name, None)
        if mod is None or not isinstance(mod, types.ModuleType):
            mod = imp.load_module(name, *data)
            sys.modules[name] = mod

        scope[name] = mod

        frame = sys._getframe(2)
        global_scope = frame.f_globals
        local_scope = frame.f_locals

        if name in local_scope:
            if name.__class__.__name__ == 'ModuleProxy': 
                local_scope[name] = mod
        elif name in global_scope:
            if name.__class__.__name__ == 'ModuleProxy': 
                global_scope[name] = mod

        return mod

    class ModuleProxy(object):
        # we don't add any docs for the module in case the 
        # user tries accessing '__doc__'
        def __hasattr__(self, key):
            mod = _loadModule()
            return hasattr(mod, key)

        def __getattr__(self, key):
            mod = _loadModule()
            if key == '__all__':
                # the caller tried to use __all__ to implement import *.
                # Unforunately, that probably means the caller is
                # an import statement which has a handle on an import
                # object.  We can't rely on being able to overwrite that
                # variable, so just update the proxy's dict.
                self.__dict__.update(mod.__dict__)
            return getattr(mod, key)

        def __setattr__(self, key, value):
            mod = _loadModule()
            return setattr(mod, key, value)

        def __repr__(self):
            return "<moduleProxy '%s' from '%s'>" % (name, data[1])

    return ModuleProxy()

class OnDemandLoader(object):
    """ The loader takes a name and info about the module to load and 
        "loads" it - in this case returning loading a proxy that 
        will only load the class when an attribute is accessed.
    """
    def __init__(self, name, data, scope):
        self.name = name
        self.data = data
        self.scope = scope
        
    def load_module(self, fullname):
	if fullname in __builtins__:
            mod = imp.load_module(self.name, *self.data)
	    sys.modules[fullname] = mod
        else:
            mod = makeImportedModule(self.name, self.data, self.scope)
            sys.modules[fullname] = mod
        return mod
    
class OnDemandImporter(object):
    """ The on-demand importer imports a module proxy that 
        inserts the desired module into the calling scope only when 
        an attribute from the module is actually used.
    """

    def find_module(self, fullname, path=None):
        origName = fullname
        if not path:
            mod = sys.modules.get(fullname, False)
            if mod is None or mod and isinstance(mod, types.ModuleType):
                return mod
        
        frame = sys._getframe(1)
        global_scope = frame.f_globals
        # this is the scope in which import <fullname> was called

        if '.' in fullname:
            head, fullname = fullname.rsplit('.', 1)

            # this import protocol works such that if I am going to be
            # able to import fullname, then everything in front of the 
            # last . in fullname must already be loaded into sys.modules.
            mod = sys.modules.get(head,None)
            if mod is None:
                return None

            if hasattr(mod, '__path__'):
                path = mod.__path__

        try:
            data = imp.find_module(fullname, path)
            return OnDemandLoader(origName, data, global_scope)
        except ImportError:
            # don't return an import error.  That will stop 
            # the automated search mechanism from working.
            return None

def install():
    sys.meta_path.append(OnDemandImporter())
