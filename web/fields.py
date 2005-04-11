#
# Copyright (c) 2005 Specifix, Inc.
#
# All rights reserved
#

def strFields(**params):
    """Decorator for cgi fields.  Use like @strFields(foo=None, bar='foo') 
    where foo is a required parameter, and bar defaults to 'foo'.
    Converts parameters to the given type, leaves other paramters untouched.  
    """
    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    value = str(kw[name])
                elif default is None:
                    raise RuntimeError, 'Required Parameter %s missing' % name
                else:
                    value = default
                kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco

def intFields(**params):
    """Decorator for cgi fields.  Use like @intFields(foo=None, bar=2) 
    where foo is a required parameter, and bar defaults to 2.
    Converts parameters to the given type, leaves other paramters untouched.  
    """

    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    value = int(kw[name])
                elif default is None:
                    raise RuntimeError, 'Required Parameter %s missing' % name
                else:
                    value = default
                kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco

def listFields(**params):
    def deco(func):
        def wrapper(self, **kw):
            for name, default in params.iteritems():
                if name in kw:
                    if not isinstance(kw[name], list):
                        value = [kw[name]]
                    else:
                        value = kw[name]
                elif default is None:
                    raise RuntimeError, 'Required Parameter %s missing' % name
                else:
                    value = []
            kw[name] = value
            return func(self, **kw)
        return wrapper
    return deco
