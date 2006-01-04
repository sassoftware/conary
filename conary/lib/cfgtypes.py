#
# Copyright (c) 2005 rPath, Inc.
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

import copy
import inspect
import re
import sre_constants
import os

class CfgType:

    """ A config value type wrapper -- gives a config value a conversion
        to and from a string, a way to copy values, and a way to print
        the string for display (if different from converting to a string)

        NOTE: most subclasses probably don't have to implement all of these
        methods, for most it will be enough to implement parseString.

        If the subclass is a list or a dictionary, subclassing from 
        CfgDict should mean that parseString is still all that needs
        to be overridden.
    """

    # if a default isn't specified for a subclass CfgType, it defaults to None
    default = None

    def __init__(self):
        pass

    def copy(self, val):
        """ Create a new copy of the given value """
        return copy.deepcopy(val)

    def parseString(self, str):
        """ Parse the given value.  
            The return value should be as is expected to be assigned to a 
            configuration item.
        """
        return str

    def updateFromString(self, val, str):
        """ Parse the given value, and apply it to the current value.
            The return value should be as is expected to be assigned to a 
            configuration item.

            It's possible for many simple configuration items that if you
            set a config value twice, the second assignment overwrites the 
            first.   In this case, val can be ignored.

            Modifying val in place is acceptable.
        """
        return self.parseString(str)

    def setFromString(self, val, str):
        """ Parse the given value, and return the value that you'd expect
            if the parsed value were supposed to replace val.

            The return value should be as is expected to be assigned to a 
            configuration item where val is currently.

            It's possible for many simple configuration items that if you
            set a config value twice, the second assignment overwrites the 
            first.   In this case, val can be ignored.

            Modifying val in place is acceptable.

            Generally, this is the same thing as parseString,
            except in odd cases such as CfgCallback.
        """
        return self.parseString(str)

    def getDefault(self, default=None):
        """ Get the default value for this CfgType
        """
        if default is not None:
            return self.copy(default)
        else:
            return self.copy(self.default)

    def format(self, val, displayOptions=None):
        """ Return a formated version of val in a format determined by 
            displayOptions.
        """
        return str(val)

    def toStrings(self, val, displayOptions=None):
        return [self.format(val, displayOptions)]

#---------- simple configuration item types
# A configuration type converts from string -> ConfigValue and from 
# ConfigValue -> string, and may store information about how to make that 
# change, but does NOT contain actual configuration values.

class CfgString(CfgType):
    pass

class CfgPath(CfgType):
    """ 
        String configuration option that accepts ~ as a substitute for $HOME
    """

    def parseString(self, str):
        return os.path.expanduser(os.path.expandvars(str))

    def getDefault(self, default=None):
        val = CfgType.getDefault(self, default)
        if val:
            return os.path.expanduser(os.path.expandvars(val))
        else:
            return val

class CfgInt(CfgType):
     
    def parseString(self, val):
        try:
            return int(val)
        except ValueError, msg:
            raise ParseError, 'expected integeter'

class CfgBool(CfgType):

    default = False

    def parseString(self, val):
        if val.lower() in ('0', 'false'):
            return False
        elif val.lower() in ('1', 'true'):
            return True
        else:
            raise ParseError, "expected True or False"


class CfgRegExp(CfgType):
    """ RegularExpression type.  
        Stores the value as (origVal, compiledVal)
    """
    
    def copy(self, val):
        return (val[0], re.compile(val[0]))
        
    def parseString(self, val):
        try:
            return (val, re.compile(val))
        except sre_constants.error, e:
            raise ParseError, str(e)

    def format(self, val, displayOptions=None):
        return val[0]

class CfgEnum(CfgType):
    """ Enumerated value type. Checks to ensure the strings passed in are 
        matched in self.validValues
    """

    validValues = []

    def checkEntry(self, var):
        var = var.lower()
        if var not in self.validValues:
            raise ParseError, 'valid values are %s' % '|'.join(self.validValues)

    def parseString(self, var):
        self.checkEntry(var)
        var = var.lower()
        return var

class CfgCallBack(CfgType):

    def __init__(self, callBackFn, *params):
        self.callBackFn = callBackFn
        self.params = params

    def setFromString(self, curVal, str):
        self.callBack(str)

    def updateFromString(self, curVal, str):
        self.callBack(str)

    def callBack(self, val):
        self.callBackFn(*((val,) + self.params))

# ---- configuration structures

# Below here are more complicated configuration structures.
# They allow you to go from string -> container 
# The abstract containers can all be modified to change their container
# type, and their item type.

class CfgLineList(CfgType):
    def __init__(self, valueType, separator=' ', listType=list, default=[]):
        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.listType = listType

        self.separator = separator
        self.valueType = valueType
        self.default = default

    def parseString(self, val):
        return self.listType(self.valueType.parseString(x) \
                             for x in val.split(self.separator) if x)

    def getDefault(self, default=None):
        if default is None: 
            default = self.default
        return [ self.valueType.getDefault(x) for x in default ] 

    def updateFromString(self, val, str):
        return self.parseString(str)

    def copy(self, val):
        return self.listType(self.valueType.copy(x) for x in val)

    def toStrings(self, value, displayOptions=None):
        if value:
            yield self.separator.join(
                        self.valueType.format(x, displayOptions) for x in value)



class CfgList(CfgType):

    def __init__(self, valueType, listType=list, default=[]):
        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.valueType = valueType
        self.listType = listType
        self.default = default

    def parseString(self, val):
        return self.listType([self.valueType.parseString(val)])

    def updateFromString(self, val, str):
        val.extend(self.parseString(str))
        return val

    def getDefault(self, default=None):
        if default is None: 
            default = self.default
        return self.listType(self.valueType.getDefault(x) for x in default)

    def copy(self, val):
        return self.listType(self.valueType.copy(x) for x in val)

    def toStrings(self, value, displayOptions=None):
        for val in value:
            yield self.valueType.format(val, displayOptions)



class CfgDict(CfgType):

    def __init__(self, valueType, dictType=dict, default={}):
        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.valueType = valueType
        self.dictType = dictType
        self.default = default

    def setFromString(self, val, str):
        return self.dictType(self.parseString(str))

    def updateFromString(self, val, str):
        # update the dict value -- don't just overwrite it, it might be
        # that the dict value is a list, so we call updateFromString
        strs = str.split(None, 1)
        if len(strs) == 1:
            dkey, dvalue = str, ''
        else:
            (dkey, dvalue) = strs

        if dkey in val:
            val[dkey] = self.valueType.updateFromString(val[dkey], dvalue)
        else:
            val[dkey] = self.valueType.parseString(dvalue)
        return val

    def parseString(self, val):
        vals = val.split(None, 1)

        if len(vals) == 1:
            dkey, dvalue = val, ''
        else:
            (dkey, dvalue) = vals

        dvalue = self.valueType.parseString(dvalue)
        return {dkey : dvalue}

    def getDefault(self, default=None):
        if default is None: 
            default = self.default
        return self.dictType((x,self.valueType.getDefault(y)) \
                             for (x,y) in default.iteritems()) 


    def toStrings(self, value, displayOptions):
        for key in sorted(value.iterkeys()):
            val = value[key]
            for item in self.valueType.toStrings(val, displayOptions):
                yield ' '.join(('%-25s' % key, item))

    def copy(self, val):
        return dict((k, self.valueType.copy(v)) for k,v in val.iteritems())

    def __iter__(self):
        for key in self.value:
            yield key

class CfgEnumDict(CfgDict):

    validValues = {}

    def __init__(self, valueType=CfgString, default={}):
        CfgDict.__init__(self, valueType, default=default)

    def checkEntry(self, val):
        k, v = val.split(None, 1)
        k = k.lower()
        v = v.lower()
        if k not in self.validValues:
            raise ParseError, 'invalid key "%s" not in "%s"' % (k, 
                                        '|'.join(self.validValues.keys()))
        if v not in self.validValues[k]:
            raise ParseError, 'invalid value "%s" for key %s not in "%s"' % (v, 
                                k, '|'.join(self.validValues[k]))

    def parseString(self, val):
        self.checkEntry(val)
        return CfgDict.parseString(self, val)



class RegularExpressionList(list):
    """ This is the actual configuration value -- NOT a config type.
        The CfgRegExpList returns values of this class.
    """
    def __init__(self, *args, **kw):
        list.__init__(self, *args, **kw)

    def __repr__(self):
        return 'RegularExpressionList(%s)' % list.__repr__(self)

    def addExp(self, val):
        list.append(self, (val, re.compile(val)))

    def match(self, s):
        for reStr, regExp in self:
            if regExp.match(s):
                return True

        return False

class CfgRegExpList(CfgList):
    def __init__(self, default=RegularExpressionList()):
        CfgList.__init__(self, CfgRegExp,  listType=RegularExpressionList, 
                         default=default)

    def updateFromString(self, val, newStr):
        return self.listType(val +
                     [self.valueType.parseString(x) for x in newStr.split()])

    def parseString(self, val):
        return self.listType(
                    [self.valueType.parseString(x) for x in val.split()])

CfgPathList  = CfgLineList(CfgPath, ':')

# --- errors

class CfgError(Exception):
    """
    Ancestor for all exceptions raised by the cfg module.
    """
    pass

class ParseError(CfgError):
    """
    Indicates that an error occured parsing the config file.
    """
    def __str__(self):
	return self.val

    def __init__(self, val):
	self.val = str(val)
