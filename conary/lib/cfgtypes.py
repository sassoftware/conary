#
# Copyright (c) 2005-2006 rPath, Inc.
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

import copy
import inspect
import re
import shlex
import sre_constants
import os

class CfgType(object):

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

    def set(self, curVal, newVal):
        return self.copy(newVal)

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

CfgString = CfgType

_pathCache = {}
def Path(str):
    if str not in _pathCache:
        if '~' not in str and '$' not in str:
            p = _Path(str)
        else:
            p = _ExpandedPath(str)
        _pathCache[str] = p
        return p
    elif '~' in str or '$' in str:
        p = _ExpandedPath(str)
        if p != _pathCache[str]:
            _pathCache[str] = p
    return _pathCache[str]

class _Path(str):
    __slots__ = []

    def __init__(self, s):
        str.__init__(self, s)

    def _getUnexpanded(self):
        return self

    def __repr__(self):
        return "<Path '%s'>" % self

class _ExpandedPath(_Path):

    def __new__(cls, origString):
        string = os.path.expanduser(os.path.expandvars(origString))
        return str.__new__(cls, string)

    def __init__(self, origString):
        self._origString = origString

    def _getUnexpanded(self):
        return self._origString

    def __repr__(self):
        return "<Path '%s'>" % self

class CfgPath(CfgType):
    """
        String configuration option that accepts ~ as a substitute for $HOME
    """

    def parseString(self, str):
        return Path(str)

    def getDefault(self, default=None):
        val = CfgType.getDefault(self, default)
        if val:
            return Path(val)
        else:
            return val

    def format(self, val, displayOptions=None):
        if (not displayOptions.get('expandPaths', False)
            and hasattr(val, '_getUnexpanded')):
            return val._getUnexpanded()
        else:
            return str(val)

class CfgInt(CfgType):

    def parseString(self, val):
        try:
            return int(val)
        except ValueError, msg:
            raise ParseError, 'expected integer'

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

class CfgSignedRegExp(CfgRegExp):
    """SignedRegularExpression type.
    Allows for positive and negative regexp matching.
    Stores the value as (origVal, sense, compiledVal)
    """
    def copy(self, val):
        return (val[0], val[1], re.compile(val[0]))

    def parseString(self, val):
        sense = 0
        if val[0] == "+":
            sense = 1
        elif val[0] == "-":
            sense = -1
        else:
            raise ParseError, "regexp value '%s' needs to start with + or -" % (val,)
        try:
            return (val, sense, re.compile(val[1:]))
        except sre_constants.error, e:
            raise ParseError, "regexp '%s' parse error\n" % (val[1:],) + str(e)

    def format(self, val, displayOptions=None):
        return "%s%s" % ("- +"[val[1]+1], val[0])


class CfgEnum(CfgType):
    """ Enumerated value type. Checks to ensure the strings passed in are
        matched in self.validValues
        validValues can be a list or dict initially, but will be reset to a dict
    """

    validValues = {}
    origName = {}

    def checkEntry(self, val):
        if val.lower() not in self.validValues:
            raise ParseError, '%s not in (case insensitive): %s' % (str(val),
                                                 '|'.join(self.validValues))

    def parseString(self, val):
        self.checkEntry(val)
        return self.validValues[val.lower()]

    def format(self, val, displayOptions=None):
        if val not in self.origName:
            raise ParseError, "%s not in: %s" % (str(val),
                                                 '|'.join([str(x) for x in self.origName]))
        return self.origName[val]

    def __init__(self):
        CfgType.__init__(self)
        if isinstance(self.validValues, list):
            self.origName = dict([(x, x) for x in self.validValues])
            self.validValues = dict([(x.lower(), x) for x in self.validValues])

        else:
            self.origName = dict([(x[1], x[0]) \
                                for x in self.validValues.iteritems()])
            self.validValues = dict([(x[0].lower(), x[1]) \
                                     for x in self.validValues.iteritems()])

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
    def __init__(self, valueType, separator=None, listType=list, default=[]):
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
            separator = self.separator 
            if separator is None:
                separator = ' '
            yield separator.join(
                        self.valueType.format(x, displayOptions) for x in value)


class CfgQuotedLineList(CfgLineList):
    def __init__(self, valueType, listType=list, default=[]):
        CfgLineList.__init__(self, valueType=valueType, listType=listType,
                             default=default)

    def parseString(self, val):
        return self.listType(self.valueType.parseString(x) \
                             for x in shlex.split(val) if x)

    def toStrings(self, value, displayOptions=None):
        if value:
            yield "'" + "' '".join(
                    self.valueType.format(x, displayOptions) for x in value) + "'"

class CfgList(CfgType):

    def __init__(self, valueType, listType=list, default=[]):
        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.valueType = valueType
        self.listType = listType
        self.default = default

    def parseString(self, val):
        if val == '[]':
            return self.listType()
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
        if not value:
            yield '[]'
        else:
            for val in value:
                for str in self.valueType.toStrings(val, displayOptions):
                    yield str



class CfgDict(CfgType):

    def __init__(self, valueType, dictType=dict, default={}):
        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.valueType = valueType
        self.dictType = dictType
        self.default = default

    def setFromString(self, val, str):
        return self.dictType(self.parseString(str))

    def set(self, curVal, newVal):
        curVal.update(newVal)
        return curVal

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
            val[dkey] = self.parseValueString(dkey, dvalue)
        return val

    def parseString(self, val):
        vals = val.split(None, 1)

        if len(vals) == 1:
            dkey, dvalue = val, ''
        else:
            (dkey, dvalue) = vals

        dvalue = self.parseValueString(dkey, dvalue)
        return {dkey : dvalue}

    def parseValueString(self, key, value):
        return self.valueType.parseString(value)

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

    def __repr__(self):
        return 'RegularExpressionList(%s)' % list.__repr__(self)
    def addExp(self, val):
        list.append(self, CfgRegExp().parseString(val))
    def match(self, s):
        for reStr, regExp in self:
            if regExp.match(s):
                return True

        return False

class SignedRegularExpressionList(list):
    """
    Like a RegularExpressionList, but each member regexp is tagged with a
    + or - to signify a positive or negative match. Match return values
    are -1, 0 or 1 for a negative, unknown or positive match, respectively
    First match wins.
    """

    def __repr__(self):
        return "SignedRegularExpressionList(%s)" % list.__repr__(self)
    def addExp(self, val):
        list.append(self, CfgSignedRegExp().parseString(val))
    def match(self, s):
        for reStr, sense, regExp in self:
            if regExp.match(s):
                return sense
        return 0

class CfgRegExpList(CfgList):
    listType = RegularExpressionList
    valueType = CfgRegExp
    def __init__(self, default=listType()):
        CfgList.__init__(self, valueType=self.valueType, listType=self.listType,
                         default=default)

    def updateFromString(self, val, newStr):
        return self.listType(val + [self.valueType.parseString(x)
                                    for x in newStr.split()])

    def parseString(self, val):
        if val == '[]':
            return self.listType()
        return self.listType([self.valueType.parseString(x)
                              for x in val.split()])

class CfgSignedRegExpList(CfgRegExpList):
    listType = SignedRegularExpressionList
    valueType = CfgSignedRegExp

CfgPathList  = CfgLineList(CfgPath, ':')

# --- errors

class CfgError(Exception):
    """
    Ancestor for all exceptions raised by the cfg module.
    """
    pass

class ParseError(CfgError):
    """
    Indicates that an error occurred parsing the config file.
    """
    def __str__(self):
	return self.val

    def __init__(self, val):
	self.val = str(val)

class CfgEnvironmentError(CfgError):

    def __str__(self):
        return "Error reading config file %s: %s" % (self.path, self.msg)

    def __init__(self, path, msg):
        self.msg = msg
        self.path = path
