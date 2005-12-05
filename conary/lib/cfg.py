#
# Copyright (c) 2004-2005 rpath, Inc.
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
Provides a generic config file format that supports creating your own config
types and value types.
"""
import copy
import inspect
import os
import re
import sre_constants
import sys
import textwrap

from conary.lib import util

class _Config:
    """ Base configuration class.  Supports defining a configuration object, 
        and displaying that object, but has no knowledge of how the input.

        Values that are supported in this configuration object are defined
        by creating class variables.  The format of such configuration items
        is as follows:

        <name> = (ConfigType, [default])

        or

        <name> = default

        In the second format, the default ConfigType is CfgString.

        ConfigType is a class or instance that describes how to parse
        a string into a configuration item, and display it.  The
        expected interface is documented in ConfigType.
    """
    
    def __init__(self):
        self._options = {}
        self._lowerCaseMap = {}
        self._displayOptions = {}
        self._directives = {}

        # iterate through the config items defined in this class
        # and any superclasses
        for class_ in reversed(inspect.getmro(self.__class__)):
            if not hasattr(class_, '_getConfigOptions'):
                continue

            for info in class_._getConfigOptions():
                self.addConfigOption(*info)

    @classmethod
    def _getConfigOptions(class_):
        """ 
        Scrape the supported configuration items from a class definition.
        Yields (name, CfgType, default) tuples.

        Expects foo = (CfgType, [default]) variables to defined in the class
        """
        for name, keyInfo in class_.__dict__.iteritems():
            if name.startswith('_'):
                continue
            params = ('type', 'default')
            kw = dict.fromkeys(params)

            if isinstance(keyInfo, (list,tuple)):
                for param, val in zip(params, keyInfo):
                    kw[param] = val

            elif keyInfo is None or isinstance(keyInfo, str):
                kw['type'] = CfgString
                kw['default'] = keyInfo

            elif inspect.isclass(keyInfo) and issubclass(keyInfo, CfgType):
                kw['type'] = keyInfo

            elif isinstance(keyInfo, CfgType):
                kw['type'] = keyInfo

            else:
                continue

            yield [name] + [kw[x] for x in params]


    def addConfigOption(self, key, type, default=None):
        """
        Defines a Configuration Item for this configuration.  
        This config item defines an available configuration setting.
        """
        self._options[key] = ConfigOption(key, type, default)

        # remove the default class documentation from this instance
        self._options[key].__doc__ = None

        self._lowerCaseMap[key.lower()] = key
        self[key] = copy.deepcopy(self._options[key].default)

    def addListener(self, key, fn):
        """ 
        Add a listener function that will be called when the given key is 
        updated.  The function will be called with key as a single parameter.
        """
        self._options[key].addListener(fn)

    def addDirective(self, key, fn):
        """
        Add a directive that acts as a config option.  When that config 
        option is read in, the function will be called with (key, value)
        where value is whatever was after the directive in the config file.
        """
        self._directives[key] = fn

    # --- Display options allow arbitrary display parameters to be set --
    # they can be picked up by the strings printing themselves

    def _initDisplayOptions(self):
        self._displayOptions = dict(prettyPrint=False)

    def setDisplayOptions(self, **kw):
        self._displayOptions.update(kw)

    def getDisplayOption(self, key, default=None):
        if default is not None:
            return self._displayOptions.get(key, default)
        else:
            return self._displayOptions.get(key)

    # --- accessing/setting values ---
    
    def __getitem__(self, name):
        """ Provide a dict-list interface to config items """
        # getitem should not be used to access internal values
        if name[0] == '_' or name not in self._options:
            raise KeyError, 'No such config item "%s"' % name
        return self.__dict__[name]

    def __setitem__(self, key, value):
        if key[0] == '_' or key.lower() not in self._lowerCaseMap:
            raise KeyError, 'No such attribute "%s"' % key
        key = self._lowerCaseMap[key.lower()]
        self.__dict__[key] = value

    def setValue(self, key, value):
        self[key] = value

    def keys(self):
        return self._options.keys()

    def iterkeys(self):
        return self._options.iterkeys()

    def itervalues(self):
        for name, item in self._options.iterkeys():
            yield self[name]

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def iteritems(self):
        for name, item in self._options.iteritems():
            yield name, self[name]

    # --- displaying/writing values ---

    def display(self, out=None):
        """ Display the config file respecting all display options
        """
        if out is None:
            out = sys.stdout
        self._write(out, self._displayOptions, includeDocs=False)

    def store(self, out, includeDocs=True):
        """ Write the config file in a format that should be readable
            by the same config file.
        """
        self._write(out, dict(prettyPrint=False), includeDocs=includeDocs)

    def displayKey(self, key, out):
        if out is None:
            out = sys.stdout
        self._writeKey(out, self._options[key], self[key], self._displayOptions)

    def storeKey(self, key, out):
        self._writeKey(out, self._options[key], self[key], dict(prettyPrint=False))
        
    def writeToFile(self, path, includeDocs=True):
        util.mkdirChain(os.path.dirname(path))
        self.store(open(path, 'w'), includeDocs)

    def _write(self, out, options, includeDocs=True):
        for name, item in sorted(self._options.iteritems()):
            if includeDocs:
                item.writeDoc(out)
            self._writeKey(out, item, self[name], options)

    def _writeKey(self, out, cfgItem, value, options):
        cfgItem.write(out, value, options)


class ConfigFile(_Config):
    
    """ _Config class + ability to read in files """

    def __init__(self):
        _Config.__init__(self)
        self.addDirective('includeConfigFile', 'includeConfigFile')

    def read(self, path, exception=False):
	if os.path.exists(path):
	    f = open(path, "r")
	    lineno = 1
            while True:
                line = f.readline()
                if not line:
                    break

                lineCount = 1
                while len(line) > 1 and '#' not in line and line[-2] == '\\':
                    # handle \ at the end of the config line.
                    # keep track of the lines we use so that we can
                    # give accurate line #s for errors.  This config line
                    # will be considered to live on its first line even 
                    # though it spans multiple lines.

                    line = line[:-2] + f.readline()
                    lineCount += 1
		self.configLine(line, path, lineno)
		lineno = lineno + lineCount
	    f.close()
	elif exception:
	    raise IOError, "No such file or directory: '%s'" % path

    def configLine(self, line, fileName = "override", lineno = '<No line>'):
        line = line.strip()
        if not line or line[0] == '#':
            return
        parts = line.split(None, 1)
        if len(parts) == 1:
            key = parts[0]
            val = ''
        else:
            (key, val) = parts

        if key in self._directives:
            fn = getattr(self, self._directives[key])
            fn(val)
        else:
            try:
                key = self._lowerCaseMap[key.lower()]
                self[key] = self._options[key].parseString(self[key], val)
            except KeyError, msg:
                raise ParseError, "%s:%s: unknown config item '%s'" % (fileName,
                                                                      lineno, 
                                                                      key)
            except ParseError, msg:
                raise ParseError, "%s:%s: %s for configuration item '%s'" \
                                                                % (fileName,
                                                                   lineno, 
                                                                   msg, key)

    def includeConfigFile(self, val):
        for cfgfile in util.braceGlob(val):
            self.read(cfgfile, exception=True)


class SectionedConfigFile(ConfigFile):
    """ 
        A SectionedConfigFile allows the definition of sections 
        using [foo] to delineate sections.

        When a new section is discovered, a new sectionType of 
        self._sectionType is assigned.
    """

    _sectionType = None

    def __init__(self):
        ConfigFile.__init__(self)
        self._sections = {}
        self._sectionName = ''
        assert(issubclass(self._sectionType, ConfigSection))

    def hasSection(self, sectionName):
        return sectionName in self._sections

    def getSection(self, sectionName):
        return self._sections[sectionName]

    def setSection(self, sectionName):
        if sectionName not in self._sections:
            self._sections[sectionName] = self._sectionType(self)
        self._sectionName = sectionName
        return self._sections[sectionName]

    def configLine(self, line, file = "override", lineno = '<No line>'):
	line = line.strip()
        if line and line[0] == '[' and line[-1] == ']':
            self.setSection(line[1:-1])
            return
        if self._sectionName:
            self._sections[self._sectionName].configLine(line, file, lineno)
        else:
            ConfigFile.configLine(self, line, file, lineno)

    def _writeSection(self, sectionName, options):
        """ Determine whether to write the given section
        """
        return True

    def _write(self, out, options, includeDocs=True):
        ConfigFile._write(self, out, options, includeDocs)
        for sectionName in sorted(self._sections):
            if self._writeSection(sectionName, options):
                out.write("\n\n[%s]\n" % sectionName)
                self._sections[sectionName]._write(out, options, includeDocs)

    def read(self, *args, **kw):
        # when reading a new config file, reset the section.
        oldSection = self._sectionName
        self._sectionName = None
        return ConfigFile.read(self, *args, **kw)
        self._sectionName = oldSection


class ConfigSection(ConfigFile):
    """ A Config Section.  
        Basically a separate config file, except that it knows who its
        parent config file is.
    """

    def __init__(self, parent):
        self._parent = parent
        ConfigFile.__init__(self)

    def getParent(self):
        return self._parent

    def getDisplayOption(self, key):
        return self._parent.getDisplayOption(key)

#----------------------------------------------------------

class ConfigOption:
    """ A name, value Type pair that knows how to display itself and 
        parse values for itself.  
        
        Note that a config option doesn't have any particular value associated
        with it.
    """

    def __init__(self, name, valueType, default=None):
        self.name = name

        # CfgTypes must be instantiated to parse values, because they
        # optionally store data that helps them parse.

        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.valueType = valueType

        if default is not None:
            self.default = self.valueType.copy(default)
        else:
            self.default = self.valueType.copy(valueType.default)
        self.listeners = []

    def parseString(self, curVal, str):
        """ 
        Takes the current value for this option, and a string to update that
        value, and returns an updated value (which may either overwrite the 
        current value or update it depending on the valueType)
        """
        self._callListeners()

        if curVal == self.default:
            return self.valueType.setFromString(curVal, str)
        else:
            return self.valueType.updateFromString(curVal, str)

    def __deepcopy__(self, memo):
        # we implement deepcopy because this object keeps track of a 
        # set of listener functions, and copy.__deepcopy__ doesn't
        # handle copying functions.  Since we don't particularly care
        # about that use case (if you're modifying code in a function object,
        # you're on your own), just copy the list of fns.
        valueType = copy.deepcopy(self.valueType, memo)
        default = valueType.copy(self.default)
        new = self.__class__(self.name, valueType, default)
        listeners = list(self.listeners)
        new.listeners = listeners
        return new

    def addDoc(self, docString):
        self.__doc__ = docString

    def getValueType(self):
        return self.valueType

    def addListener(self, listenFn):
        self.listeners.append(listenFn)

    def _callListeners(self):
        for listenFn in self.listeners:
            listenFn(self.name)
    
    def write(self, out, value, displayOptions=None):
        """ Writes a config option name and value.
        """
        if value is None:
            return

        # note that the value for a config item may only be reproducable
        # by multiple lines in a config file.
        for line in self.valueType.toStrings(value, displayOptions):
            out.write('%-25s %s\n' % (self.name, line))

    def writeDoc(self, out, displayOptions=None):
        """ Output documentation and default information in a way that
            is parsable by ConfigFiles
        """
        tw = textwrap.TextWrapper(initial_indent='# ', 
                                  subsequent_indent='# ', width=70)
        out.write('# %s (Default: %s)\n' % (self.name, ', '.join(self.valueType.toStrings(self.default, displayOptions))))
        if self.__doc__:
            out.write('\n'.join(tw.wrap(self.__doc__)))
        out.write('\n')


#---------- simple configuration item types

# NOTE:
# The rest of this file defines configuration types.
# A configuration type converts from string -> ConfigValue and from 
# ConfigValue -> string, and may store information about how to make that 
# change, but does NOT contain actual configuration values.

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

    def format(self, val, displayOptions=None):
        """ Return a formated version of val in a format determined by 
            displayOptions.
        """
        return str(val)

    def toStrings(self, val, displayOptions=None):
        return [self.format(val, displayOptions)]

class CfgString(CfgType):
    pass

class CfgPath(CfgType):
    """ 
        String configuration option that accepts ~ as a substitute for $HOME
    """

    def parseString(self, str):
        return os.path.expanduser(str)

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

    def copy(self, val):
        return self.listType(self.valueType.copy(x) for x in val)

    def toStrings(self, value, displayOptions=None):
        for val in value:
            yield self.valueType.format(val, displayOptions)



class CfgDict(CfgType):

    dictType = dict
    
    def __init__(self, valueType, default={}):
        if inspect.isclass(valueType) and issubclass(valueType, CfgType):
            valueType = valueType()

        self.valueType = valueType
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

    def addExp(self, val):
        list.append(self, (val, re.compile(val)))

    def match(self, s):
        for reStr, regExp in self:
            if regExp.match(s):
                return True

        return False

class CfgRegExpList(CfgList):
    def __init__(self, default=[]):
        CfgList.__init__(self, CfgRegExp,  listType=RegularExpressionList, 
                         default=default)

    def updateFromString(self, val, str):
        val.extend(self.parseString(x) for x in val.split())

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
