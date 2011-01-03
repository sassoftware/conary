#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

from xml.parsers import expat

class XmlParser(object):

    """
    Very, very basic nonvalidating-in-the-slightest parser built around expat.
    Designed to be derived from, with subclasses defining newElement() and
    finishElement(), or both. Elements get an integer key associated with them
    which is used to track parent/child relationships in case it's needed.

    Example:

        class MyParser(XmlParser):

            def newElement(self, parent, this, name, attrs):
                self.elements[this] = name

            def finishElement(self, parent, this, name, attrs, data):
                print "found element", name, "child of", self.elements[parent]
    """

    def __init__(self, s):
        self.currentData = ''
        self.currentName = None
        self.currentAttrs = None
        self.elementStack = []
        self.currentKey = None
        self.lastKey = 0

        self.p = expat.ParserCreate()
        self.p.StartElementHandler = self.StartElementHandler
        self.p.EndElementHandler = self.EndElementHandler
        self.p.CharacterDataHandler = self.CharacterDataHandler

    def parse(self, s):
        self.p.Parse(s)

    def newElement(self, parentElement, thisElement, thisName, thisAttrs):
        pass

    def finishElement(self, parentElement, thisElement, thisName, thisAttrs,
                      thisData):
        pass

    def CharacterDataHandler(self, data):
        self.currentData += data

    def EndElementHandler(self, name):
        assert(self.currentName == name)
        self.finishElement(self.elementStack[-1][0], self.currentKey,
                           self.currentName, self.currentAttrs,
                           self.currentData)
        (self.currentKey, self.currentName, self.currentAttrs,
         self.currentData) = self.elementStack.pop()

    def StartElementHandler(self, name, attrs):
        self.elementStack.append( (self.currentKey, self.currentName,
                                   self.currentAttrs, self.currentData) )
        self.currentName = name
        self.currentAttrs = attrs
        self.currentData = ''
        self.newElement(self.currentKey, self.lastKey, name, attrs)
        self.currentKey = self.lastKey
        self.lastKey += 1