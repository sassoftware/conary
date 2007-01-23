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

from conary.errors import ParseError

"""
Contains the base Recipe class
"""
RECIPE_TYPE_UNKNOWN   = 0
RECIPE_TYPE_PACKAGE   = 1
RECIPE_TYPE_FILESET   = 2
RECIPE_TYPE_GROUP     = 3
RECIPE_TYPE_INFO      = 4
RECIPE_TYPE_REDIRECT  = 5

def isPackageRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_PACKAGE

def isFileSetRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_FILESET

def isGroupRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_GROUP

def isInfoRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_INFO

def isRedirectRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_REDIRECT

class Recipe:
    """Virtual base class for all Recipes"""
    _trove = None
    _trackedFlags = None
    _loadedTroves = []
    _loadedSpecs = {}
    _recipeType = RECIPE_TYPE_UNKNOWN
    _isDerived = False

    def __init__(self):
        assert(self.__class__ is not Recipe)
        self.validate()

    @classmethod
    def getType(class_):
        return class_._recipeType

    @classmethod
    def getLoadedTroves(class_):
        # return a copy to avoid editing-in-place which
        # could result in modifying the Recipe _loadedTroves
        # list.
        return list(class_._loadedTroves)

    @classmethod
    def getLoadedSpecs(class_):
        return list(class_._loadedSpecs)

    @classmethod
    def addLoadedTroves(class_, newTroves):
        # NOTE: we have these method to ensure that the
        # class variable we're using is assigned to _this_
        # class and not some superclass.
        class_._loadedTroves = class_._loadedTroves + newTroves

    @classmethod
    def addLoadedSpecs(class_, newSpecs):
        class_._loadedSpecs = dict(class_._loadedSpecs)
        class_._loadedSpecs.update(newSpecs)

    def __repr__(self):
        return "<%s Object>" % self.__class__

    @classmethod
    def validateClass(class_):
        if class_.version == '':
            raise ParseError("empty release string")

    def validate(self):
        pass



