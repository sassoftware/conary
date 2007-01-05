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

def isDerivedPackageRecipe(recipeClass):
    return recipeClass.getType() == RECIPE_TYPE_DERIVEDPKG

class Recipe:
    """Virtual base class for all Recipes"""
    _trove = None
    _trackedFlags = None
    _loadedTroves = []
    _loadedSpecs = {}
    _recipeType = RECIPE_TYPE_UNKNOWN

    def __init__(self):
        assert(self.__class__ is not Recipe)
        self.validate()

    @classmethod
    def getType(class_):
        return class_._recipeType

    @classmethod
    def getLoadedTroves(class_):
        return class_._loadedTroves

    @classmethod
    def getLoadedSpecs(class_):
        return class_._loadedSpecs

    def __repr__(self):
        return "<%s Object>" % self.__class__

    @classmethod
    def validateClass(class_):
        pass

    def validate(self):
        pass



