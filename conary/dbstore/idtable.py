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

# FIXME: convert to use the dbstore modules

class IdTable:
    """
    Generic table for assigning id's to simple items.
    """
    def __init__(self, db, tableName, keyName, strName):
        self.db = db
	self.tableName = tableName
	self.keyName = keyName
	self.strName = strName

        if  tableName in db.tables:
            return
        cu = self.db.cursor()
        cu.execute("""
        CREATE TABLE %s (
            %s INTEGER PRIMARY KEY AUTO_INCREMENT,
            %s VARCHAR(254)
        )""" %(self.tableName, self.keyName, self.strName))
        cu.execute("CREATE UNIQUE INDEX %s_uq on %s (%s)" %(
            self.tableName, self.tableName, self.strName))
        self.initTable(cu)
        db.commit()
        db.loadSchema()

    def initTable(self, cu):
	pass

    def getOrAddId(self, item):
        id = self.get(item, None)
        if id == None:
            id = self.addId(item)

        return id

    # DBSTORE: use dbstore sequences
    def addId(self, item):
        cu = self.db.cursor()
        cu.execute("INSERT INTO %s (%s, %s) VALUES (NULL, ?)" %(
            self.tableName, self.keyName, self.strName), (item,))
        return cu.lastrowid

    def getOrAddIds(self, items):
        cu = self.db.cursor()
        cu.execute('CREATE TEMPORARY TABLE neededIds (num INT, %s STR)' % self.strName)
        for num, item in enumerate(items):
            cu.execute('INSERT INTO neededIds VALUES (?, ?)', num, item)

        cu.execute('''INSERT INTO %(tableName)s (%(keyName)s, %(strName)s)
                      SELECT DISTINCT
                         NULL, neededIds.%(strName)s FROM neededIds
                         LEFT JOIN %(tableName)s AS existing USING(%(strName)s)
                         WHERE existing.%(keyName)s IS NULL
                   ''' % self.__dict__)
        ids = [ x[0] for x in
                cu.execute("""SELECT %s FROM neededIds JOIN %s USING(%s)
                              ORDER BY NUM"""
                           %(self.keyName, self.tableName, self.strName))]
        cu.execute('DROP TABLE neededIds')
        return ids

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM %s WHERE %s=?"
                   %(self.tableName, self.keyName), (theId,))

    def getId(self, theId):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   %(self.strName, self.tableName, self.keyName), (theId,))
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, theId

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   %(self.keyName, self.tableName, self.strName), (item,))
	return not(cu.fetchone() == None)

    def __delitem__(self, item):
        assert(type(item) is str)
        cu = self.db.cursor()
        cu.execute("DELETE FROM %s WHERE %s=?"
                   %(self.tableName, self.strName), item)

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   %(self.keyName, self.tableName, self.strName), (item,))
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   %(self.keyName, self.tableName, self.strName), (item,))
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def getItemDict(self, itemSeq):
	cu = self.db.cursor()
        cu.execute("SELECT %s, %s FROM %s WHERE %s in (%s)"
                   % (self.strName, self.keyName, self.tableName, self.strName,
		      ",".join(["'%s'" % x for x in itemSeq])))
	return dict(cu)

    def iterkeys(self):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s" %(self.strName, self.tableName))
        for row in cu:
            yield row[0]

    def itervalues(self):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s" %(self.keyName, self.tableName))
        for row in cu:
            yield row[0]

    def iteritems(self):
        cu = self.db.cursor()
        cu.execute("SELECT %s, %s FROM %s" 
		   %(self.strName, self.keyName, self.tableName))
        for row in cu:
            yield row

    def keys(self):
	return [ x for x in self.iterkeys() ]

    def values(self):
	return [ x for x in self.itervalues() ]

    def items(self):
	return [ x for x in self.iteritems() ]

class CachedIdTable(IdTable):
    """
    Provides an IdTable mapping with three differences -- ids are cached,
    they can't be removed, and getting a tag creates it if it doesn't
    already exist. This is designed for small tables!
    """

    def __init__(self, db, tableName, keyName, strName):
	IdTable.__init__(self, db, tableName, keyName, strName)
	cu = db.cursor()
	self.cache = {}
	self.revCache = {}
	cu.execute("SELECT %s, %s from %s" % (keyName, strName, tableName))
	for (idNum, s) in cu:
	    self.cache[s] = idNum
	    self.revCache[idNum] = s

    def getId(self, theId):
	return self.revCache[theId]

    def __getitem__(self, item):
	v = self.get(item, None)
	if v is not None:
	    return v

	return self.addId(item)

    def get(self, item, defValue):
	return self.cache.get(item, defValue)

    def addId(self, item):
	newId = IdTable.addId(self, item)
	self.cache[item] = newId
	self.revCache[newId] = item
	return newId

    def getItemDict(self, itemSeq):
	raise NotImplementedError

    def delId(self, theId):
	raise NotImplementedError

    def __delitem__(self, item):
	raise NotImplementedError

class IdPairMapping:
    """
    Maps an id tuple onto another id. The tuple can only map onto a single
    id.
    """
    def __init__(self, db, tableName, tup1, tup2, item):
        self.db = db
	self.tup1 = tup1
	self.tup2 = tup2
	self.item = item
	self.tableName = tableName

        if self.tableName in db.tables:
            return
        cu = self.db.cursor()
        cu.execute("""
        CREATE TABLE %s(
            %s INTEGER,
            %s INTEGER,
            %s INTEGER
        )""" % (tableName, tup1, tup2, item))
        self.initTable(cu)
        db.commit()
        db.loadSchema()

    def initTable(self, cu):
        pass

    def __setitem__(self, key, val):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("INSERT INTO %s (%s, %s, %s) "
                   "VALUES (?, ?, ?)"
                   % (self.tableName, self.tup1, self.tup2, self.item),
                   (first, second, val))

    def __getitem__(self, key):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=? AND %s=?"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, key

    def get(self, key, defValue):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=? AND %s=?"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def has_key(self, key):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=? AND %s=?"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	item = cu.fetchone()
	return item != None

    def __delitem__(self, key):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("DELETE FROM %s WHERE %s=? AND %s=?"
                   % (self.tableName, self.tup1, self.tup2),
		   (first, second))

class IdMapping:
    """
    Maps an one id onto another id. The mapping must be unique.
    """
    def __init__(self, db, tableName, key, item):
        self.db = db
	self.key = key
	self.item = item
	self.tableName = tableName

        if self.tableName in db.tables:
            return
        cu = self.db.cursor()
        cu.execute("""
        CREATE TABLE %s(
            %s INTEGER,
            %s INTEGER
        )""" % (tableName, key, item))
        self.initTable(cu)
        db.commit()
        db.loadSchema()


    def initTable(self, cu):
        pass

    def __setitem__(self, key, val):
        cu = self.db.cursor()
        cu.execute("INSERT INTO %s (%s, %s) "
                   "VALUES (?, ?)"
		   % (self.tableName, self.key. self.item),
                   (key, val))

    def __getitem__(self, key):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   % (self.item, self.tableName, self.key),
		   key)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, key

    def get(self, key, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   % (self.item, self.tableName, self.key),
		   key)
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def has_key(self, key):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   % (self.item, self.tableName, self.key),
		   key)
	item = cu.fetchone()
	return (item != None)

    def __delitem__(self, key):
        cu = self.db.cursor()
        cu.execute("DELETE FROM %s WHERE %s=?"
                   % (self.tableName, self.key),
		   key)

class IdPairSet(IdPairMapping):

    """
    Maps an id tuple onto another id. The tuple can map onto multiple
    ids.
    """
    def _getitemgen(self, first, cu):
	yield first[0]

	for match in cu:
	    yield match[0]

    def __getitem__(self, key):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=? AND %s=?"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	first = cu.fetchone()
	if not first:
	    raise KeyError, key
	return self._getitemgen(first, cu)

    def get(self, key, default):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=? AND %s=?"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	first = cu.fetchone()
	if not first:
            return default

	return self._getitemgen(first, cu)

    def getByFirst(self, first):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=?"
                   % (self.item, self.tableName, self.tup1),
		   first)
	first = cu.fetchone()
	if not first:
	    raise KeyError, first
	return self._getitemgen(first, cu)

    def __setitem__(self, key, value):
	raise AttributeError

    def addItem(self, key, val):
	IdPairMapping.__setitem__(self, key, val)

    def delItem(self, key, val):
	(first, second) = key
        cu = self.db.cursor()
        cu.execute("DELETE FROM %s WHERE %s=? AND %s=? AND %s=?"
                   % (self.tableName, self.tup1, self.tup2, self.item),
		   (first, second, val))
