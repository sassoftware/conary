#
# Copyright (c) 2004 Specifix, Inc.
# All rights reserved
#

class IdTable:
    """
    Generic table for assigning id's to simple items.
    """
    def __init__(self, db, name):
        self.db = db
        self.name = name
	self.capName = self.name.capitalize()
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if '%ss' % self.capName not in tables:
            cu.execute("CREATE TABLE %ss(%sId integer primary key, %s str unique)"
                       %(self.capName, name, name))
	    self.initTable()

    def initTable(self):
	pass
    
    def addId(self, item):
        cu = self.db.cursor()
        cu.execute("INSERT INTO %ss VALUES (NULL, %%s)"
                   %(self.capName, ), (item,))
	return cu.lastrowid

    def delId(self, theId):
        assert(type(theId) is int)
        cu = self.db.cursor()
        cu.execute("DELETE FROM %ss WHERE %sId=%%d"
                   %(self.capName, self.name), (theId,))

    def getId(self, theId):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %ss WHERE %sId=%%s"
                   %(self.name, self.capName, self.name), (theId,))
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, theId

    def has_key(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT %sId FROM %ss WHERE %s=%%s"
                   %(self.name, self.capName, self.name), (item,))
	return not(cu.fetchone() == None)

    def __delitem__(self, item):
        assert(type(item) is str)
        cu = self.db.cursor()
        cu.execute("DELETE FROM %ss WHERE %s=%%s"
                   %(self.capName, self.name), (item,))

    def __getitem__(self, item):
        cu = self.db.cursor()
        cu.execute("SELECT %sId FROM %ss WHERE %s=%%s"
                   %(self.name, self.capName, self.name), (item,))
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, item

    def get(self, item, defValue):
        cu = self.db.cursor()
        cu.execute("SELECT %sId FROM %ss WHERE %s=%%s"
                   %(self.name, self.capName, self.name), (item,))
	item = cu.fetchone()
	if not item:
	    return defValue
	return item[0]

    def iterkeys(self):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %ss" %(self.name, self.capName))
        for row in cu:
            yield row[0]

    def itervalues(self):
        cu = self.db.cursor()
        cu.execute("SELECT %sId FROM %ss" %(self.name, self.capName))
        for row in cu:
            yield row[0]

    def iteritems(self):
        cu = self.db.cursor()
        cu.execute("SELECT %s, %sId FROM %ss" 
		   %(self.name, self.name, self.capName))
        for row in cu:
            yield row

    def keys(self):
	return [ x for x in self.iterkeys() ]

    def values(self):
	return [ x for x in self.itervalues() ]

    def items(self):
	return [ x for x in self.iteritems() ]

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
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if self.tableName not in tables:
            cu.execute("CREATE TABLE %s(%s integer, "
				       "%s integer, "
				       "%s integer)" 
			% (tableName, tup1, tup2, item))

    def __setitem__(self, key, val):
	(first, second) = key

        cu = self.db.cursor()
        cu.execute("INSERT INTO %s VALUES (%%d, %%d, %%d)"
		   % (self.tableName),
                   (first, second, val))

    def __getitem__(self, key):
	(first, second) = key

        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%d AND %s=%%d"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, key

    def get(self, key, defValue):
	(first, second) = key

        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%d AND %s=%%d"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	item = cu.fetchone()	
	if not item:
	    return defValue
	return item[0]
	    
    def has_key(self, key):
	(first, second) = key

        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%d AND %s=%%d"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))
	item = cu.fetchone()	
	return item != None

    def tup2InTable(self, val):
        cu = self.db.cursor()
        cu.execute("SELECT %s FROM %s WHERE %s=%%d"
                   % (self.item, self.tableName, self.tup2),
		   (val))
	item = cu.fetchone()	
	return item != None
	    
    def __delitem__(self, key):
	(first, second) = key

        cu = self.db.cursor()
	
        cu.execute("DELETE FROM %s WHERE %s=%%d AND %s=%%d"
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
        
        cu = self.db.cursor()
        cu.execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        tables = [ x[0] for x in cu ]
        if self.tableName not in tables:
            cu.execute("CREATE TABLE %s(%s integer, "
				       "%s integer)" 
			% (tableName, key, item))

    def __setitem__(self, key, val):
        cu = self.db.cursor()
        cu.execute("INSERT INTO %s VALUES (%%d, %%d)"
		   % (self.tableName),
                   (key, val))

    def __getitem__(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%s"
                   % (self.item, self.tableName, self.key),
		   key)
	try:
	    return cu.next()[0]
	except StopIteration:
            raise KeyError, key

    def get(self, key, defValue):
        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%s"
                   % (self.item, self.tableName, self.key),
		   key)
	item = cu.fetchone()	
	if not item:
	    return defValue
	return item[0]

    def has_key(self, key):
        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%s"
                   % (self.item, self.tableName, self.key),
		   key)
	item = cu.fetchone()	
	return (item != None)
	    
    def __delitem__(self, key):
        cu = self.db.cursor()
	
        cu.execute("DELETE FROM %s WHERE %s=%%s"
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
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%s AND %s=%%s"
                   % (self.item, self.tableName, self.tup1, self.tup2),
		   (first, second))

	first = cu.fetchone()
	if not first:
	    raise KeyError, key
	return self._getitemgen(first, cu)

    def getByFirst(self, first):
        cu = self.db.cursor()
	
        cu.execute("SELECT %s FROM %s WHERE %s=%%s"
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
	
        cu.execute("DELETE FROM %s WHERE %s=%%s AND %s=%%s AND %s=%%s"
                   % (self.tableName, self.tup1, self.tup2, self.item),
		   (first, second, val))
