class EnumeratedType(dict):

    def __getattr__(self, item):
	if self.has_key(item):
	    return self[item]

	return self.__dict__[item] 

    def __init__(self, name, *vals):
	for item in vals:
	    self[item] = "%s-%s" % (name, item)
