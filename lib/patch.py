# this just applies hunks, not files... in other words, the --- and +++
# lines should be omitted, and only a single file can be patched
def patch(oldLines, unifiedDiff):
    i = 0
    last = len(unifiedDiff)
    result = []

    fromLine = 0

    while (i < last):
	try:
	    (magic1, fromRange, toRange, magic2) = unifiedDiff[i].split()
	    if magic1 != "@@" or magic2 != "@@" or fromRange[0] != "-" or \
	       toRange[0] != "+":
		raise BadHunkHeader()
	except:
	    raise 

	(fromStart, fromCount) = fromRange.split(",")
	fromStart = int(fromStart[1:]) - 1
	fromCount = int(fromCount)
	
	(toStart, toCount) = toRange.split(",")
	toStart = int(toStart[1:]) - 1
	toCount = int(toCount)

	lastItem = fromStart + fromCount
	if lastItem > len(oldLines):
	    raise BadHunkHeader()

	while (fromLine < fromStart):
	    result.append(oldLines[fromLine])
	    fromLine = fromLine + 1

	assert(toStart == len(result))

	i = i + 1
	while i < last and unifiedDiff[i][0] != "@":
	    if unifiedDiff[i][0] == " ":
		if oldLines[fromLine] != unifiedDiff[i][1:]:
		    raise Conflict()
		result.append(oldLines[fromLine])
		fromLine = fromLine + 1
	    elif unifiedDiff[i][0] == "+":
		result.append(unifiedDiff[i][1:])
	    elif unifiedDiff[i][0] == "-":
		fromLine = fromLine + 1
	    else:
		raise BadHunk()

	    i = i + 1

	if (fromStart + fromCount != fromLine):
	    raise BadHunk
	if (toStart + toCount != len(result)):
	    raise BadHunk
	
	return result
		
class BadHunkHeader(Exception):

    pass

class BadHunk(Exception):

    pass

class Conflict(Exception):

    pass
