
class Hunk:

    # the new lines which result from this hunk are returned
    def apply(self, src, srcLine):
	result = []
	fromLine = srcLine
	for line in self.lines:
	    if line[0] == " ":
		if src[fromLine] != line[1:]:
		    raise Conflict()
		result.append(src[fromLine])
		fromLine = fromLine + 1
	    elif line[0] == "+":
		result.append(line[1:])
	    elif line[0] == "-":
		if src[fromLine] != line[1:]:
		    raise Conflict()

		fromLine = fromLine + 1

	assert(fromLine == self.fromLen + srcLine)
	assert(len(result) == self.toLen)

	return result

    def countConflicts(self, src, srcLine):
	conflicts = 0
	for line in self.lines:
	    if line[0] == " " or line[0] == "-":
		if src[srcLine] != line[1:]: 
		    conflicts += 1
		srcLine += 1

	return conflicts

    def __init__(self, fromStart, fromLen, toStart, toLen, lines):
	self.fromStart = fromStart
	self.toStart = toStart
	self.fromLen = fromLen
	self.toLen = toLen
	self.lines = lines


# this just applies hunks, not files... in other words, the --- and +++
# lines should be omitted, and only a single file can be patched
def patch(oldLines, unifiedDiff):
    i = 0
    last = len(unifiedDiff)
    hunks = []
    while i < last:
	(magic1, fromRange, toRange, magic2) = unifiedDiff[i].split()
	if magic1 != "@@" or magic2 != "@@" or fromRange[0] != "-" or \
	   toRange[0] != "+":
	    raise BadHunkHeader()

	(fromStart, fromLen) = fromRange.split(",")
	fromStart = int(fromStart[1:]) - 1
	fromLen = int(fromLen)
	
	(toStart, toLen) = toRange.split(",")
	toStart = int(toStart[1:]) - 1
	toLen = int(toLen)

	fromCount = 0
	toCount = 0
	lines = []
	i += 1
	while i < last and unifiedDiff[i][0] != '@':
	    lines.append(unifiedDiff[i])
	    ch = unifiedDiff[i][0]
	    if ch == " ":
		fromCount += 1
		toCount += 1
	    elif ch == "-":
		fromCount += 1
	    elif ch == "+":
		toCount += 1
	    else:
		raise BadHunk()

	    i += 1

	if toCount != toLen or fromCount != fromLen:
	    print toCount, toLen
	    raise BadHunk()

	hunks.append(Hunk(fromStart, fromLen, toStart, toLen, lines))


    i = 0
    last = len(unifiedDiff)
    result = []

    fromLine = 0
    offset = 0

    for hunk in hunks:
	start = hunk.fromStart + offset
	conflicts = hunk.countConflicts(oldLines, start)

	i = 0
	while conflicts:
	    i = i + 1
	    tried = 0
	    if (start - abs(i) >= 0):
		tried = 1
		conflicts = hunk.countConflicts(oldLines, start - i)
		if not conflicts:
		    i = -i
		    break

	    if (start + i < (len(oldLines) - hunk.fromLen)):
		tried = 1
		conflicts = hunk.countConflicts(oldLines, start + i)
		if not conflicts:
		    break

	    if not tried:
		break

	if conflicts: raise Conflict()

	offset = i
	start += i

	while (fromLine < start):
	    result.append(oldLines[fromLine])
	    fromLine = fromLine + 1

	result += hunk.apply(oldLines, start)
	fromLine += hunk.fromLen

    while (fromLine < len(oldLines)):
	result.append(oldLines[fromLine])
	fromLine = fromLine + 1
	
    return result
		
class BadHunkHeader(Exception):

    pass

class BadHunk(Exception):

    pass

class Conflict(Exception):

    pass

def reverse(lines):
    result = []
    for line in lines:
	if line[0] == " ":
	    result.append(line)
	elif line[0] == "+":
	    result.append("-" + line[1:])
	elif line[0] == "-":
	    result.append("+" + line[1:])
	elif line[0] == "@":
	    fields = line.split()
	    new = [ fields[0], "-" + fields[2][1:], 
		    "+" + fields[1][1:], fields[3] ]
	    result.append(" ".join(new) + "\n")

    return result
	    
