#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Applies unified format diffs
"""

from conary.lib import fixeddifflib, log
import types

class Hunk:

    # The new lines which result from this hunk are returned. This does
    # not check for conflicts; countConflicts() should be used for that
    def apply(self, src, srcLine):
        result = []
        fromLine = srcLine
        for line in self.lines:
            if line[0] == " ":
                if fromLine >= len(src):
                    continue

                result.append(src[fromLine])
                fromLine = fromLine + 1
            elif line[0] == "+":
                result.append(line[1:])
            elif line[0] == "-":
                fromLine = fromLine + 1

        return result

    def countConflicts(self, src, origSrcLine):
        # returns -1 if the patch was already applied, or the number of lines
        # which conflict
        conflicts = 0
        srcLen = len(src)
        srcLine = origSrcLine
        for line in self.lines:
            if line[0] == " ":
                if srcLine >= srcLen:
                    conflicts += 1
                elif src[srcLine] != line[1:]:
                    conflicts += 1
                srcLine += 1
            elif line[0] == "-":
                # - lines need to be exact matches; we don't want to erase
                # a line which has been changed. Return that every line
                # conflicts to ensure we don't apply this.
                if srcLine >= srcLen:
                    conflicts = len(self.lines)
                elif src[srcLine] != line[1:]:
                    conflicts = len(self.lines)
                srcLine += 1

        if conflicts:
            # has this patch already been applied?
            applied = True
            srcLine = origSrcLine
            for line in self.lines:
                if line[0] == " " or line[0] == "+":
                    if srcLine >= srcLen:
                        applied = False
                        break
                    elif src[srcLine] != line[1:]:
                        applied = False
                        break

                    srcLine += 1

            if applied:
                return -1

        return conflicts

    def write(self, f):
        f.write(self.asString())

    def asString(self):
        str = "@@ -%d,%s +%d,%d @@\n" % (self.fromStart, self.fromLen,
                self.toStart, self.toLen)
        str += "".join(self.lines)
        return str

    def __init__(self, fromStart, fromLen, toStart, toLen, lines, contextCount):
        self.fromStart = fromStart
        self.toStart = toStart
        self.fromLen = fromLen
        self.toLen = toLen
        self.lines = lines
        self.contextCount = contextCount

class FailedHunkList(list):

    def write(self, filename, oldName, newName):
        f = open(filename, "w")
        f.write("--- %s\n" % oldName)
        f.write("+++ %s\n" % newName)
        for hunk in self:
            hunk.write(f)

# this just applies hunks, not files... in other words, the --- and +++
# lines should be omitted, and only a single file can be patched
def patch(oldLines, unifiedDiff):
    i = 0

    if type(unifiedDiff) == types.GeneratorType:
        # convert to a proper list
        unifiedDiff = [ l for l in unifiedDiff ]

    # split the diff up into a set of hunks
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
        contextCount = 0
        lines = []
        i += 1
        while i < last and unifiedDiff[i][0] != '@':
            lines.append(unifiedDiff[i])
            ch = unifiedDiff[i][0]
            if ch == " ":
                fromCount += 1
                toCount += 1
                contextCount += 1
            elif ch == "-":
                fromCount += 1
            elif ch == "+":
                toCount += 1
            elif unifiedDiff[i] == '\ No newline at end of file\n':
                del lines[-1]
                lines[-1] = lines[-1][:-1]
            else:
                raise BadHunk()

            i += 1

        if toCount != toLen or fromCount != fromLen:
            raise BadHunk()

        hunks.append(Hunk(fromStart, fromLen, toStart, toLen, lines,
                          contextCount))

    i = 0
    result = []
    failedHunks = FailedHunkList()

    fromLine = 0
    offset = 0

    numHunks = len(hunks)
    for idx, hunk in enumerate(hunks):
        log.info('patch: applying hunk %s of %s', idx + 1, numHunks)
        start = hunk.fromStart + offset
        conflicts = hunk.countConflicts(oldLines, start)
        best = (conflicts, 0)

        i = 0
        while best[0]:
            i = i + 1
            tried = 0
            if (start - abs(i) >= 0):
                tried = 1
                conflicts = hunk.countConflicts(oldLines, start - i)
                if conflicts < best[0]:
                    best = (conflicts, -i)

                if not conflicts:
                    i = -i
                    break

            if ((start + i) <= (len(oldLines) - hunk.fromLen)):
                tried = 1
                conflicts = hunk.countConflicts(oldLines, start + i)
                if conflicts < best[0]:
                    best = (conflicts, i)
                if not conflicts:
                    break

            if not tried:
                break

        conflictCount = best[0]

        if conflictCount == -1:
            # this hunk has already been applied; skip it
            continue

        if conflictCount and (hunk.contextCount - conflictCount) < 2:
            failedHunks.append(hunk)
            continue

        offset = best[1]
        start += offset

        while (fromLine < start):
            result.append(oldLines[fromLine])
            fromLine = fromLine + 1

        result += hunk.apply(oldLines, start)
        fromLine += hunk.fromLen

    while (fromLine < len(oldLines)):
        result.append(oldLines[fromLine])
        fromLine = fromLine + 1

    return (result, failedHunks)

class BadHunkHeader(Exception):

    pass

class BadHunk(Exception):

    pass

def reverse(lines):
    for line in lines:
        if line[0] == " ":
            yield line
        elif line[0] == "+":
            yield "-" + line[1:]
        elif line[0] == "-":
            yield "+" + line[1:]
        elif line[0] == "@":
            fields = line.split()
            new = [ fields[0], "-" + fields[2][1:],
                    "+" + fields[1][1:], fields[3] ]
            yield " ".join(new) + "\n"

def unifiedDiff(first, second, *args, **kwargs):
    # return a unified diff like difflib.unified_diff, but add the right magic
    # for missing trailing newlines

    diff = list(fixeddifflib.unified_diff(first, second, *args, **kwargs))
    missingNewLines = []
    for i, line in enumerate(diff):
        if line[-1] != '\n':
            missingNewLines.append(i)

    for i in reversed(missingNewLines):
        diff.insert(i + 1, '\ No newline at end of file\n')
        diff[i] += '\n'

    # be a drop in replacement for difflib.unified_diff
    for line in diff:
        yield line
