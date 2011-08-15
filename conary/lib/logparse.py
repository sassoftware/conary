#
# Copyright (c) rPath, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


def getConaryLogLineList(conaryLogFile='/var/log/conary'):
    return [ x.strip() for x in file(conaryLogFile).readlines() ]

def getConaryLogEventList(lines=None):
    """
    Returns a list of tuples associating the first timestamp in an
    event with a list of lines without timestamps:
    [(timestamp, ['list', 'of', 'lines]), ...]
    """
    #
    # Note: if it would be convenient to add more thorough parsing,
    # add a new interface for the other parsing.  This interface
    # should stay stable.
    #

    def _stripdate(logentry):
        if not logentry.startswith('['):
            # no date present (e.g. part of a traceback)
            return logentry
        dateend = logentry.find(']')
        if dateend <= 0:
            return logentry
        return logentry[dateend+1:].strip()

    if not lines:
        lines = getConaryLogLineList()
    eventStart = []

    inTraceBack = False
    for n in range(len(lines)):
        thisLine = lines[n]

        if thisLine.startswith('Traceback '):
            inTraceBack = True
        if thisLine.startswith('['):
            inTraceBack = False
        if inTraceBack:
            continue

        dateend = thisLine.find(']')
        if (dateend > 0 and len(thisLine) > dateend+2
            and thisLine[dateend+2] == ' '):
            continue
        if lines[n].endswith('command complete'):
            continue
        eventStart.append(n)

    slices = []
    for n in range(len(eventStart) - 1):
        slices.append((eventStart[n], eventStart[n+1]))
    slices.append((eventStart[len(eventStart) - 1], len(lines)))

    events = []
    for start, end in slices:
        first = lines[start]
        date = ''
        dateend = first.find(']')
        if dateend > 0:
            date = first[1:dateend]
        events.append((date, [_stripdate(x) for x in lines[start:end]]))

    return events
