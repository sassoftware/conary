#
# Copyright (c) 2006 rPath, Inc.
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
        dateend = logentry.find(']')
        if dateend <= 0:
            return logentry
        return logentry[dateend+1:].strip()

    if not lines:
        lines = getConaryLogLineList()
    eventStart = []

    for n in range(len(lines)):
        dateend = lines[n].find(']')
        if dateend > 0 and lines[n][dateend+2] == ' ': continue
        if lines[n].endswith('command complete'): continue
        eventStart.append(n)

    slices = []
    for n in range(len(eventStart) - 1):
        slices.append((eventStart[n], eventStart[n+1]))
    slices.append((eventStart[len(eventStart) - 1], len(lines)))

    events = []
    for start, end in slices:
        first = lines[n]
        date = ''
        dateend = first.find(']')
        if dateend > 0:
            date = first[1:dateend-1]
        events.append((date, [_stripdate(x) for x in lines[start:end]]))

    return events
