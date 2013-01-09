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
