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


import re


SUBST_IDENTIFIER = re.compile(r'([ (,]):([a-zA-Z_][a-zA-Z0-9_]*)')


def quoteIdentifier(name):
    name = name.replace('"', '""')
    return '"%s"' % (name,)


def _swapOnce(query):
    query = query.replace('?', '%s')
    query = SUBST_IDENTIFIER.sub(r'\1%(\2)s', query)
    return query


def _min(*args):
    """Returns the smallest non-negative argument."""
    args = [x for x in args if x > -1]
    if args:
        return min(args)
    else:
        return -1


def swapPlaceholders(query):
    """Change ? to %s and :foo to %(foo)s while honoring quoting rules."""
    # This is worth optimizing at some point, but it currently takes on the
    # order of 0.1ms per conversion so it's not too significant next to the
    # actual database call.
    out = []
    # Mangle positional markers, while careful to ignore quoted sections.
    while query:
        # Find the first token
        squote = query.find("'")
        dquote = query.find('"')
        comment = query.find('--')
        start = _min(squote, dquote, comment)
        # Mangle everything before the token
        if start > 0:
            out.append(_swapOnce(query[:start]))
        elif start == -1:
            out.append(_swapOnce(query))
            break
        # Copy stuff from one token to the next, unharmed.
        if start == comment:
            end = query.find('\n', start + 2)
        elif start == squote or start == dquote:
            whichQuote = query[start]
            end = query.find(whichQuote, start + 1)
            if end == -1:
                raise ValueError("Mismatched %r quote" % (whichQuote,))

        if end == -1:
            out.append(query[start:])
            break
        else:
            out.append(query[start:end+1])
            query = query[end+1:]
    value = ''.join(out)
    return value
