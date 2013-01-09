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
