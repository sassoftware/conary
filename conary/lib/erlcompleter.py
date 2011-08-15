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


import rlcompleter

class ECompleter(rlcompleter.Completer):

    def global_matches(self, text):
        """Compute matches when text is a simple name.

        Return a list of all keywords, built-in functions and names currently
        defined in self.namespace that match.

        """
# lines commented out make tab completion match keywords and builtins,
# which we don't want
#        import keyword
        matches = []
        n = len(text)
        for list in [
#                   keyword.kwlist,
#                     __builtin__.__dict__,
                     self.namespace]:
            for word in list:
                if word[:n] == text and word != "__builtins__":
                    matches.append(word)
        return matches
