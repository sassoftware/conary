# A version of difflib that does not have the recursion problem described in:
# https://issues.rpath.com/browse/RPL-1086
# https://issues.rpath.com/browse/CNY-1377

import difflib

if hasattr(difflib.SequenceMatcher, "_SequenceMatcher__helper"):
    # Old python. Use the fixed get_matching_blocks
    class SequenceMatcher(difflib.SequenceMatcher):
        def get_matching_blocks(self):
            """Return list of triples describing matching subsequences.

            Each triple is of the form (i, j, n), and means that
            a[i:i+n] == b[j:j+n].  The triples are monotonically increasing in
            i and in j.  New in Python 2.5, it's also guaranteed that if
            (i, j, n) and (i', j', n') are adjacent triples in the list, and
            the second is not the last triple in the list, then i+n != i' or
            j+n != j'.  IOW, adjacent triples never describe adjacent equal
            blocks.

            The last triple is a dummy, (len(a), len(b), 0), and is the only
            triple with n==0.

            >>> s = SequenceMatcher(None, "abxcd", "abcd")
            >>> s.get_matching_blocks()
            [(0, 0, 2), (3, 2, 2), (5, 4, 0)]
            """

            if self.matching_blocks is not None:
                return self.matching_blocks
            la, lb = len(self.a), len(self.b)

            # This is most naturally expressed as a recursive algorithm, but
            # at least one user bumped into extreme use cases that exceeded
            # the recursion limit on their box.  So, now we maintain a list
            # ('queue`) of blocks we still need to look at, and append partial
            # results to `matching_blocks` in a loop; the matches are sorted
            # at the end.
            queue = [(0, la, 0, lb)]
            matching_blocks = []
            while queue:
                alo, ahi, blo, bhi = queue.pop()
                i, j, k = x = self.find_longest_match(alo, ahi, blo, bhi)
                # a[alo:i] vs b[blo:j] unknown
                # a[i:i+k] same as b[j:j+k]
                # a[i+k:ahi] vs b[j+k:bhi] unknown
                if k:   # if k is 0, there was no matching block
                    matching_blocks.append(x)
                    if alo < i and blo < j:
                        queue.append((alo, i, blo, j))
                    if i+k < ahi and j+k < bhi:
                        queue.append((i+k, ahi, j+k, bhi))
            matching_blocks.sort()

            # It's possible that we have adjacent equal blocks in the
            # matching_blocks list now.  Starting with 2.5, this code was added
            # to collapse them.
            i1 = j1 = k1 = 0
            non_adjacent = []
            for i2, j2, k2 in matching_blocks:
                # Is this block adjacent to i1, j1, k1?
                if i1 + k1 == i2 and j1 + k1 == j2:
                    # Yes, so collapse them -- this just increases the length of
                    # the first block by the length of the second, and the first
                    # block so lengthened remains the block to compare against.
                    k1 += k2
                else:
                    # Not adjacent.  Remember the first block (k1==0 means it's
                    # the dummy we started with), and make the second block the
                    # new block to compare against.
                    if k1:
                        non_adjacent.append((i1, j1, k1))
                    i1, j1, k1 = i2, j2, k2
            if k1:
                non_adjacent.append((i1, j1, k1))

            non_adjacent.append( (la, lb, 0) )
            self.matching_blocks = non_adjacent
            return self.matching_blocks

        # Mark that we fixed difflib
        del difflib.SequenceMatcher._SequenceMatcher__helper

    # Replace the SequenceMatcher class in the global difflib, so we don't
    # have to fix unified_diff
    difflib.SequenceMatcher = SequenceMatcher

unified_diff = difflib.unified_diff
