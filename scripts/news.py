#!/usr/bin/python
#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import codecs
import os
import sys
import textwrap
import time
from mercurial import hg, ui


HEADINGS = [
        ('feature', 'New Features'),
        ('bugfix', 'Bug Fixes'),
        ('internal', 'Internal Changes'),
        ]
KINDS = set(x[0] for x in HEADINGS)


def main():
    rootdir = os.path.realpath(__file__ + '/../..')
    os.chdir(rootdir)

    if not os.path.isdir('news'):
        sys.exit("Can't find news directory")

    repo = hg.repository(ui.ui(), '.')

    args = sys.argv[1:]
    if args:
        command = args.pop(0)
    else:
        command = 'preview'

    if command == 'generate':
        generate(repo)
    elif command == 'preview':
        for line in preview(repo)[0]:
            print line
    else:
        sys.exit("Usage: %s <preview|generate>" % sys.argv[0])


def preview(repo, modifiedOK=True):
    mod, add, rem, del_, unk, ign, cln = repo.status(clean=True)
    ok = set(cln)
    bad = set(mod + add + rem + del_)

    kind_map = {}
    files = set()
    for filename in os.listdir('news'):
        path = 'news/' + filename
        if filename[0] == '.' or '.' not in filename:
            continue
        issue, kind = filename.rsplit('.', 1)
        if kind not in KINDS:
            print >> sys.stderr, "Ignoring '%s' due to unknown type '%s'" % (
                    filename, kind)
            continue

        if path in bad:
            if modifiedOK:
                print >> sys.stderr, "warning: '%s' is modified." % (path,)
                modified = time.time()
            else:
                sys.exit("File '%s' is modified and must be committed first." %
                        (path,))
        elif path not in ok:
            if modifiedOK:
                print >> sys.stderr, "warning: '%s' is not checked in." % (
                        path,)
                modified = time.time()
            else:
                sys.exit("File '%s' is not checked in and must be "
                        "committed first." % (path,))
        else:
            files.add(path)
            modified = _lastModified(repo, path)

        for n, line in enumerate(codecs.open(path, 'r', 'utf8')):
            entry = line.strip()
            if entry:
                kind_map.setdefault(kind, []).append((modified, issue, n,
                    entry))

    out = ['Changes in %s:' % _getVersion()]
    for kind, heading in HEADINGS:
        entries = kind_map.get(kind, ())
        if not entries:
            continue
        out.append('  o %s:' % heading)
        for _, issue, _, entry in sorted(entries):
            if not issue.startswith('misc-'):
                entry += ' (%s)' % issue
            lines = textwrap.wrap(entry, 66)
            out.append('    * %s' % (lines.pop(0),))
            for line in lines:
                out.append('      %s' % (line,))
        out.append('')
    return out, files


def generate(repo):
    version = _getVersion()
    old = open('NEWS').read()
    if '@NEW@' in old:
        sys.exit("error: NEWS contains a @NEW@ section")
    elif ('Changes in %s:' % version) in old:
        sys.exit("error: NEWS already contains a %s section" % version)

    lines, files = preview(repo, modifiedOK=False)
    new = '\n'.join(lines) + '\n'

    doc = new + old
    open('NEWS', 'w').write(doc)

    sys.stdout.write(new)
    print >> sys.stderr, "Updated NEWS"

    repo.remove(files, unlink=True)
    print >> sys.stderr, "Deleted %s news fragments" % len(files)


def _lastModified(repo, path):
    filenodes = []
    for cp in repo[None].parents():
        if not cp:
            continue
        filenodes.append(cp.filenode(path))
    assert len(filenodes) == 1
    fl = repo.file(path)
    ctx = repo[fl.linkrev(fl.rev(filenodes[0]))]
    return ctx.date()[0]


def _getVersion():
    f = os.popen("make show-version")
    return f.read().strip()


main()
