#!/usr/bin/python
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


import codecs
import os
import re
import sys
import textwrap
import time
from mercurial import hg, ui


PRODUCT_NAME = "Conary"
HEADINGS = [
        ('feature', 'New Features'),
        ('api', 'API Changes'),
        ('bugfix', 'Bug Fixes'),
        ('internal', 'Internal Changes'),
        ]
KINDS = set(x[0] for x in HEADINGS)
NEWSDIR = 'NEWS.src'

RE_ISSUE = re.compile('^[A-Z0-9]+-\d+')


def main():
    rootdir = os.path.realpath(__file__ + '/../..')
    os.chdir(rootdir)

    if not os.path.isdir(NEWSDIR):
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
        out, htmlOut, _ = preview(repo)
        print 'Text Version:\n'
        for line in out:
            print line
        print 'Html Version:\n'
        for line in htmlOut:
            print line
    else:
        sys.exit("Usage: %s <preview|generate>" % sys.argv[0])


def preview(repo, modifiedOK=True):
    mod, add, rem, del_, unk, ign, cln = repo.status(clean=True)
    ok = set(cln)
    bad = set(mod + add + rem + del_)

    kind_map = {}
    files = set()
    for filename in os.listdir(NEWSDIR):
        path = '/'.join((NEWSDIR, filename))
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
            modified = _firstModified(repo, path)

        entries = [x.replace('\n', ' ') for x in
                   codecs.open(path, 'r', 'utf8').read().split('\n\n')]
        for n, line in enumerate(entries):
            entry = line.strip()
            if entry:
                kind_map.setdefault(kind, []).append((modified, issue, n,
                    entry))

    out = ['Changes in %s:' % _getVersion()]
    htmlOut = ['<p>%s %s is a maintenance release</p>' % (PRODUCT_NAME,
                                                           _getVersion())]
    for kind, heading in HEADINGS:
        entries = kind_map.get(kind, ())
        if not entries:
            continue
        out.append('  o %s:' % heading)
        htmlOut.append('<strong>%s:</strong>' % heading)
        htmlOut.append("<ul>")
        for _, issue, _, entry in sorted(entries):
            htmlEntry = '    <li>' + entry
            if RE_ISSUE.match(issue):
                entry += ' (%s)' % issue
                htmlEntry += ' (<a href="https://issues.rpath.com/browse/%s">%s</a>)' % (issue,issue)
            lines = textwrap.wrap(entry, 66)
            out.append('    * %s' % (lines.pop(0),))
            for line in lines:
                out.append('      %s' % (line,))
            htmlEntry += '</li>'
            htmlOut.append(htmlEntry)
        out.append('')
        htmlOut.append('</ul>')
    return out, htmlOut, files


def generate(repo):
    version = _getVersion()
    old = codecs.open('NEWS', 'r', 'utf8').read()
    if '@NEW@' in old:
        sys.exit("error: NEWS contains a @NEW@ section")
    elif ('Changes in %s:' % version) in old:
        sys.exit("error: NEWS already contains a %s section" % version)

    lines, htmlLines, files = preview(repo, modifiedOK=False)
    new = '\n'.join(lines) + '\n'
    newHtml = '\n'.join(htmlLines) + '\n'

    doc = new + old
    codecs.open('NEWS', 'w', 'utf8').write(doc)
    codecs.open('NEWS.html', 'w', 'utf8').write(newHtml)

    sys.stdout.write(new)
    print >> sys.stderr, "Updated NEWS"
    print >> sys.stderr, "Wrote NEWS.html"

    wlock = repo.wlock()
    try:
        for name in files:
            os.unlink(name)
            repo.dirstate.remove(name)
    finally:
        wlock.release()
    print >> sys.stderr, "Deleted %s news fragments" % len(files)


def _firstModified(repo, path):
    fl = repo.file(path)
    ctx = repo[fl.linkrev(0)]
    return ctx.date()[0]


def _getVersion():
    f = os.popen("make show-version")
    return f.read().strip()


main()
