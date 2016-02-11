#!/usr/bin/python
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


import codecs
import os
import re
import subprocess
import sys
import textwrap


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


def git(args):
    args = ['git'] + list(args)
    proc = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE)
    stdout, _ = proc.communicate()
    if proc.returncode:
        sys.exit("git exited with status code %s" % proc.returncode)
    return stdout


def ls_files():
    return set(
            git(['ls-tree', '--name-only', 'HEAD', NEWSDIR + '/']
        ).splitlines())


def ls_changed():
    return set(x[3:] for x in
            git(['status', '--porcelain', NEWSDIR + '/']
        ).splitlines())


def main():
    rootdir = os.path.realpath(__file__ + '/../..')
    os.chdir(rootdir)

    if not os.path.isdir(NEWSDIR):
        sys.exit("Can't find news directory")

    args = sys.argv[1:]
    if args:
        command = args.pop(0)
    else:
        command = 'preview'

    if command == 'generate':
        generate()
    elif command == 'preview':
        out, htmlOut, _ = preview()
        print 'Text Version:\n'
        for line in out:
            print line
        print 'Html Version:\n'
        for line in htmlOut:
            print line
    else:
        sys.exit("Usage: %s <preview|generate>" % sys.argv[0])


def preview(modifiedOK=True):
    existing = ls_files()
    changed = ls_changed()
    ok = existing - changed

    kind_map = {}
    files = set()
    for filename in sorted(os.listdir(NEWSDIR)):
        path = '/'.join((NEWSDIR, filename))
        if filename[0] == '.' or '.' not in filename:
            continue
        issue, kind = filename.rsplit('.', 1)
        if kind not in KINDS:
            print >> sys.stderr, "Ignoring '%s' due to unknown type '%s'" % (
                    filename, kind)
            continue

        if path in changed:
            if modifiedOK:
                print >> sys.stderr, "warning: '%s' is modified." % (path,)
            else:
                sys.exit("File '%s' is modified and must be committed first." %
                        (path,))
        elif path not in ok:
            if modifiedOK:
                print >> sys.stderr, "warning: '%s' is not checked in." % (
                        path,)
            else:
                sys.exit("File '%s' is not checked in and must be "
                        "committed first." % (path,))
        else:
            files.add(path)

        entries = [x.replace('\n', ' ') for x in
                   codecs.open(path, 'r', 'utf8').read().split('\n\n')]
        for n, line in enumerate(entries):
            entry = line.strip()
            if entry:
                kind_map.setdefault(kind, []).append((issue, n, entry))

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
        for issue, _, entry in sorted(entries):
            htmlEntry = '    <li>' + entry
            if RE_ISSUE.match(issue):
                entry += ' (%s)' % issue
                htmlEntry += ' (<a href="https://opensource.sas.com/its/browse/%s">%s</a>)' % (issue,issue)
            elif issue.isdigit():
                entry += ' (#%s)' % issue
                htmlEntry += ' (<a href="https://github.com/sassoftware/conary/issues/%s">#%s</a>)' % (issue,issue)
            lines = textwrap.wrap(entry, 66)
            out.append('    * %s' % (lines.pop(0),))
            for line in lines:
                out.append('      %s' % (line,))
            htmlEntry += '</li>'
            htmlOut.append(htmlEntry)
        out.append('')
        htmlOut.append('</ul>')
    return out, htmlOut, files


def generate():
    version = _getVersion()
    old = codecs.open('NEWS', 'r', 'utf8').read()
    if '@NEW@' in old:
        sys.exit("error: NEWS contains a @NEW@ section")
    elif ('Changes in %s:' % version) in old:
        sys.exit("error: NEWS already contains a %s section" % version)

    lines, htmlLines, files = preview(modifiedOK=False)
    new = '\n'.join(lines) + '\n'
    newHtml = '\n'.join(htmlLines) + '\n'

    doc = new + old
    codecs.open('NEWS', 'w', 'utf8').write(doc)
    codecs.open('NEWS.html', 'w', 'utf8').write(newHtml)

    sys.stdout.write(new)
    print >> sys.stderr, "Updated NEWS"
    print >> sys.stderr, "Wrote NEWS.html"

    git(['rm'] + sorted(files))
    git(['add', 'NEWS'])
    print >> sys.stderr, "Deleted %s news fragments" % len(files)
    os.system("git status")


def _getVersion():
    f = os.popen("make show-version")
    return f.read().strip()


main()
