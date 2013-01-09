#!/usr/bin/env python
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


import os
import sys
import smtplib
import traceback
# python 2.4 does not have email.mime; email.MIMEText is available in 2.6
from email.MIMEText import MIMEText

if 'CONARY_PATH' in os.environ:
    sys.path.insert(0, os.environ['CONARY_PATH'])
    sys.path.insert(0, os.environ['CONARY_PATH']+"/scripts")
            
import tempfile
import textwrap

from conary import checkin
from conary.lib import options

def usage(exitcode=1):
    sys.stderr.write("\n".join((
     "Usage: commitaction [commitaction args] ",
     "         --module '/path/to/changemail [--sourceuser <user>]",
     "         [--mailhost <mailhost>] ",
     "         [--from <fromaddress>] [--binaryuser <user>] ",
     "         [--user <user>] [--email <email>]*'",
     ""
    )))
    return exitcode

def fail(code, srcMap, pkgMap, grpMap, argv):
    print >>sys.stderr, "An error occurred while processing changemail.  Code: %d" % code
    print >>sys.stderr, "    srcMap=%s" % srcMap.items()
    print >>sys.stderr, "    pkgMap=%s" % pkgMap.items()
    print >>sys.stderr, "    grpMap=%s" % grpMap.items()
    print >>sys.stderr, "    argv=%s" % argv
    sys.stderr.flush()

def process(repos, cfg, commitList, srcMap, pkgMap, grpMap, argv, otherArgs):
    if not len(argv) and not len(otherArgs):
        return usage()

    argDef = {
        'user': options.ONE_PARAM,
        'sourceuser': options.ONE_PARAM,
        'binaryuser': options.ONE_PARAM,
        'from': options.ONE_PARAM,
        'email': options.MULT_PARAM,
        'maxsize': options.ONE_PARAM,
        'mailhost': options.ONE_PARAM,
    }

    # create an argv[0] for processArgs to ignore
    argv[0:0] = ['']
    argSet, someArgs = options.processArgs(argDef, {}, cfg, usage, argv=argv)
    # and now remove argv[0] again
    argv.pop(0)
    if len(someArgs):
        someArgs.pop(0)
    otherArgs.extend(someArgs)

    if 'email' in argSet:
        argSet['email'].extend(otherArgs)
    else:
        if otherArgs:
            argSet['email'] = otherArgs
        else:
            return usage()

    sourceuser = None
    binaryuser = None
    fromaddr = None
    maxsize = None
    if 'sourceuser' in argSet:
        sourceuser = argSet['sourceuser']
    if 'binaryuser' in argSet:
        binaryuser = argSet['binaryuser']
    if not sourceuser and 'user' in argSet:
        sourceuser = argSet['user']
    if not binaryuser and 'user' in argSet:
        binaryuser = argSet['user']
    if 'from' in argSet:
        fromaddr = argSet['from']
    if 'maxsize' in argSet:
        maxsize = int(argSet['maxsize'])

    pid = os.fork()
    if not pid:
        #child 1
        pid2 = os.fork()
        if not pid2:
            #child 2
            exitCode = doWork(repos, cfg, srcMap, pkgMap, grpMap, sourceuser, binaryuser, fromaddr, maxsize, argSet)
            sys.exit(exitCode)
        else:
            #parent 2
            pid2, status = os.waitpid(pid2, 0)
            if status:
                fail(status, srcMap, pkgMap, grpMap, argv)
            sys.exit(0)
    return 0


def doWork(repos, cfg, srcMap, pkgMap, grpMap, sourceuser, binaryuser, fromaddr, maxsize, argSet):
    exitCode = 0
    tmpfd, tmppath = tempfile.mkstemp('', 'changemail-')
    os.unlink(tmppath)
    tmpfile = os.fdopen(tmpfd)
    sys.stdout.flush()
    oldStdOut = os.dup(sys.stdout.fileno())
    os.dup2(tmpfd, 1)
    mailhost = argSet.pop('mailhost', 'localhost')

    if srcMap:
        sources = sorted(srcMap.keys())
        names = [ x.split(':')[0] for x in sources ]
        subjectList = []
        for sourceName in sources:
            for ver, shortver in srcMap[sourceName]:
                subjectList.append('%s=%s' %(
                    sourceName.split(':')[0], shortver))
        subject = 'Source: %s' %" ".join(subjectList)

        for sourceName in sources:
            for ver, shortver in srcMap[sourceName]:
                new = repos.findTrove(cfg.buildLabel, (sourceName, ver, None))
                newV = new[0][1]
                old, oldV = checkin.findRelativeVersion(repos, sourceName,
                                                        1, newV)
                if old:
                    old = ' (previous: %s)'%oldV.trailingRevision().asString()
                else:
                    old = ''
                print '================================'
                print '%s=%s%s' %(sourceName, shortver, old)
                print 'cvc rdiff %s -1 %s' %(sourceName[:-7], ver)
                print '================================'
                try:
                    checkin.rdiff(repos, cfg.buildLabel, sourceName, '-1', ver)
                except:
                    exitCode = 2
                    print 'rdiff failed for %s' %sourceName
                    try:
                        t, v, tb = sys.exc_info()
                        tbd = traceback.format_exception(t, v, tb)
                        sys.stdout.write(''.join(tbd[-min(2, len(tbd)):]))
                        sys.stderr.write(''.join(tbd))
                    except:
                        print 'Failed to print exception information'

                    print ''
                    print 'Please include a copy of this message in an issue'
                    print 'filed at https://issues.rpath.com/'
                print
        if sourceuser:
            print 'Committed by: %s' %sourceuser

        sendMail(tmpfile, subject, fromaddr, maxsize, argSet['email'], mailhost)

    if pkgMap or grpMap:
        # stdout is the tmpfile
        sys.stdout.flush()
        sys.stdout.seek(0)
        sys.stdout.truncate()

        binaries = sorted(pkgMap.keys())
        groups = sorted(grpMap.keys())
        subject = 'Binary: %s' %" ".join(binaries+groups)

        wrap = textwrap.TextWrapper(
            initial_indent='    ',
            subsequent_indent='        ',
        )

        if binaries:
            print "Binary package commits:"
            if binaryuser:
                print 'Committed by: %s' %binaryuser
        for package in binaries:
            for version in sorted(pkgMap[package].keys()):
                print '================================'
                print '%s=%s' %(package, version)
                flavorDict = pkgMap[package][version]
                for flavor in sorted(flavorDict.keys()):
                    print wrap.fill('%s:%s [%s]' %(package,
                        ' :'.join(sorted(flavorDict[flavor])),
                        ', '.join(flavor.split(','))))
                print

        if groups:
            print "Group commits:"
        for group in groups:
            for version in sorted(grpMap[group].keys()):
                print '================================'
                print '%s=%s' %(group, version)
                flavorSet = grpMap[group][version]
                for flavor in sorted(flavorSet):
                    print wrap.fill('[%s]' %
                        ', '.join(flavor.split(',')))
                print

        sendMail(tmpfile, subject, fromaddr, maxsize, argSet['email'], mailhost)
        os.dup2(oldStdOut, 1)

    return exitCode

def sendMail(tmpfile, subject, fromaddr, maxsize, addresses, mailhost='localhost'):
    # stdout is the tmpfile, so make sure it has been flushed!
    sys.stdout.flush()

    if maxsize:
        tmpfile.seek(0, 2)
        size = tmpfile.tell()
        if size > maxsize:
            tmpfile.truncate(maxsize-6)
            tmpfile.seek(0, 2)
            tmpfile.write('\n...\n')

    if not fromaddr:
        fromaddr = 'root@localhost'

    s = smtplib.SMTP()
    s.connect(mailhost)
    for address in addresses:
        # explicitly set different To addresses in different messages
        # in case some recipient addresses are not intended to be exposed
        # to other recipients
        tmpfile.seek(0)
        msg = MIMEText(tmpfile.read())
        msg['Subject'] = subject
        msg['From'] = fromaddr
        msg['To'] = address

        s.sendmail(fromaddr, [address], msg.as_string())

    s.quit()

if __name__ == "__main__":
    sys.exit(usage())
