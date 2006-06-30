#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (C) 2005-2006 rPath, Inc.
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

"""
Keeps jira up to date with repositories.  Needs to be driven with
a script that contains local information; something like the following:

#!/usr/bin/python2.4

import os
import re
import sys
from conary import conaryjira

def getPerson(name, labelText):
    for person, exp in personRe:
        if exp.match(name):
            return person
    return None

personRe = (
    ('<email-jira-id>', re.compile(r'<contact-info-regular-expression>')),
    ...
)

def main(argv):

    test = False
    if '--test' in argv:
        test = True

    updatelist = [
        ('<label>', '<name>'),
    ]

    mine = conaryjirz.jiraMine('<password>',
                               getPerson=getPerson)

    for label, product in updatelist:
        mine.mineLabel(label, product)
        if not test:
            mine.processAssignments(product)

if __name__ == "__main__":
    sys.exit(main(sys.argv))
"""

import os
import re
import sys
import textwrap

from conary import conarycfg
from conary import conaryclient
from conary import dbstore
from conary import updatecmd
from conary import versions
from conary.deps import deps
from conary.lib import util
from conary.local import database
from conary.repository import netclient

class jiraMine:

    def __init__(self, passwd = None, getPerson=None,
                 host='localhost', user='bugs', database='jira',
                 verbose=False):

        self.getPerson=getPerson
        self.verbose=verbose
        self.sourceMap = {}
        self.sourceOwner = {}
        self.personIdMap = None
        self.productIdMap = None
        if not passwd:
            self.db = dbstore.connect("%s@%s/%s" % (user, host, database),
                                      driver = 'mysql' )
        else:
            self.db = dbstore.connect("%s:%s@%s/%s" %
                                            (user, passwd, host, database),
                                      driver = 'mysql' )

        cfg = conarycfg.ConaryConfiguration()
        client = conaryclient.ConaryClient(cfg)
        self.repos = client.getRepos()
        self.indentWrapper = textwrap.TextWrapper(
            subsequent_indent='    ',
            break_long_words=False
        )


    def log(self, message):
        if self.verbose:
            print message




    def _getIdMap(self, selection, table):
        cu = self.db.cursor()

        idMap = {}
        cu.execute("SELECT %s FROM %s" %(selection, table))
        for r in cu:
            idMap[r[1]] = r[0]

        return idMap

    def getPersonIdMap(self):
        if not self.personIdMap:
            self.personIdMap = self._getIdMap('id, username', 'userbase')
        return self.personIdMap

    def getProjectIdMap(self):
        if not self.productIdMap:
            self.productIdMap = self._getIdMap('id, pname', 'project')
        return self.productIdMap




    def mineLabel(self, labelText, jiraProject):

        print 'Looking at %s product...' %jiraProject

        sourceMap = {}
        sourceOwner = {}
        label = versions.Label(labelText)

        repoPkgs = frozenset([ x for x in self.repos.troveNames(label) if ':' not in x and not (x.startswith('cross-') or x.startswith('bootstrap-') or x.startswith('group-')) ])

        cu = self.db.cursor()
        cu.execute("""SELECT component.cname
                      FROM component, project
                      WHERE component.project = project.id
                        AND project.pname = %s""", jiraProject)
        jiraPkgs = frozenset([r[0] for r in cu.fetchall()])

        newPkgs = sorted(list(repoPkgs-jiraPkgs))

        troveVersions = self.repos.getTroveLeavesByLabel(
            dict.fromkeys(newPkgs, {label: None}))

        for troveName in newPkgs:
            self.log('checking binary package ' + troveName)
            # need latest version
            troveVersion = sorted(troveVersions[troveName].keys())[-1]
            # we only need one flavor, any flavor, to get the sourceName
            troveFlavor = troveVersions[troveName][troveVersion][0]
            trove = self.repos.getTrove(troveName, troveVersion, troveFlavor,
                                   withFiles=False)
            if trove.isRedirect():
                # We do not want to modify jira automatically when we
                # see a redirect, because the redirect may not apply to
                # all versions, and we might really want to keep existing
                # versions the same.
                self.log(' ...ignoring redirected trove ' + troveName)
                continue

            sourceName = trove.getSourceName()
            if not sourceName:
                # old package from before troveinfo
                continue
            sourceNick = sourceName.split(':')[0]
            if sourceNick in jiraPkgs:
                # database doesn't like double-adds
                self.log(' ...source trove %s already in jira' %sourceNick)
                continue
            if sourceNick in sourceMap:
                sourceMap[sourceNick][trove.getName()] = True
                # only investigate each source trove once
                self.log(' ...already checked source trove ' + sourceNick)
                continue
            sourceMap[sourceNick] = {trove.getName(): True}

            sourceVerList = self.repos.getTroveVersionsByLabel(
                {sourceName: {label : None} })
            sourceVerList = sorted(sourceVerList[sourceName].keys())
            l = []
            for sourceVer in sourceVerList:
                l.extend(((sourceName, sourceVer, deps.Flavor()),))
            sourceTroves = self.repos.getTroves(l)

            personMap = {}
            firstPerson = None
            for sourceTrove in sourceTroves:
                cl = sourceTrove.getChangeLog()
                person = self.getPerson(cl.getName(), labelText)
                if not firstPerson:
                    firstPerson = person
                if person in personMap:
                    personMap[person] += 1
                else:
                    personMap[person] = 1
            if firstPerson:
                # original committer is more likely to be the responsible party
                personMap[firstPerson] += 3

            candidate = sorted(personMap.items(), key=lambda x: x[1])[-1][0]
            if not candidate:
                print "No best owner recognized for %s" %sourceNick
                continue
            sourceOwner[sourceNick] = candidate
            print " Best owner for source %s is %s" %(
                    sourceNick, sourceOwner[sourceNick])

        self.sourceMap[jiraProject] = sourceMap
        self.sourceOwner[jiraProject] = sourceOwner



    def processAssignments(self, jiraProject):
        mailMap = {}
        cu = self.db.cursor()
        sourceMap = self.sourceMap[jiraProject]
        sourceOwner = self.sourceOwner[jiraProject]

        personIdMap = self.getPersonIdMap()
        projectIdMap = self.getProjectIdMap()

        for sourceNick in sourceMap:
            if sourceNick not in sourceOwner:
                continue
            if not sourceMap[sourceNick]:
                continue
            thisSourceOwner = sourceOwner[sourceNick]
            troveMap = sourceMap[sourceNick]
            troveList = sorted(troveMap.keys())
            if len(troveList) > 1:
                desc = ', '.join(troveList) + ' packages'
            else:
                desc = troveList[0] + ' package'
            print "assigning %s in %s to %s" %(
                sourceNick, jiraProject, thisSourceOwner)
            cu.execute("INSERT INTO component"
                       "(cname, lead,  description, project)"
                       " VALUES ('%s', '%s', '%s', '%s');" %(
                       sourceNick, personIdMap[thisSourceOwner], desc,
                       projectIdMap[jiraProject]))
            if thisSourceOwner in mailMap:
                mailMap[thisSourceOwner].append(sourceNick)
            else:
                mailMap[thisSourceOwner] = [sourceNick]

        self.db.commit()

        for Owner in mailMap:
            os.system('''mail %s -s "new packages assigned to you in jira project %s" <<EOF
    %s
EOF
            ''' %(Owner, jiraProject,
                  self.indentWrapper.fill(', '.join(mailMap[Owner]))))
