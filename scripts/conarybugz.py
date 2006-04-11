#!/usr/bin/python
# -*- mode: python -*-
#
# Copyright (c) 2005 rPath, Inc.
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
Keeps bugzilla up to date with repositories.  Needs to be driven with
a script that contains local information; something like the following:

#!/usr/bin/python

import os
import re
import sys
from conary import conarybugz

def getPerson(name, labelText):
    for person, exp in personRe:
        if exp.match(name):
            return person
    return None

personRe = (
    ('<email-bugzilla-id>', re.compile(r'<contact-info-regular-expression>')),
    ...
)

def main(argv):

    test = False
    if '--test' in argv:
        test = True

    updatelist = [
        ('<label>', '<name>'),
    ]

    mine = conarybugz.bugzMine('<password>',
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
import MySQLdb
import textwrap

from conary import conarycfg
from conary import conaryclient
from conary import updatecmd
from conary import versions
from conary.deps import deps
from conary.lib import util
from conary.local import database
from conary.repository import netclient

class bugzMine:

    def __init__(self, passwd, getPerson=None,
                 host='localhost', user='bugs', database='bugs',
                 verbose=False):

        self.getPerson=getPerson
        self.verbose=verbose
        self.sourceMap = {}
        self.sourceOwner = {}
        self.personIdMap = None
        self.productIdMap = None
        self.db = MySQLdb.connect(host=host, user=user,
                                  passwd=passwd, db=database)
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
        for r in cu.fetchall():
            idMap[r[1]] = r[0]

        return idMap

    def getPersonIdMap(self):
        if not self.personIdMap:
            self.personIdMap = self._getIdMap('userid, login_name', 'profiles')
        return self.personIdMap

    def getProductIdMap(self):
        if not self.productIdMap:
            self.productIdMap = self._getIdMap('id, name', 'products')
        return self.productIdMap




    def mineLabel(self, labelText, bugzillaProduct):

        print 'Looking at %s product...' %bugzillaProduct

        sourceMap = {}
        sourceOwner = {}
        label = versions.Label(labelText)

        repoPkgs = frozenset([ x for x in self.repos.troveNames(label) if ':' not in x and not (x.startswith('cross-') or x.startswith('bootstrap-') or x.startswith('group-')) ])

        cu = self.db.cursor()
        cu.execute("""SELECT components.name
                      FROM components, products
                      WHERE components.product_id = products.id
                        AND products.name = %s""", bugzillaProduct)
        bugzPkgs = frozenset([r[0] for r in cu.fetchall()])
        
        newPkgs = sorted(list(repoPkgs-bugzPkgs))

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
                # We do not want to modify bugzilla automatically when we
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
            if sourceNick in bugzPkgs:
                # database doesn't like double-adds
                self.log(' ...source trove %s already in bugzilla' %sourceNick)
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
                l.extend(((sourceName, sourceVer, deps.DependencySet()),))
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
                personMap[firstPerson] += 4

            candidate = sorted(personMap.items(), key=lambda x: x[1])[-1][0]
            if not candidate:
                print "No best owner recognized for %s" %sourceNick
                continue
            sourceOwner[sourceNick] = candidate
            print " Best owner for source %s is %s" %(
                    sourceNick, sourceOwner[sourceNick])

        self.sourceMap[bugzillaProduct] = sourceMap
        self.sourceOwner[bugzillaProduct] = sourceOwner



    def processAssignments(self, bugzillaProduct):
        mailMap = {}
        cu = self.db.cursor()
        sourceMap = self.sourceMap[bugzillaProduct]
        sourceOwner = self.sourceOwner[bugzillaProduct]

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
                sourceNick, bugzillaProduct, thisSourceOwner)
            cu.execute("INSERT INTO components"
                       "(name, initialowner, initialqacontact, description, product_id, id)"
                       " VALUES ('%s', '%s', '0', '%s', '%s', NULL);" %(
                       sourceNick, self.getPersonIdMap()[thisSourceOwner], desc,
                       self.getProductIdMap()[bugzillaProduct]))
            if thisSourceOwner in mailMap:
                mailMap[thisSourceOwner].append(sourceNick)
            else:
                mailMap[thisSourceOwner] = [sourceNick]

        for Owner in mailMap:
            os.system('''mail %s -s "new packages assigned to you in bugzilla product %s" <<EOF
    %s
EOF
            ''' %(Owner, bugzillaProduct,
                  self.indentWrapper.fill(', '.join(mailMap[Owner]))))




    def reportAssignments(self, bugzillaProduct, userList):
        cu = self.db.cursor()
        cu.execute("""SELECT COUNT(components.name) AS count, profiles.login_name
                      FROM components, profiles
                      WHERE components.product_id='%s'
                        AND profiles.userid=components.initialowner
                      GROUP BY profiles.login_name
                      ORDER BY count DESC;""" %(
                   self.getProductIdMap()[bugzillaProduct]))
        print 'Bug assignment totals for %s:' %bugzillaProduct
        for count, login in cu.fetchall():
            print '%5s %s' %(count, login)

        print '\n\nComplete Bug Assignment List by User for %s:' %bugzillaProduct
        for user in userList:
            cu.execute("""SELECT components.name
                          FROM components, profiles
                          WHERE components.initialowner=profiles.userid
                            AND profiles.login_name
                            LIKE '%s%%' and components.product_id='%s';""" %(
                       user, self.getProductIdMap()[bugzillaProduct]))
            pkgList = [ x[0] for x in cu.fetchall() ]
            msg = "%s: %s" %(user, ', '.join(pkgList))
            print self.indentWrapper.fill(msg)
            print
