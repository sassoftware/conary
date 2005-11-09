# -*- mode: python -*-
#
# Copyright (c) 2004 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.opensource.org/licenses/cpl.php.
#
# This program is distributed in the hope that it will be useful, but
# without any waranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#
"""
Generates an XML trove description file.
"""

import sys
import xmlrpclib

from conary import conarycfg
from conary import metadata
from conary.lib import log
from conary.lib import options
from conary.lib import util
from conary.repository import netclient

argDef = {}
argDef['dir'] = 1

sys.excepthook = util.genExcepthook()
def usage(rc = 1):
    print "usage: cvcdesc --source <freshmeat|...> --pkg-name <package name> <outputFileName>"
    print "                          or"
    print "               --short-desc <string>"
    print "               [--long-desc <string>]"
    print "               [--url <url>]+"
    print "               [--license <license>]+"
    print "               [--category <category>]+"
    print "               [--language <language code>]+"
    print "               <outputFileName>"
    
    return rc

def realMain(cfg, argv=sys.argv):
    argDef = {}
    cfgMap = {}

    (NO_PARAM,  ONE_PARAM)  = (options.NO_PARAM, options.ONE_PARAM)
    (OPT_PARAM, MULT_PARAM) = (options.OPT_PARAM, options.MULT_PARAM)

    argDef["source"] = ONE_PARAM
    argDef["pkg-name"] = ONE_PARAM
    argDef["long-desc"] = ONE_PARAM
    argDef["short-desc"] = ONE_PARAM
    argDef["url"] = MULT_PARAM
    argDef["license"] = MULT_PARAM
    argDef["category"] = MULT_PARAM

    argDef.update(argDef)

    try:
        argSet, otherArgs = options.processArgs(argDef, cfgMap, cfg, usage,
                                                argv=argv)
    except options.OptionError, e:
        sys.exit(e.val)
    except versions.ParseError, e:
        print >> sys.stderr, e
        sys.exit(1)

    if argSet.has_key('version'):
        print constants.version
        sys.exit(0)

    args = otherArgs[1:]
    log.setVerbosity(1)

    if len(args) != 1:
        return usage()

    longDesc = ""
    urls = []
    categories = []
    licenses = []
    source = "local"
    language = "C"
    
    outFile = args[0]

    if "source" in argSet:
        if argSet["source"] == "freshmeat":
            if "pkg-name" not in argSet:
                log.info("package name required when using external source")
            else:
                fmName = argSet["pkg-name"]

            log.info("fetching metadata from %s", argSet["source"])

            try:
                md = metadata.fetchFreshmeat(fmName)
            except metadata.NoFreshmeatRecord:
                log.error("no freshmeat record found for %s", fmName)
                return
            log.info("found record: '%s'", md.getShortDesc())
        else:
            log.error("unsupported metadata source: %s", argSet["source"])

        shortDesc = md.getShortDesc()
        longDesc = md.getLongDesc()
        urls = md.getUrls()
        categories = md.getCategories()
        licenses = md.getLicenses()
        source = md.getSource()
        language = "C"
    else:
        if "short-desc" in argSet:
            shortDesc = argSet["short-desc"]
        else:
            log.error("short description must be specified")
            return 0
        
        if "long-desc" in argSet:   longDesc = argSet["long-desc"]
        if "url" in argSet:         urls = argSet["url"]
        if "category" in argSet:    categories = argSet["category"]
        if "license" in argSet:     licenses = argSet["license"]
        if "language" in argSet:    language = argSet["language"]

    xml = """<trove>
    <shortDesc>%s</shortDesc>
    <longDesc>%s</longDesc>
    <source>%s</source>
    <language>%s</language>
""" % (shortDesc, longDesc, source, language)

    for url in urls:
        xml += "    <url>%s</url>\n" % url
    for license in licenses:
        xml += "    <license>%s</license>\n" % license
    for category in categories:
        xml += "    <category>%s</category>\n" % category

    xml += "</trove>\n"

    f = open(outFile, "w")
    f.write(xml)
    f.close()
    log.info("metadata written to %s", outFile)
    return 0

def main(argv=sys.argv):
    try:
        if '--skip-default-config' in argv:
            argv = argv[:]
            argv.remove('--skip-default-config')
            cfg = conarycfg.ConaryConfiguration(False)
        else:
            cfg = conarycfg.ConaryConfiguration()
        # reset the excepthook (using cfg values for exception settings)
        sys.excepthook = util.genExcepthook(cfg.dumpStackOnError)
	realMain(cfg, argv)
    except conarycfg.ConaryCfgError, e:
        log.error(str(e))
        sys.exit(1)

if __name__ == "__main__":
    sys.exit(main())
