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


import os
import socket
import httplib
from SimpleHTTPServer import SimpleHTTPRequestHandler

from testrunner import testhelp

from conary_test import rephelp
from conary.lib import log, util
from conary.build import lookaside

class LookAsideTest(rephelp.RepositoryHelper):
    def testLookAsideTest1(self):
        # this works only when connected to the net
        # gethostbyname will fail immediately when not connected
        try:
            socket.gethostbyname('www.conary.com')
        except:
            raise testhelp.SkipTestException
        self.resetCache()
        self.resetWork()
        self.resetRepository()
        repos = self.openRepository()
        cfg = self.cfg
        repCache = lookaside.RepositoryCache(repos, cfg=cfg)

        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('test')
        os.chdir('test')
        testdir = '/'.join((self.workDir, 'test'))

        log.setVerbosity(log.INFO)
        self.logFilter.add()
        # first, look for a file that does not exist
        assert(lookaside.findAll(self.cfg, repCache,
            'http://example.conary.com/foo', 'test', (testdir,),
            allowNone=True) is None)
        # make sure that we got a negative cache entry
        assert(os.stat('/'.join((self.cacheDir, 'NEGATIVE/test/example.conary.com/foo'))))

        # now make sure that this works for ftp as well (urllib workaround)
        #    XXX WORKAROUND until FTP works in eng lab
        #assert(lookaside.findAll(self.cfg, repCache,
        #    'ftp://download.rpath.com/blah', 'test', (testdir,),
        #     allowNone=True) is None)
        # make sure that we got a negative cache entry
        #assert(os.stat('/'.join((self.cacheDir,
        #                         'NEGATIVE/test/download.rpath.com/blah'))))

        # now we put a file in place
        f = file(os.sep.join((testdir, 'bar')), 'w')
        f.write('this is a test')
        f.close()
        c = lookaside.findAll(self.cfg, repCache, 'bar', 'test', (testdir,))
        # it does not need to cache it; it is known to exist
        assert(c == os.sep.join((testdir, 'bar')))
        
        # Test httpHeaders:
        c = util.normpath(lookaside.findAll(self.cfg, repCache,
            'http://www.google.com/preferences', 'test', (testdir,),
            httpHeaders={'Accept-Language': 'es-es'}))
        assert(c == '/'.join((self.cacheDir, 'test/www.google.com/preferences')))
        #open the page and check to see if it's in spanish
        f = open(c)
        contents = f.read()
        f.close()
        assert 'Preferencias globales' in contents

        # we need a web page to actually test the cache in operation
        # we do it a second time to make sure that the cache works
        for i in (0, 1):
            c = util.normpath(lookaside.findAll(self.cfg, repCache,
                'http://wiki.rpath.com/wiki/Main_Page', 'test', (testdir,)))
            assert(c == '/'.join((self.cacheDir, 'test/wiki.rpath.com/wiki/Main_Page')))
        self.logFilter.remove()
        self.logFilter.compare(
            ('+ Trying http://example.conary.com/foo...',
        # XXX WORKAROUND until FTP works in eng lab
             #'+ Trying ftp://download.rpath.com/blah...',
             #'+ Downloading ftp://download.rpath.com/blah...',
             '+ Trying http://www.google.com/preferences...',
             '+ Downloading http://www.google.com/preferences...',
             '+ Trying http://wiki.rpath.com/wiki/Main_Page...',
             '+ Downloading http://wiki.rpath.com/wiki/Main_Page...'))

        recipestr = """
class TestLookaside(PackageRecipe):
    name = 'test'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.addSource('bar', dest='/')
"""
        self.writeFile('test.recipe', recipestr)
        self.addfile('test.recipe')
        self.addfile('bar', binary = True)
        self.commit()
        os.chdir(origDir)

        # ensure that a localOnly=True lookup in the repository works;
        # for this, we need a prepped recipeObj for its RepositoryCache
        # object
        recipeObj = self.getRecipeObjFromRepos('test', repos)
        self.logFilter.add()
        c = lookaside.findAll(self.cfg, recipeObj.laReposCache,
                              'bar', 'test', (), localOnly=True)
        self.logFilter.remove()
        self.logFilter.compare(
            '+ found bar in repository',
        )
        assert(c == os.sep.join((self.cacheDir, 'test', 'bar')))

        # FIXME:
        # test more combinations of types of lookups


    def testRefresh(self):
        # Using 2?param specifically to verify CNY-3722
        recipeStr = """
class RefreshTest(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addSource('%s1', dir='/foo/')
        r.addSource('%s2?param', dir='/foo/')
"""
        self.resetRepository()
        cfg = self.cfg
        self.repos = self.openRepository()
        self.resetWork()
        self.resetCache()
        try:
            contentServer = rephelp.HTTPServerController(getRequester())
            contentURL = contentServer.url()

            #self.logFilter.add()
            os.chdir(self.workDir)
            self.newpkg('test')
            os.chdir('test')
            self.writeFile('test.recipe', recipeStr %(
                contentURL, contentURL))
            self.addfile('test.recipe')
            self.logCheck(self.commit, (),
                [ '. Trying http://localhost:[0-9]*/1\.\.\.',
                  '. Downloading http://localhost:[0-9]*/1\.\.\.',
                  '. Trying http://localhost:[0-9]*/2\?param\.\.\.',
                  '. Downloading http://localhost:[0-9]*/2\?param\.\.\.' ],
                regExp = True)
            self.cookItem(self.repos, cfg, 'test')
            self.updatePkg(self.workDir, 'test')
            self.verifyFile(self.workDir + '/foo/1', '/1:1\n')
            self.verifyFile(self.workDir + '/foo/2?param', '/2?param:1\n')
            self.logCheck(self.refresh, ('2*',),
                [ '. Trying http://localhost:[0-9]*/2\?param\.\.\.',
                  '. Downloading http://localhost:[0-9]*/2\?param\.\.\.' ],
                regExp = True)
            self.cookItem(self.repos, cfg, 'test.recipe')
            self.logCheck(self.commit, (),
                [ '. found http://localhost:[0-9]*/1 in repository' ], regExp = True)
            self.cookItem(self.repos, cfg, 'test')
            self.updatePkg(self.workDir, 'test')
            self.verifyFile(self.workDir + '/foo/1', '/1:1\n')
            self.verifyFile(self.workDir + '/foo/2?param', '/2?param:2\n')
            self.logCheck(self.refresh, (),
                [
                  '. Trying http://localhost:[0-9]*/1\.\.\.',
                  '. Downloading http://localhost:[0-9]*/1\.\.\.',
                  '. Trying http://localhost:[0-9]*/2\?param\.\.\.',
                  '. Downloading http://localhost:[0-9]*/2\?param\.\.\.' ],
                regExp = True)
            self.cookItem(self.repos, cfg, 'test.recipe')
            self.commit()
            self.cookItem(self.repos, cfg, 'test')
            self.updatePkg(self.workDir, 'test')
            self.verifyFile(self.workDir + '/foo/1', '/1:2\n')
            self.verifyFile(self.workDir + '/foo/2?param', '/2?param:3\n')

        finally:
            contentServer.kill()

    def testRefreshNegativeLookaside(self):
        recipeStr = """
class RefreshTest(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addSource('%s1', dir='/foo/')
        r.addSource('%s2', dir='/foo/')
"""
        self.resetRepository()
        cfg = self.cfg
        self.repos = self.openRepository()
        self.resetWork()
        self.resetCache()
        try:
            contentServer = rephelp.HTTPServerController(getRequester())
            contentURL = contentServer.url()

            #self.logFilter.add()
            os.chdir(self.workDir)
            self.newpkg('test')
            os.chdir('test')
            self.writeFile('test.recipe', recipeStr %(
                contentURL, contentURL))
            self.addfile('test.recipe')
            self.logCheck(self.commit, (),
                [ '. Trying http://localhost:[0-9]*/1\.\.\.',
                  '. Downloading http://localhost:[0-9]*/1\.\.\.',
                  '. Trying http://localhost:[0-9]*/2\.\.\.',
                  '. Downloading http://localhost:[0-9]*/2\.\.\.' ],
                regExp = True)
            self.cookItem(self.repos, cfg, 'test')

            negativePath = os.path.join(self.cacheDir, 'NEGATIVE/test',
                'localhost:%d' % contentServer.port, '2')
            util.mkdirChain(os.path.dirname(negativePath))
            file(negativePath, "w")

            self.logCheck(self.refresh, ('1', ),
                [ '. Trying http://localhost:[0-9]*/1\.\.\.',
                  '. Downloading http://localhost:[0-9]*/1\.\.\.', ],
                regExp = True)
            self.assertFalse(os.path.exists(negativePath))
        finally:
            contentServer.kill()

    def testCookies(self):
        # CNY-321
        try:
            contentServer = rephelp.HTTPServerController(cookieRequester())
            contentURL = contentServer.url()
            name = 'foo.tar.gz'
            url = contentURL + '/' + name
            cached = lookaside.fetchURL(self.cfg, url, name)
            f = open(cached, 'r')
            self.assertEqual(f.read(), 'Hello, world!\n')
        finally:
            contentServer.kill()

    def testAuth(self):
        # CNY-981
        try:
            contentServer = rephelp.HTTPServerController(authRequester())
            baseUrl = contentServer.url()

            # test with user:pass
            contentURL = 'http://user:pass@%s' % httplib.urlsplit(baseUrl)[1]
            name = 'foo.tar.gz'
            url = contentURL + '/' + name
            cached = lookaside.fetchURL(self.cfg, url, name)
            f = open(cached, 'r')
            self.assertEqual(f.read(), 'Hello, world!\n')

            # test with no password given
            contentURL = 'http://user@%s' % httplib.urlsplit(baseUrl)[1]
            name = 'foo2.tar.gz'
            url = contentURL + '/' + name
            cached = lookaside.fetchURL(self.cfg, url, name)
            f = open(cached, 'r')
            self.assertEqual(f.read(), 'Hello, world 2!\n')

            # test with no password at all
            name = 'foo3.tar.gz'
            url = baseUrl + '/' + name
            cached = self.logCheck(lookaside.fetchURL, (self.cfg, url, name),
                                   ['error: error downloading http://localhost:[0-9]*//foo3.tar.gz: HTTP Error 401: Unauthorized'],
                                   regExp=True)
            self.assertEqual(cached, None)

            # test ftp with user:pass
            def fakeOpen(od, req, *args, **kw):
                self.req = req
                import StringIO
                s = 'baz file contents'
                r = StringIO.StringIO(s)
                r.headers = {'contentLength': len(s)}
                return r

            import urllib2
            self.mock(urllib2.OpenerDirector, 'open', fakeOpen)
            url = 'ftp://user:pass@foo.com/bar/baz.tgz'
            name = 'baz.tgz'
            cached = lookaside.fetchURL(self.cfg, url, name)
            self.assertEqual(url, self.req.get_full_url())
            self.assertEqual(open(cached).read(), 'baz file contents')

        finally:
            contentServer.kill()

    def testSourceGoesAway(self):
        self.resetRepository()
        cfg = self.cfg
        self.repos = self.openRepository()
        self.resetWork()
        self.resetCache()

        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('test')
        os.chdir('test')
        testdir = '/'.join((self.workDir, 'test'))

        log.setVerbosity(log.INFO)
        self.logFilter.add()

        recipeStr = """
class SourceGoesAwayTest(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addSource('%s1', dir='/foo/')
"""
        try:
            contentServer = rephelp.HTTPServerController(getRequester())
            contentURL = contentServer.url()
            curl = contentURL + "file1.txt"

            self.writeFile('test.recipe', recipeStr %(curl, ))
            self.addfile('test.recipe')
            # Test all sorts of cooks, to make sure cooking from the recipe
            # does not re-download
            self.cookItem(self.repos, cfg, 'test.recipe')
            self.commit()

            self.cookItem(self.repos, cfg, 'test.recipe')
            self.cookItem(self.repos, cfg, 'test')
        finally:
            contentServer.kill()
            os.chdir(origDir)

        # Get rid of the cache
        self.resetWork()
        self.resetCache()

        # Server for source file is shut down now
        try:
            # Get rid of this directory
            os.chdir(self.workDir)

            self.checkout('test')
            os.chdir('test')

            # Cook the recipe, should work
            self.cookItem(self.repos, cfg, 'test.recipe')
        finally:
            os.chdir(origDir)

    def testRefreshNoAutoSource(self):
        # CNY-1160

        self.resetRepository()
        cfg = self.cfg
        self.repos = self.openRepository()
        self.resetWork()
        self.resetCache()

        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('test')
        os.chdir('test')
        testdir = '/'.join((self.workDir, 'test'))

        recipeStr = """
class SourceGoesAwayTest(PackageRecipe):
    name = 'test'
    version = '0'
    clearBuildReqs()

    def setup(r):
        r.addSource('%s1', dir='/foo/')
        r.addSource('somefile.txt')
"""
        try:
            contentServer = rephelp.HTTPServerController(getRequester())
            contentURL = contentServer.url()
            curl = contentURL + "file1.txt"

            self.writeFile('test.recipe', recipeStr %(curl, ))
            self.addfile('test.recipe')
            self.writeFile('somefile.txt', 'Some lame content\n')
            self.addfile('somefile.txt')
            expected = [
                '+ Trying %s1...' % curl,
                '+ Downloading %s1...' % curl,
            ]
            ret, out = self.captureOutput(self.commit)
            self.assertEqual(out, '\n'.join(expected) + '\n')

            expected.append('warning: somefile.txt is not autosourced and '
                            'cannot be refreshed') 

            self.logFilter.add()
            self.refresh()
            self.logFilter.compare(expected)
        finally:
            contentServer.kill()
            os.chdir(origDir)

    def testRepoCooksDoNotReDownload(self):
        # CNY-3221

        # Make sure that when cooking from the repository we do not hit the
        # server again.

        # Create archive
        file(os.path.join(self.workDir, "file1"), "w").write("cont\n")
        archiveFile = os.path.join(self.workDir, "archive-1.tar.gz")
        cmd = ["tar", "zcf", archiveFile, "-C", self.workDir, "file1"]
        p = rephelp.subprocess.Popen(cmd)
        p.communicate()
        self.assertTrue(os.path.exists(archiveFile))

        class TarGzRequestor(SimpleHTTPRequestHandler):
            requestLog = os.path.join(self.workDir, "request.log")

            # shut up!
            def log_message(self, *args, **kw):
                pass

            def do_GET(self):
                file(self.requestLog, "a").write(self.path + '\n')
                if not self.path.endswith('.tar.gz'):
                    self.send_response(404)
                    self.end_headers()
                    return
                content = file(archiveFile).read()
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Length', str(len(content)))
                self.end_headers()
                self.wfile.write(content)

        recipeStr = """
class ArchiveTest(PackageRecipe):
    name = 'archive'
    version = '1'
    clearBuildReqs()

    def setup(r):
        r.addArchive('%s/', dir='/foo/')
"""
        origDir = os.getcwd()
        os.chdir(self.workDir)
        self.newpkg('archive')
        testdir = '/'.join((self.workDir, 'archive'))

        try:
            os.chdir(testdir)
            contentServer = rephelp.HTTPServerController(TarGzRequestor)
            contentUrl = contentServer.url()

            self.writeFile('archive.recipe', recipeStr %(contentUrl, ))
            self.addfile('archive.recipe')
            expected = [
                '+ Trying %s/archive-1.tar.bz2...' % contentUrl,
                '+ Trying %s/archive-1.tar.gz...' % contentUrl,
                '+ Downloading %s/archive-1.tar.gz...' % contentUrl,
            ]
            ret, out = self.captureOutput(self.commit)
            self.assertEqual(out, '\n'.join(expected) + '\n')

            # Make sure we have the proper files in the request log
            self.assertEqual(
                [ x.strip() for x in file(TarGzRequestor.requestLog) ],
                [ '//archive-1.tar.bz2', '//archive-1.tar.gz', ])

            # Empty the request log file
            file(TarGzRequestor.requestLog, "w")

            # When cooking from the repository, we should not hit the web
            # server again
            ret = self.cookFromRepository('archive')
            # Make sure we did build something
            self.assertEqual(ret[0][1], '/localhost@rpl:linux/1-1-1')

            # Request log should be empty
            self.assertEqual(
                [ x.strip() for x in file(TarGzRequestor.requestLog) ],
                [])

        finally:
            contentServer.kill()
            os.chdir(origDir)

    def testDuplicateBasenames(self):
        self.resetCache()
        self.resetWork()
        self.resetRepository()
        repos = self.openRepository()
        cfg = self.cfg
        repCache = lookaside.RepositoryCache(repos, cfg=cfg)

        try:
            contentServer = rephelp.HTTPServerController(getRequester())
            contentURL = contentServer.url()

            origDir = os.getcwd()
            os.chdir(self.workDir)
            self.newpkg('test')
            os.chdir('test')
            testdir = '/'.join((self.workDir, 'test'))

            log.setVerbosity(log.INFO)
            self.logFilter.add()

            # first, look for a file that does not exist
            assert lookaside.findAll(self.cfg, repCache,
                    contentURL + '/404/foo', 'test', (testdir,), allowNone=True
                ) is None
            # make sure that we got a negative cache entry
            assert os.stat(self.cacheDir + '/NEGATIVE/test/404/foo')

            # now look for a file that does exist
            assert lookaside.findAll(self.cfg, repCache,
                    contentURL + '/200/foo', 'test', (testdir,), allowNone=True
                ) is not None
            # make sure that we got a the cache entry
            assert os.stat(self.cacheDir + '/test/200/foo')

            # put two different files with the same name name in the cache 
            fooDir = os.path.join(self.cacheDir,'test/foo.conary.com/foo/')
            os.makedirs(fooDir)
            self.writeFile(os.path.join(fooDir,'theFile'),
                           'Foo version of the file\n')
            barDir = os.path.join(self.cacheDir,'test/bar.conary.com/bar/')
            os.makedirs(barDir)
            self.writeFile(os.path.join(barDir,'theFile'),
                           'Bar version of the file\n')

            # this file shouldn't be found
            path = lookaside.findAll(self.cfg, repCache,
                                     'http://baz.conary.com/foo/theFile', 'test', (testdir,),
                                     allowNone=True)
            self.assertEqual(path,None)
            # this file should be found and have the right contents
            path = lookaside.findAll(self.cfg, repCache,
                                     'http://foo.conary.com/foo/theFile', 'test', (testdir,),
                                     allowNone=True)
            f = open(path)
            self.assertEqual(f.readline()[0:3],'Foo')
            f.close()
            # so should this one
            path = lookaside.findAll(self.cfg, repCache,
                                     'http://bar.conary.com/bar/theFile', 'test', (testdir,),
                                     allowNone=True)
            f = open(path)
            self.assertEqual(f.readline()[0:3],'Bar')
            f.close()
        finally:
            contentServer.kill()

    def testHTTPProxy(self):
        '''Make sure that the lookaside cache can fetch through an http proxy'''
        if not os.path.exists(rephelp.HTTPProxy.proxyBinPath):
            raise testhelp.SkipTestException(
                'testHTTPProxy depends on squid being installed')

        class Always200Handler(SimpleHTTPRequestHandler):
            def log_message(self, *args, **kw):
                pass

            def do_GET(self):
                response = 'Hello, world!'
                self.send_response(200)
                self.send_header("Content-type", "text/unknown")
                self.send_header("Content-Length", len(response))
                self.end_headers()
                self.wfile.write(response)

        # create the file server
        server = rephelp.HTTPServerController(Always200Handler)

        proxy = rephelp.HTTPProxy(os.path.join(self.workDir, "http-cache"))
        proxyUri = proxy.start()

        repos = self.openRepository()

        try:
            self.cfg.proxy = { 'http' : 'http://localhost:%d/' % proxy.port }
            url1 = 'http://localhost:%d/noAuthProxy.txt' \
                % (server.port)
            path = lookaside.fetchURL(self.cfg, url1,'recipename')
            self.assertTrue(path)
            self.cfg.proxy = { 'http' : 'http://rpath:rpath@localhost:%d/'
                               % proxy.authPort }
            url2 = 'http://localhost:%d/authProxy.txt' \
                % (server.port)
            path = lookaside.fetchURL(self.cfg, url2,'recipename')
            self.assertTrue(path)

            proxy.stop()
            l = open(proxy.accessLog).read()
            self.assertTrue(url1 in l)
            self.assertTrue(url2 in l)

        finally:
            proxy.stop()
            server.stop()

    def testURLArguments(self):
        # standard url
        url1 = lookaside.laUrl('http://foo.example.com/bar.tar')
        self.assertEqual(url1.filePath(), '/foo.example.com/bar.tar')

        # url with arguments
        url2 = lookaside.laUrl('http://foo.example.com/bar.tar?arg=value')
        self.assertEqual(url2.filePath(),
                             '/foo.example.com/bar.tar?arg=value')

        # mirror url with arguments
        mirrorUrl = lookaside.laUrl('mirror://testmirror.com/baz.tar?arg=bif')
        url3 = lookaside.laUrl('http://foo.example.com/bar.tar?arg=value',
                               parent=mirrorUrl)
        self.assertEqual(url3.filePath(),
                             '/testmirror.com/baz.tar?arg=bif')

        # url with arguments and no filename
        url4 = lookaside.laUrl('http://foo.example.com/?arg=value')
        self.assertEqual(url4.filePath(), '/foo.example.com/?arg=value')

        # CNY-3674
        url5 = lookaside.laUrl('lookaside://lp:lightdm/lp:lightdm--466.tar.bz2')
        self.assertEqual(url5.host, 'lp:lightdm')


def getRequester():
    accessed = {}

    class SmallFileHttpRequestor(SimpleHTTPRequestHandler):
        # shut up!
        def log_message(self, *args, **kw):
            pass

        def do_GET(self):
            if self.path in accessed:
                accessed[self.path] += 1
            else:
                accessed[self.path] = 1
            response = '%s:%d\n' %(self.path, accessed[self.path])
            if '404' in self.path:
                self.send_response(404)
            else:
                self.send_response(200)
            self.send_header("Content-type", "text/unknown")
            self.send_header("Content-Length", len(response))
            self.end_headers()
            self.wfile.write(response)
    return SmallFileHttpRequestor

def cookieRequester():
    class CookieFileHttpRequestor(SimpleHTTPRequestHandler):
        # shut up!
        def log_message(self, *args, **kw):
            pass

        def do_GET(self):
            if 'Cookie' not in self.headers:
                self.send_response(302)
                baseUrl = 'http://%s:%s/' %(self.server.server_name,
                                            self.server.server_port)
                self.send_header('Set-Cookie', 'session=1;')
                self.send_header('Location', baseUrl + self.path)
                self.end_headers()
            else:
                self.send_response(200)
                response = 'Hello, world!\n'
                self.send_header('Content-type', 'text/unknown')
                self.send_header('Content-Length', len(response))
                self.end_headers()
                self.wfile.write(response)
    return CookieFileHttpRequestor

def authRequester():
    class AuthFileHttpRequestor(SimpleHTTPRequestHandler):
        def log_message(self, *args, **kw):
            pass

        def do_GET(self):
            if 'Authorization' not in self.headers:
                self.send_response(401)
                self.send_header("WWW-Authenticate",
                                 'Basic realm="Password Required"')
                self.end_headers()
            else:
                # user:pass
                if self.headers['Authorization'] == 'Basic dXNlcjpwYXNz':
                    self.send_response(200)
                    response = 'Hello, world!\n'
                    self.send_header('Content-type', 'text/unknown')
                    self.send_header('Content-Length', len(response))
                    self.end_headers()
                    self.wfile.write(response)
                elif self.headers['Authorization'] == 'Basic dXNlcjo=':
                    self.send_response(200)
                    response = 'Hello, world 2!\n'
                    self.send_header('Content-type', 'text/unknown')
                    self.send_header('Content-Length', len(response))
                    self.end_headers()
                    self.wfile.write(response)
                else:
                    self.send_response(401)
                    self.send_header("WWW-Authenticate",
                                     'Basic realm="Password Required"')
                    self.end_headers()
                    return

    return AuthFileHttpRequestor
