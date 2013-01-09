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


from testrunner import testhelp
import os, sys, time, errno
import select

from conary import callbacks
from conary.lib import util
from conary_test import recipes
from conary_test import rephelp
import fcntl

realPoll = select.poll
class DelayedStderr(object):
    def __init__(self, *args, **kwargs):
        self.poller = realPoll()
        self.registered = []
        self.pollCount = 0

    def register(self, fd, *args, **kwargs):
        self.registered.append(fd)
        return self.poller.register(fd, *args, **kwargs)

    def unregister(self, fd):
        if fd in self.registered:
            self.registered.remove(fd)
        return self.poller.unregister(fd)

    def poll(self):
        res = self.poller.poll()
        if len(self.registered) != 1:
            res = [x for x in res if x[0] == self.registered[0]]
        return res

class TagTest(rephelp.RepositoryHelper):

    def testMultiTag(self):
        self.resetRepository()
        self.resetRoot()
        script = self.rootDir + "/SCRIPT"
        multitag = self.build(recipes.multiTagRecipe, "MultiTag")
        self.updatePkg(self.rootDir, 'multitag', tagScript = script,
                       depCheck=False)

        self.verifyFile(script, """\
/usr/libexec/conary/tags/foo files update <<EOF
bar foo
/foo
EOF
""")
        self.resetRoot()

    def testMultiTagWhenOnlyOneTagUsed(self):
        self.resetRepository()
        self.resetRoot()
        script = self.rootDir + "/SCRIPT"
        multitag = self.build(recipes.multiTagRecipe2, "MultiTag")
        self.updatePkg(self.rootDir, 'multitag', tagScript = script,
                       depCheck=False)

        self.verifyFile(script, """\
/usr/libexec/conary/tags/foo files update <<EOF
foo
/foo
EOF
""")
        self.resetRoot()

    def testNewTagUpdates(self):
        """
        This tests behavior that is currently functional, but appears
        to possibly have been broken in the past.  An old version of
        dovecot had a file that was a config file but not tagged as
        an initscript, and the contents were missing when we went to
        update it with a version that also had the config file tagged
        as an initscript.
        """
        self.resetRepository()
        self.resetWork()
        self.repos = self.openRepository()

        self.addTestPkg(1, version='1.0',
            packageSpecs=[
                "r.Create('/etc/test1', contents='test1')",
                "r.Config(exceptions='/etc/test1')",
                ])
        self.cookTestPkg(1)
        self.addTestPkg(1, version='1.1',
            packageSpecs=[
                "r.Create('/etc/test1', contents='test1')",
                ])
        self.cookTestPkg(1)
        self.addTestPkg(1, version='1.2',
            packageSpecs=[
                "r.Create('/etc/test1', contents='test1')",
                "r.TagSpec('initscript', '/etc/test1')"
                ])
        self.cookTestPkg(1)
        self.addTestPkg(1, version='1.3',
            packageSpecs=[
                "r.Create('/etc/test1', contents='test2')",
                "r.TagSpec('initscript', '/etc/test1')"
                ])
        self.cookTestPkg(1)

        # initial version, no config file
        self.updatePkg(self.rootDir, 'test1', '1.0-1-1', depCheck=False)
        # now we have a config file
        self.updatePkg(self.rootDir, 'test1', '1.1-1-1', depCheck=False)
        # config file gets tag added
        self.updatePkg(self.rootDir, 'test1', '1.2-1-1', depCheck=False)
        # config file gets content change
        self.updatePkg(self.rootDir, 'test1', '1.3-1-1', depCheck=False)

    @testhelp.context("rollback")
    def testTags(self):
        script = self.rootDir + "/SCRIPT"

        tagProvider1 = self.build(recipes.tagProviderRecipe1, "TagProvider")
        firstUser1 = self.build(recipes.firstTagUserRecipe1, "FirstTagUser")
        secondUser1 = self.build(recipes.secondTagUserRecipe1, "SecondTagUser")

        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'tagprovider', tagScript = script)
        self.updatePkg(self.rootDir, 'secondtaguser', tagScript = script)

        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag files update /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
/usr/libexec/conary/tags/testtag files update /etc/testsecond.1
""")

        self.resetRoot()
        self.updatePkg(self.rootDir, 'tagprovider', tagScript = script)
        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'secondtaguser', tagScript = script)
        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag files update /etc/testself.1
/usr/libexec/conary/tags/testtag files update /etc/testfirst.1 /etc/testfirst.2
/usr/libexec/conary/tags/testtag files update /etc/testsecond.1
""")
        os.unlink(script)
        self.erasePkg(self.rootDir, "secondtaguser", 
                      secondUser1.getVersion().asString(), tagScript = script)
        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag files remove /etc/testsecond.1
""")
        os.unlink(script)
        self.erasePkg(self.rootDir, "tagprovider", 
                      tagProvider1.getVersion().asString(), tagScript = script)
#       XXX ewt needs to fix this
#       also should probably test preremove
#        self.verifyFile(script, """\
#/usr/libexec/conary/tags/testtag files remove /etc/testself.1
#""")

        tagProvider2 = self.build(recipes.tagProviderRecipe2, "TagProvider")

        self.resetRoot()
        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'tagprovider', tagScript = script)
        self.updatePkg(self.rootDir, 'secondtaguser', tagScript = script)
        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag handler update /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
# /usr/libexec/conary/tags/testtag files preupdate /etc/testsecond.1
/usr/libexec/conary/tags/testtag files update /etc/testsecond.1
""")
        os.unlink(script)
        self.erasePkg(self.rootDir, "secondtaguser", 
                      secondUser1.getVersion().asString(), tagScript = script)
        self.verifyFile(script, """\
# /usr/libexec/conary/tags/testtag files preremove /etc/testsecond.1
/usr/libexec/conary/tags/testtag files remove /etc/testsecond.1
""")

        os.unlink(script)
        self.erasePkg(self.rootDir, "tagprovider", 
                      tagProvider2.getVersion().asString(), tagScript = script)
        self.verifyFile(script, """\
# /usr/libexec/conary/tags/testtag handler preremove /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
""")

        os.unlink(script)
        # Each rollback generates two file update events; the first for 
        # replacing the file and the second is for changing the owner/group
        # for that file to the id of whoever is running the test case. This
        # check will fail when the test suite is run by root, since there
        # aren't any local changes in that case.
        self.rollback(self.rootDir, 4, tagScript = script)
        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag handler update /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
# /usr/libexec/conary/tags/testtag files preupdate /etc/testself.1
/usr/libexec/conary/tags/testtag handler update /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
""")

        os.unlink(script)
        self.rollback(self.rootDir, 3, tagScript = script)
        self.verifyFile(script, """\
# /usr/libexec/conary/tags/testtag files preupdate /etc/testsecond.1
/usr/libexec/conary/tags/testtag files update /etc/testsecond.1
# /usr/libexec/conary/tags/testtag files preupdate /etc/testsecond.1
/usr/libexec/conary/tags/testtag files update /etc/testsecond.1
""")

        # make sure "files update" doesn't get run when "handler update"
        # does
        self.resetRoot()
        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'tagprovider', tagScript = script)
        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag handler update /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
""")

        self.resetRoot()
        tagProvider3 = self.build(recipes.tagProviderRecipe3, "TagProvider")
        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'tagprovider', tagScript = script)
        self.updatePkg(self.rootDir, 'secondtaguser', tagScript = script)
        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag files update <<EOF
/etc/testfirst.1
/etc/testfirst.2
EOF
/usr/libexec/conary/tags/testtag files update <<EOF
/etc/testsecond.1
EOF
""")

        # make sure "handler update" gets called when just the handler
        # changes
        script = self.rootDir + "/SCRIPT"

        self.resetRoot()
        self.resetRepository()
        tagProvider2 = self.build(recipes.tagProviderRecipe2, "TagProvider")
        tagProvider4 = self.build(recipes.tagProviderRecipe4, "TagProvider")
        firstUser1 = self.build(recipes.firstTagUserRecipe1, "FirstTagUser")

        self.updatePkg(self.rootDir, 'tagprovider', version = '1-1-1',
                       tagScript = script)
        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'tagprovider', tagScript = script)

        # make sure "handler update" gets called when just the handler
        # changes, even if the original description doesn't say that
        # 'handler update' is supported
        self.resetRoot()
        tagProvider5 = self.build(recipes.tagProviderRecipe5, "TagProvider")
        self.updatePkg(self.rootDir, 'tagprovider', version = '1-1-3',
                       tagScript = script)
        self.updatePkg(self.rootDir, 'firsttaguser', tagScript = script)
        self.updatePkg(self.rootDir, 'tagprovider', version = '1-1-2',
                       tagScript = script)

        self.verifyFile(script, """\
/usr/libexec/conary/tags/testtag handler update /etc/testfirst.1 /etc/testfirst.2 /etc/testself.1
""")

        self.resetRepository()
        self.resetRoot()

        tagProvider2 = self.build(recipes.tagProviderRecipe2, "TagProvider")
        firstUser1 = self.build(recipes.firstTagUserRecipe1, "FirstTagUser")
        secondUser1 = self.build(recipes.secondTagUserRecipe1, "SecondTagUser")

        self.updatePkg(self.rootDir, 'firsttaguser')
        self.updatePkg(self.rootDir, 'tagprovider')
        self.updatePkg(self.rootDir, 'secondtaguser')

    def testTagExecution(self):
        class Callback(callbacks.UpdateCallback):
            pass

            def tagHandlerOutput(self, tag, msg, stderr = False):
                self.msgs.append((tag, msg, stderr))

            def __init__(self, *args, **kwargs):
                callbacks.UpdateCallback.__init__(self, *args, **kwargs)
                self.msgs = []

        # We don't have a proper chroot, so this is tricky. We have to
        # stub out chroot, stub the execv used to run the tag handler,
        # and provide a tag handler to check. We want to make sure the
        # file arguments get through properly and that any output from the
        # script gets tagged properly. We need to do this for all three
        # execution types -- command line args, stdin, and multitag.

        oldFuncs = (os.getuid, os.lchown, os.chroot)

        try:
            os.getuid = lambda : 0
            os.lchown = lambda x, y, z : None
            os.chroot = lambda x : None

            stdinTagFile = rephelp.RegularFile(
                                    contents = stdinTagConfig % self.rootDir,
                                    perms = 0644, tags = [ 'tagdescription' ] )
            argsTagFile = rephelp.RegularFile(
                                    contents = argsTagConfig % self.rootDir,
                                    perms = 0644, tags = [ 'tagdescription' ] )
            multiTagFile = rephelp.RegularFile(
                                    contents = multiTagConfig % self.rootDir,
                                    perms = 0644, tags = [ 'tagdescription' ] )
            stdinHandler = rephelp.RegularFile(
                                    contents = stdinScript, perms = 0775,
                                    tags = [ 'taghandler' ] )
            chattyHandler = rephelp.RegularFile(
                                    contents = chattyScript, perms = 0775,
                                    tags = [ 'taghandler' ] )
            taggedFile = rephelp.RegularFile(tags = [ 'testtag' ])
            multiTaggedFile = rephelp.RegularFile(
                tags = [ 'testtag1', 'testtag2' ])

            util.mkdirChain(self.rootDir)

            self.addComponent('stdin:runtime', '1.0-1-1',
                fileContents = [
                    ('/etc/conary/tags/testtag', stdinTagFile),
                    ('/bin/taghandler', stdinHandler),
                    ('/etc/test-tagged', taggedFile)
                ] )

            self.addComponent('args:runtime', '1.0-1-1',
                fileContents = [
                    ('/etc/conary/tags/testtag', argsTagFile),
                    ('/bin/taghandler', stdinHandler),
                    ('/etc/test-tagged', taggedFile)
                ] )

            self.addComponent('multi:runtime', '1.0-1-1',
                fileContents = [
                    ('/etc/conary/tags/testtag', multiTagFile),
                    ('/bin/taghandler', stdinHandler),
                    ('/etc/test-tagged', taggedFile)
                ] )

            self.addComponent('chatty:runtime', '1.0-1-1',
                fileContents = [
                    ('/etc/conary/tags/testtag', argsTagFile),
                    ('/bin/taghandler', chattyHandler),
                    ('/etc/test-tagged', taggedFile)
                ] )

            self.addComponent('multi2:runtime', '1.0-1-1',
                fileContents = [
                    ('/etc/conary/tags/testtag1', multiTagFile),
                    ('/etc/conary/tags/testtag2', multiTagFile),
                    ('/bin/taghandler', stdinHandler),
                    ('/etc/test-multi-tagged', multiTaggedFile)
                ] )

            cb = Callback()
            self.updatePkg('stdin:runtime', callback = cb, test=True)
            assert(not os.path.exists(self.rootDir + "tag-output"))

            self.updatePkg('stdin:runtime', callback = cb)
            self.verifyFile(self.rootDir + "/tag-output",
                            'ARGS: files update\n/etc/test-tagged\n')
            self.assertEqual(cb.msgs, [ ('testtag', '/etc/test-tagged\n', False) ])

            self.resetRoot()
            cb = Callback()
            self.updatePkg('args:runtime', callback = cb, test=True)
            assert(not os.path.exists(self.rootDir + "tag-output"))
            self.updatePkg('args:runtime', callback = cb)
            self.verifyFile(self.rootDir + "/tag-output",
                            'ARGS: files update /etc/test-tagged\n')
            self.assertEqual(cb.msgs, [])

            self.resetRoot()
            cb = Callback()
            self.updatePkg('multi:runtime', callback = cb, test=True)
            assert(not os.path.exists(self.rootDir + "tag-output"))
            self.updatePkg('multi:runtime', callback = cb)
            self.assertEqual(cb.msgs, [ ('testtag', 'testtag\n', False),
                                ('testtag', '/etc/test-tagged\n', False) ])
            self.verifyFile(self.rootDir + "/tag-output",
                            'ARGS: files update\ntesttag\n/etc/test-tagged\n')

            self.callCount = 0
            realPoll = select.poll
            def fakePoll():
                # only touch the update code. too bad it's named "run"
                action = sys._getframe(1).f_code.co_name
                if action == 'run':
                    return DelayedStderr()
                else:
                    return realPoll()
            self.resetRoot()
            cb = Callback()
            self.mock(select, 'poll', fakePoll)
            self.updatePkg('chatty:runtime', callback = cb)
            self.assertEqual([ x[0:2] for x in cb.msgs if x[2] == False ],
                      [ ('testtag', 'first\n'), ('testtag', 'second\n'),
                        ('testtag', 'partial\n') ] )
            self.assertEqual([ x[0:2] for x in cb.msgs if x[2] == True ],
                      [ ('testtag', 'error\n') ] )

            # make sure the default callbacks display messages. losing
            # tag handler output would suck.
            self.resetRoot()
            (rc, res) = self.captureOutput(self.updatePkg, 'chatty:runtime')

            # normally, stderr could be moved with respect to the rest
            # of the messages, but we forced the worst case by mocking poll
            self.assertEquals(res, '\n'.join(('[testtag] first',
                '[testtag] second', '[testtag] partial',
                '[testtag] error', '')))

            # Same tag handler assigned to 2 tags.
            self.resetRoot()
            cb = Callback()
            self.updatePkg('multi2:runtime', callback = cb)
            self.assertEqual(cb.msgs,
                [('testtag1 testtag2', 'testtag1 testtag2\n', False),
                 ('testtag1 testtag2', '/etc/test-multi-tagged\n', False)]
            )



        finally:
            (os.getuid, os.lchown, os.chroot) = oldFuncs

    def testTagHandlerDoesNotExist(self):
        myRecipe = recipes.multiTagRecipe0
        myRecipe += '        r.ComponentSpec(":tag", "%(taghandlerdir)s/")\n'
        multitag = self.build(myRecipe, "MultiTag", returnTrove='multitag')
        self.updatePkg('multitag:runtime')
        fooFile = rephelp.RegularFile(
                                contents = 'foo\n',
                                perms = 0644, tags = [ 'foo' ] )
        self.addComponent('foo:runtime', [('/bam', fooFile)])
        oldFuncs = (os.getuid, os.lchown, os.chroot)

        self.mock(os, "getuid", lambda : 0)
        self.mock(os, "lchown", lambda x, y, z : None)
        self.mock(os, "chroot", lambda x :None)
        # this fixes a race between new tag handler process exiting and
        # writing files into the pipe for that tag handler; we let the
        # write finish before the handler process terminates
        origExec = os.execve
        self.mock(os, "execve", lambda *args : (time.sleep(0.1),
                                                origExec(*args)))

        rc, txt = self.captureOutput(self.updatePkg, 'foo:runtime',
                                     _removeBokenPipeErrors=True)

        self.assertEquals(txt.lstrip(), '[foo] [Errno 2] No such file or directory\nerror: /usr/libexec/conary/tags/foo failed\n')

    def testTagHandlerEnvironment(self):
        # prevent the callback from catching (and discarding) the exception
        class EnvironmentError(Exception):
            errorIsUncatchable = True

        class Callback(callbacks.UpdateCallback):
            def tagHandlerOutput(self, tag, msg, stderr = False):
                if msg.strip() != 'UNDEFINED':
                    raise EnvironmentError()

        cb = Callback()

        self.resetRepository()
        self.resetRoot()
        multitag = self.build(recipes.multiTagRecipe3, "MultiTag")

        oldFuncs = (os.getuid, os.lchown, os.chroot, os.execve)

        # When mocking execve, we have to mock the closing of file descriptors
        # too
        def mockMassCloseFileDescriptors(start, count):
            pass
        self.mock(util, 'massCloseFileDescriptors',
                  mockMassCloseFileDescriptors)

        try:
            # Replace some of the standard functions to force the codepath the
            # way we want it to go
            # We cannot chroot as non-root, so fake chroot and execve
            os.getuid = lambda : 0
            os.lchown = lambda x, y, z : None
            os.chroot = lambda x : None
            os.execve = lambda x, y, z: oldFuncs[3](self.rootDir + x,
                        [self.rootDir + x] + y[1:], z)

            os.environ['SOMEVAR'] = 'SOMEVALUE'
            try:
                self.updatePkg(self.rootDir, 'multitag', callback=cb,
                               depCheck=False)
            except EnvironmentError:
                self.fail(
                    "Tag handler wrongly inherits env vars from parent process")
        finally:
            (os.getuid, os.lchown, os.chroot, os.execve) = oldFuncs
            del os.environ['SOMEVAR']

        self.resetRoot()

    def testFileDescriptors(self):

        def execStub(*args):
            # don't really exec, just succeed
            openFds = os.listdir('/proc/%d/fd' % os.getpid())
            while sys.stdin.read():
                pass
            rc = 0
            for fd in openFds:
                fd = int(fd)
                if fd in (0, 1, 2):
                    continue
                try:
                    cloexec = fcntl.fcntl(fd, fcntl.F_GETFD)
                except IOError, e:
                    if e.errno == errno.EBADF:
                        continue
                if cloexec:
                    continue
                rc = 1
                fn = os.readlink("/proc/%d/fd/%d" % (os.getpid(), fd))
                print "fd %d (%s) is not close-on-exec" % (fd, fn)

            os._exit(rc)

        stdinTagFile = rephelp.RegularFile(
                                contents = stdinTagConfig % self.rootDir,
                                perms = 0644, tags = [ 'tagdescription' ] )

        stdinHandler = rephelp.RegularFile(
                                contents = stdinScript, perms = 0775,
                                tags = [ 'taghandler' ] )

        taggedFile = rephelp.RegularFile(tags = [ 'testtag' ])

        self.addComponent('stdin:runtime', '1.0-1-1',
            fileContents = [
                ('/etc/conary/tags/testtag', stdinTagFile),
                ('/bin/taghandler', stdinHandler),
                ('/etc/test-tagged', taggedFile)
            ] )

        # how nice. we stub out the exec so we can ensure all file descriptors
        # are set as close-on-exec and return success/failure based on
        # the result
        self.mock(os, "execve", execStub)
        self.mock(os, "getuid", lambda *args: 0)
        self.mock(os, "chroot", lambda *args: 0)
        self.mock(os, "lchown", lambda *args: 0)

        (rc, str) = self.captureOutput(self.updatePkg, 'stdin:runtime')
        if str:
            print str
            assert(0)

    def testTagScriptOrdering(self):
        manyTag = self.build(manyTagRecipe,
                             "FooRecipe")
        script = self.rootDir + "/SCRIPT"
        self.updatePkg('foo', tagScript=script, test=True)
        assert(not os.path.exists(script))
        self.updatePkg('foo', tagScript=script)
        lines = open(script).read().split('\n')
        lines = [ x.split()[0] for x in lines if x ]
        thname = '/usr/libexec/conary/tags/taghandler'
        lines = [ int(x[len(thname):]) for x in lines ]
        assert(lines == [ 0,1,10,2,3,4,5,6,7,8,9])

stdinScript = """\
#!/bin/bash
echo ARGS: $* > tag-output
tee -a tag-output
"""

chattyScript = """\
#!/bin/bash
echo "first"
echo "error" 1>&2
echo "second"
echo -n "partial"
cat > /dev/null
"""

stdinTagConfig = """\
file            %s/bin/taghandler
implements      files update
datasource      stdin
include         /etc/test.*
"""

argsTagConfig = """\
file            %s/bin/taghandler
implements      files update
datasource      args
include         /etc/test.*
"""

multiTagConfig = """\
file            %s/bin/taghandler
implements      files update
datasource      multitag
include         /etc/test.*
"""

manyTagRecipe = """
class FooRecipe(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        for i in range(11):
            r.Create('%%(tagdescriptiondir)s/tag%s' % i, 
            contents = '''
file            %%(taghandlerdir)s/taghandler%s
implements      files update
datasource      args
include         /etc/test%s
''' % (i, i))
            import time
            r.Create('/etc/test%s' % i, contents=str(time.time()) + '\\n')
            r.Create('%%(taghandlerdir)s/tag%s' % i, mode=0755)
            r.TagSpec('tag%s' % i, '/etc/test%s' % i)
"""
