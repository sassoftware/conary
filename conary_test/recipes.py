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


testRecipe1 = """\
class TestRecipe1(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()
    owner = 'root'
    group = 'root'
    withBinary = True
    withUse = False

    changedconfig = '%(sysconfdir)s/changedconfig'
    unchangedconfig = '%(sysconfdir)s/unchangedconfig'
    changed = '%(datadir)s/changed'
    unchanged = '%(datadir)s/unchanged'

    initialFileText = '\\n'.join([str(x) for x in range(0,10)]) + '\\n'
    fileText = initialFileText

    def modifyFiles(self):
        pass
    
    def setup(self):
        if self.withUse:
            if Use.readline:
                pass

        if self.withBinary:
            self.Run('''
cat > hello.c <<'EOF'
#include <stdio.h>

int main(void) {
    return printf("Hello, world.\\\\n");
}
EOF
            ''')
            self.Make('hello', preMake='LDFLAGS="-static"')
            self.Install('hello', '%(bindir)s/')
        self.Create(self.changedconfig, self.unchangedconfig,
             self.changed, self.unchanged, contents=self.initialFileText)
        self.modifyFiles()
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(datadir)s/', '%(sysconfdir)s/')
        self.Strip(debuginfo=False)
"""
        
testRecipe2="""\
class TestRecipe2(TestRecipe1):
    version = '1.1'

    fileText = TestRecipe1.fileText.replace("5", "1")

    def modifyFile(self, path):
        return 'sed -i s/^5/1/g %(destdir)s'+path

    def modifyFiles(self):
        for path in (self.changedconfig, self.changed):
            self.Run(self.modifyFile(path))

    def setup(self):
        TestRecipe1.setup(self)
"""

testRecipe3="""\
class TestRecipe3(TestRecipe1):
    version = '1.2'

    fileText = TestRecipe1.fileText.replace("6", "2")

    def modifyFile(self, path):
        return 'sed -i s/^6/2/g %(destdir)s'+path

    def modifyFiles(self):
        for path in (self.changedconfig,):
            self.Run(self.modifyFile(path))

    def setup(self):
        TestRecipe1.setup(self)
"""

testRecipe4="""\
class TestRecipe4(TestRecipe1):
    version = '1.3'

    def setup(self):
        TestRecipe1.setup(self)
        self.Config(exceptions = "/etc/.*")
"""

# like TestRecipe1, but only includes /usr/bin/hello
testRecipe5="""\
class TestRecipe5(TestRecipe1):
    version = '1.4'

    def setup(r):
        TestRecipe1.setup(r)
        r.Remove(r.changed)
        r.Remove(r.unchanged)
        r.Remove(r.changedconfig)
        r.Remove(r.unchangedconfig)
"""

testTransientRecipe1=r"""\
class TransientRecipe1(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()
    fileText = 'bar\n'
    def setup(r):
        r.Create('/foo', contents=r.fileText)
        r.Transient('/foo')
"""
testTransientRecipe2=r"""\
class TransientRecipe2(PackageRecipe):
    name = 'testcase'
    version = '1.1'
    clearBuildReqs()
    fileText = 'blah\n'
    def setup(r):
        r.Create('/foo', contents=r.fileText)
        r.Transient('/foo')
"""

testTransientRecipe3=r"""\
class TransientRecipe3(PackageRecipe):
    name = 'testcase'
    version = '1.2'
    clearBuildReqs()
    fileText = 'blah\n'
    def setup(r):
        #don't create foo
        r.Create('/foo2', contents=r.fileText)
        r.Transient('/foo2')
"""

testTransientRecipe4=r"""\
class TransientRecipe4(PackageRecipe):
    name = 'testcase'
    version = '1.3'
    clearBuildReqs()
    fileText = 'blahblech\n'
    def setup(r):
        #don't create foo
        r.Create('/foo3', contents=r.fileText)
        r.Transient('/foo3')
"""

libhelloRecipePreface="""\
class Libhello(PackageRecipe):
    name = 'libhello'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        # NormalizeInterpreterPaths not the purpose of these tests,
        # and dealing with it running would make tests needlessly
        # and uselessly more verbose.
        del self.NormalizeInterpreterPaths
        self.Create('libhello.c', contents='''
/* libhello.c - Simple example of a shared library */

void return_one(void) {
    return 1;
}
        ''')
        self.Create('true.c', contents='''
int main() {
    return 0;
}
        ''')
        self.Create('user.c', contents='''
int main() {
    return return_one();
}
        ''')
"""
libhelloRecipe = libhelloRecipePreface + r"""
        self.Run('%(cc)s %(ldflags)s -fPIC -shared -Wl,-soname,libhello.so.0 -o libhello.so.0.0 libhello.c -nostdlib')
        self.Run('%(cc)s %(ldflags)s -static -o true true.c')
        self.Run('%(cc)s %(ldflags)s -nostdlib -o user user.c libhello.so.0.0')
        self.Install('libhello.so.0.0', '%(libdir)s/libhello.so.0.0')
        self.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        self.Install('user', '%(essentialsbindir)s/user', mode=0755)
        self.Create('/etc/ld.so.conf', contents='/%(lib)s')
        self.Create('%(essentialbindir)s/script', 
                    contents='#!%(essentialsbindir)s/user', mode = 0755)
        self.Provides('file',  '%(essentialsbindir)s/user')
        self.ComponentSpec('runtime', '%(essentialsbindir)s/ldconfig',
                           '%(libdir)s/libhello.so.0.*',
                           '%(sysconfdir)s/')
        self.ComponentSpec('user', '%(essentialsbindir)s/user')
        self.ComponentSpec('script', '%(essentialbindir)s/script')
        self.Strip(debuginfo=False)
"""

libhelloRecipeLdConfD = libhelloRecipePreface + r"""
        self.Run('%(cc)s %(ldflags)s -fPIC -shared -Wl,-soname,libhello.so.0 -o libhello.so.0.0 libhello.c -nostdlib')
        self.Run('%(cc)s %(ldflags)s -static -o true true.c')
        self.Run('%(cc)s %(ldflags)s -nostdlib -o user user.c libhello.so.0.0')
        self.Install('libhello.so.0.0', '%(libdir)s/libhello.so.0.0')
        self.Install('libhello.so.0.0', '%(essentiallibdir)s/libhello.so.0.0')
        self.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        self.Install('user', '%(essentialsbindir)s/user', mode=0755)
        self.Create('/etc/ld.so.conf', contents='/opt/foo')
        self.Create('/etc/ld.so.conf.d/first.conf', contents='%(essentiallibdir)s')
        self.Create('%(essentialbindir)s/script', 
                    contents='#!%(essentialsbindir)s/user', mode = 0755)
        self.Provides('file',  '%(essentialsbindir)s/user')
        self.ComponentSpec('runtime', '%(essentialsbindir)s/ldconfig',
                           '%(libdir)s/libhello.so.0.*',
                           '%(essentiallibdir)s/libhello.so.0.*',
                           '/etc/ld.so.conf.d/first.conf',
                           '%(sysconfdir)s/')
        self.ComponentSpec('user', '%(essentialsbindir)s/user')
        self.ComponentSpec('script', '%(essentialbindir)s/script')
        self.Strip(debuginfo=False)
"""

libhelloRecipeNoVersion = libhelloRecipePreface + """\
        self.Run('%(cc)s %(ldflags)s -fPIC -shared -Wl,-soname,libhello.so -o libhello.so libhello.c -nostdlib')
        self.Run('%(cc)s %(ldflags)s -static -o true true.c')
        self.Run('%(cc)s %(ldflags)s -nostdlib -o user user.c libhello.so')
        self.Install('libhello.so', '%(libdir)s/libhello.so', mode=0644)
        self.Install('true', '%(essentialsbindir)s/ldconfig', mode=0755)
        self.Install('user', '%(essentialsbindir)s/user', mode=0755)
        self.Create('/etc/ld.so.conf', contents='/lib')
        self.Create('%(essentialbindir)s/script', 
                    contents='#!%(essentialsbindir)s/user', mode = 0755)
        self.Provides('file',  '%(essentialsbindir)s/user')
        self.ComponentSpec('runtime', '%(essentialsbindir)s/ldconfig',
                           '%(libdir)s/libhello.so',
                           '%(sysconfdir)s/')
        self.ComponentSpec('user', '%(essentialsbindir)s/user')
        self.ComponentSpec('script', '%(essentialbindir)s/script')
        self.Strip(debuginfo=False)
"""

bashRecipe="""\
class Bash(PackageRecipe):
    name = 'bash'
    version = '0'
    clearBuildReqs()
    def setup(r):
        del r.NormalizeInterpreterPaths
        r.Create('%(essentialbindir)s/bash', mode=0755)
        r.Create('%(essentialbindir)s/conflict', mode=0755)
        r.Provides('file', '%(essentialbindir)s/(ba)?sh')
        if Use.ssl:
            # turn on this use flag; we use this in the tests for flavor
            # dependent resolution
            pass
"""

bashMissingRecipe="""\
class Bash(PackageRecipe):
    name = 'bash'
    version = '1'
    clearBuildReqs()
    def setup(r):
        del r.NormalizeInterpreterPaths
        r.Create('%(essentialbindir)s/conflict', mode=0755)
        if Use.ssl:
            # turn on this use flag; we use this in the tests for flavor
            # dependent resolution
            pass
"""

bashUserRecipe="""\
class BashUser(PackageRecipe):
    name = 'bashuser'
    version = '0'
    clearBuildReqs()
    def setup(r):
        del r.NormalizeInterpreterPaths
        r.Create('%(essentialbindir)s/script', mode=0755,
                 contents = '#!/bin/bash')
"""

bashTroveUserRecipe="""\
class BashTroveUser(PackageRecipe):
    name = 'bashtroveuser'
    version = '0'
    clearBuildReqs()
    def setup(r):
        del r.NormalizeInterpreterPaths
        r.Create('%(essentiallibdir)s/empty', mode=0644)
        r.Requires('bash:runtime', '%(essentiallibdir)s/empty')

"""

gconfRecipe="""\
class Gconf(PackageRecipe):
    name = 'gconf'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('%(sysconfdir)s/gconf/schemas/foo')
        r.Install('/bin/true', '%(bindir)s/gconftool-2', mode=0755)
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

chkconfigRecipe="""\
class ChkconfigTest(PackageRecipe):
    name = 'testchk'
    version = '0'
    clearBuildReqs()
    
    def setup(self):
        self.Run('''
cat > chkconfig.c <<'EOF'
int main(int argc, char ** argv) {
    int fd;
    char ** chptr;

    fd = open(\"OUT\", 0102, 0666);
    for (chptr = argv; *chptr; chptr++) {
        write(fd, *chptr, strlen(*chptr));
        if (*(chptr + 1)) write(fd, \" \", 1);
    }

    write(fd, \"\\\\n\", 1);
    close(fd);
}
EOF
''')
        self.Run('''
cat > testchk <<'EOF'
# chkconfig: 345 95 5
# description: Runs commands scheduled by the at command at the time \
#    specified when at was run, and runs batch commands when the load \
#    average is low enough.
# processname: atd
EOF
        
''')
        self.Run('%(cc)s %(ldflags)s -static -o chkconfig chkconfig.c')
        self.Install("chkconfig", "%(essentialsbindir)s/", mode = 0755)
        self.Install("testchk", "%(initdir)s/", mode = 0755)
        self.Strip(debuginfo=False)
"""

doubleRecipe1 = """
class Double(PackageRecipe):
    name = 'double'
    version = '1.0'
    clearBuildReqs()
    owner = 'root'
    group = 'root'

    def setup(self):
        self.Create("/etc/foo1", contents = "text1")
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

doubleRecipe1_1 = """
class Double(PackageRecipe):
    name = 'double'
    version = '1.1'
    clearBuildReqs()
    owner = 'root'
    group = 'root'

    def setup(self):
        self.Create("/etc/foo1.1", contents = "text1.1")
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

doubleRecipe1_2 = """
class Double(PackageRecipe):
    name = 'double'
    version = '1.2'
    clearBuildReqs()
    owner = 'root'
    group = 'root'

    def setup(self):
        self.Create("/etc/foo1.2", contents = "text1.2")
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

doubleRecipe1_3 = """
class Double(PackageRecipe):
    name = 'double'
    version = '1.3'
    clearBuildReqs()
    owner = 'root'
    group = 'root'

    def setup(self):
        self.Create("/etc/foo1.3", contents = "text1.3")
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

doubleRecipe2 = """
class Double(PackageRecipe):
    name = 'double'
    version = '2.0'
    clearBuildReqs()
    owner = 'root'
    group = 'root'

    def setup(self):
        self.Create("/etc/foo2", contents = "text2")
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

doubleRecipe2_1 = """
class Double(PackageRecipe):
    name = 'double'
    version = '2.1'
    clearBuildReqs()
    owner = 'root'
    group = 'root'

    def setup(self):
        self.Create("/etc/foo2.1", contents = "text2.1")
        self.Ownership(self.owner, self.group, '.*')
        self.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

simpleTagHandler = """r.Run('''
cat > testtag.taghandler.c <<'EOF'
int main(int argc, char ** argv) {
    int fd;
    char ** chptr;

    fd = open(\"OUT%s\", 0102, 0666);
    for (chptr = argv; *chptr; chptr++) {
        write(fd, *chptr, strlen(*chptr));
        if (*(chptr + 1)) write(fd, \" \", 1);
    }

    write(fd, \"\\\\n\", 1);
    close(fd);
}
EOF
''')
        r.Run('%%(cc)s %%(ldflags)s -static -o testtag.taghandler testtag.taghandler.c')
        r.Strip(debuginfo=False)"""

tagProviderRecipe1 = """
class TagProvider(PackageRecipe):
    name = 'tagprovider'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testtag.tagdescription <<EOF
file                /usr/libexec/conary/tags/testtag
implements      files update
implements      files remove
include                /etc/test.*
EOF
''')

        %(simpleTagHandler)s

        r.Install('testtag.tagdescription',
                  '%%(tagdescriptiondir)s/testtag')
        r.Install('testtag.taghandler',
                  '%%(taghandlerdir)s/testtag')
        # Also test tagging our own files
        r.Create('/etc/testself.1')
        r.ComponentSpec('runtime', '%%(sysconfdir)s/')
""" % { 'simpleTagHandler' : (simpleTagHandler % "") }

tagProviderRecipe2 = """
class TagProvider(PackageRecipe):
    name = 'tagprovider'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testtag.tagdescription <<EOF
file                /usr/libexec/conary/tags/testtag
implements      files update
implements      files preremove
implements      files remove
implements      files preupdate
implements      handler update
implements      handler preremove
datasource        args
include                /etc/test.*
EOF
''')

        %(simpleTagHandler)s

        r.Install('testtag.tagdescription',
                  '%%(tagdescriptiondir)s/testtag')
        r.Install('testtag.taghandler',
                  '%%(taghandlerdir)s/testtag')
        # Also test tagging our own files
        r.Create('/etc/testself.1')
        r.ComponentSpec('runtime', '%%(sysconfdir)s/')
""" % { 'simpleTagHandler' : (simpleTagHandler % "") }

tagProviderRecipe3 = """
class TagProvider(PackageRecipe):
    name = 'tagprovider'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testtag.tagdescription <<EOF
file                /usr/libexec/conary/tags/testtag
implements      files update
datasource        stdin
include                /etc/test.*
EOF
''')

        %(simpleTagHandler)s

        r.Install('testtag.tagdescription',
                  '%%(tagdescriptiondir)s/testtag')
        r.Install('testtag.taghandler',
                  '%%(taghandlerdir)s/testtag')
""" % { 'simpleTagHandler' : (simpleTagHandler % "") }

# this is just like tagProviderRecipe2, but the tagdescription will create
# /tmp/OUT2 instead of /tmp/OUT
tagProviderRecipe4 = """
class TagProvider(PackageRecipe):
    name = 'tagprovider'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testtag.tagdescription <<EOF
file                /usr/libexec/conary/tags/testtag
implements      files update
implements      files preremove
implements      files remove
implements      handler update
implements      handler preremove
datasource        args
include                /etc/test.*
EOF
''')

        %(simpleTagHandler)s

        r.Install('testtag.tagdescription',
                  '%%(tagdescriptiondir)s/testtag')
        r.Install('testtag.taghandler',
                  '%%(taghandlerdir)s/testtag')
        # Also test tagging our own files
        r.Create('/etc/testself.1')
        r.ComponentSpec('runtime', '%%(sysconfdir)s/')
""" % { 'simpleTagHandler' : (simpleTagHandler % "2") }

# this is just like tagProviderRecipe2, but it has a more limited implements
# set
tagProviderRecipe5 = """
class TagProvider(PackageRecipe):
    name = 'tagprovider'
    version = '1'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testtag.tagdescription <<EOF
file                /usr/libexec/conary/tags/testtag
implements      files remove
datasource        args
include                /etc/test.*
EOF
''')

        %(simpleTagHandler)s

        r.Install('testtag.tagdescription',
                  '%%(tagdescriptiondir)s/testtag')
        r.Install('testtag.taghandler',
                  '%%(taghandlerdir)s/testtag')
        # Also test tagging our own files
        r.Create('/etc/testself.1')
        r.ComponentSpec('runtime', '%%(sysconfdir)s/')
""" % { 'simpleTagHandler' : (simpleTagHandler % "") }

firstTagUserRecipe1 = """
class FirstTagUser(PackageRecipe):
    name = 'firsttaguser'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testfirst.1 <<EOF
first.1
EOF
''')

        r.Run('''
cat > testfirst.2 <<EOF
first.2
EOF
''')

        r.Install('testfirst.1', '/etc/testfirst.1')
        r.Install('testfirst.2', '/etc/testfirst.2')
        r.TagSpec('testtag', '/etc/test.*')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

secondTagUserRecipe1 = """
class SecondTagUser(PackageRecipe):
    name = 'secondtaguser'
    version = '0'
    clearBuildReqs()
    
    def setup(r):
        r.Run('''
cat > testsecond.1 <<EOF
second.1
EOF
''')

        r.Install('testsecond.1', '/etc/testsecond.1')
        r.TagSpec('testtag', '/etc/test.*')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

multiTagRecipe0 = """
class MultiTag(PackageRecipe):
    name = 'multitag'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('%(tagdescriptiondir)s/foo', contents='''file          %(taghandlerdir)s/foo
implements    files update
implements    files remove
datasource    multitag
''')
        r.Create('%(tagdescriptiondir)s/bar', contents='''file          %(taghandlerdir)s/foo
implements    files update
implements    files remove
datasource    multitag
''')
        r.Create('%(taghandlerdir)s/foo', mode=0755, contents='''\
#!/bin/bash

exit 0
''')
        r.Create('/foo')
        r.TagSpec('foo', '/foo')
"""

multiTagRecipe = multiTagRecipe0 + """
        r.TagSpec('bar', '/foo')
"""

multiTagRecipe2 = multiTagRecipe0

# Test 
multiTagRecipe3 = multiTagRecipe0.replace("exit 0", 
    "echo ${SOMEVAR:-UNDEFINED}; exit 0")

linkRecipe1 = """\
class LinkRecipe(PackageRecipe):
    name = 'linktest'
    version = '1.0'
    clearBuildReqs()
    hard = 1

    paths = ("/usr/share/foo", "/usr/share/bar")

    initialFileText = '\\n'.join([str(x) for x in range(0,10)]) + '\\n'
    fileText = initialFileText

    def setup(r):
        r.Create(r.paths[0], contents=r.initialFileText)
        for path in r.paths[1:]:
            if r.hard:
                r.Run("ln %%(destdir)s/%s %%(destdir)s/%s" % (r.paths[0], path))
            else:
                r.Run("ln -s %s %%(destdir)s/%s" % (r.paths[0], path))
"""

linkRecipe2 = """\
class LinkRecipe2(LinkRecipe):
    name = 'linktest'
    version = '1.1'

"""

linkRecipe3 = """\
class LinkRecipe3(LinkRecipe):
    name = 'linktest'
    version = '1.2'

    paths = ("/usr/share/foo", "/usr/share/bar", "/usr/share/foobar")
"""

# two link groups, both linkgroups have the same contents sha1
linkRecipe4 = """\
class LinkRecipe(PackageRecipe):
    name = 'linktest'
    version = '1.0'
    clearBuildReqs()
    hard = 1

    paths = ('/usr/share/lg1-1',
             '/usr/share/lg1-2',
             '/usr/share/lg2-1',
             '/usr/share/lg2-2')

    initialFileText = '\\n'.join([str(x) for x in range(0,10)]) + '\\n'
    fileText = initialFileText

    def setup(r):
        r.Create(r.paths[0], contents=r.initialFileText)
        r.Run("ln %%(destdir)s/%s %%(destdir)s/%s" % (r.paths[0],
                                                      r.paths[1]))
        r.Create(r.paths[2], contents=r.initialFileText)
        r.Run("ln %%(destdir)s/%s %%(destdir)s/%s" % (r.paths[2],
                                                      r.paths[3]))
"""

idChange1 = """\
class IdChange1(PackageRecipe):
    name = 'idchange'
    version = '1.0'
    clearBuildReqs()

    paths = [ "/etc/foo", "/etc/bar" ]

    fileText = '\\n'.join([str(x) for x in range(0,10)]) + '\\n'

    def setup(r):
        for path in r.paths:
            r.Create(path, contents=r.fileText)
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

idChange2 = """\
class IdChange2(IdChange1):
    paths = [ "/etc/foo" ]
    fileText = IdChange1.fileText
    fileText.replace("5", "10")
    version = '1.1'
"""

idChange3 = """\
class IdChange3(IdChange1):
    paths = [ "/etc/foo", "/etc/bar" ]
    fileText = IdChange1.fileText
    fileText.replace("6", "11")
    version = '1.2'
"""

testUnresolved = """\
class Unresolved(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/usr/bin/test', mode=0755)
        r.Requires('bar:foo', '/usr/bin/test')
"""

testTroveDepA = """\
class A(PackageRecipe):
    name = 'a'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/usr/bin/a', mode=0755)
"""

testTroveDepB = """\
class B(PackageRecipe):
    name = 'b'
    version = '1.0'
    clearBuildReqs()
    
    def setup(r):
        r.Create('/usr/bin/b', mode=0755)
        r.Requires('a:runtime', '/usr/bin/b')
"""


# these test updating a config file from a version which will no longer
# exist (and be cleared from the content store) to a new one
simpleConfig1 = """\
class SimpleConfig1(PackageRecipe):
    name = 'simpleconfig'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create("/etc/foo", contents = "text 1")
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

simpleConfig2 = """\
class SimpleConfig2(PackageRecipe):
    name = 'simpleconfig'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create("/etc/foo", contents = "text 2")
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

testRecipeTemplate = """\
class TestRecipe%(num)d(PackageRecipe):

    name = 'test%(num)d'
    version = '%(version)s'
    clearBuildReqs()
    buildRequires = [ %(requires)s ]

    %(header)s

    %(flags)s

    def setup(r):

        %(flavor)s

        r.Create('/usr/bin/test%(num)s',contents='''\
#!/bin/sh
echo "This is test%(num)s"
%(fileContents)s
''', mode=0755)
        del r.NormalizeInterpreterPaths

        if %(binary)s:
            r.Run('''
cat > hello.c <<'EOF'
#include <stdio.h>

int main(void) {
    return printf("Hello, world.\\\\n");
}
EOF
                ''')
            r.Make('hello', preMake='LDFLAGS="-static"')
            r.Install('hello', '%%(bindir)s/')
        %(content)s
        %(subpkgs)s
        %(tagspec)s
        %(fail)s
        # override :config
        r.ComponentSpec('runtime', '.*')
"""

def createRecipe(num, requires=[], fail=False, content='', 
                 packageSpecs=[],
                 subPackages = [], version='1.0', localflags=[], flags=[],
                 header='', fileContents='', tag=None, binary=False):
    reqList = []
    for req in requires:
        reqList.append("'test%d:runtime'" % req)
    subs = {}
    subs['requires'] = ', '.join(reqList)
    subs['version'] = version
    subs['num'] = num
    subs['content'] = content
    subs['fileContents'] = fileContents
    subs['header'] = header
    subs['binary'] = binary
    subpkgStrs = []
    flagStrs = []
    flavorStrs = []
    if localflags and not isinstance(localflags, (tuple, list)):
        localflags = [localflags]

    for flag in localflags:
        flagStr = 'Flags.%s = True' % flag
        flavorStr = 'if Flags.%s: pass' % flag
        flagStrs.append(flagStr)
        flavorStrs.append(flavorStr)

    if tag:
        subs['tagspec'] = "r.TagSpec('%s', '/usr/bin/test1')" % tag
    else:
        subs['tagspec'] = ''
        

    if flags and not isinstance(flags, (tuple, list)):
        flags = [flags]
    for flag in flags:
        flavorStr = 'if %s: pass' % flag
        flavorStrs.append(flavorStr)
    subs['flags'] = '\n    '.join(flagStrs)
    subs['flavor'] = '\n        '.join(flavorStrs)

    # add indentation
    subpkgStrs.append('\n        '.join(packageSpecs))

    for subpkg in subPackages:
        subpkgStr = '''
        r.Create('%%(thisdocdir)s/README-%(subpkg)s')
        r.Create('/asdf/runtime-%(subpkg)s')
        r.PackageSpec('%(name)s-%(subpkg)s', 'README-%(subpkg)s')
        r.PackageSpec('%(name)s-%(subpkg)s', 'runtime-%(subpkg)s')
              ''' % { 'name' : ('test%d' % num), 'subpkg' : subpkg } 
        subpkgStrs.append(subpkgStr)

    subs['subpkgs'] = '\n'.join(subpkgStrs)

    if fail:
        subs['fail'] = 'r.Run("exit 1")'
    else:
        subs['fail'] = ''
    return testRecipeTemplate % subs

fileTypeChangeRecipe1="""\
class FileTypeChange(PackageRecipe):
    name = 'filetypechange'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('%(essentialbindir)s/foo', mode=0755, contents = 'some text')
"""

fileTypeChangeRecipe2="""\
class FileTypeChange(PackageRecipe):
    name = 'filetypechange'
    version = '2'
    clearBuildReqs()
    def setup(r):
        r.Run("mkdir %(destdir)s%(essentialbindir)s")
        r.Run("ln -s foo %(destdir)s%(essentialbindir)s/foo")
"""

manyFlavors = """\
class ManyFlavors(PackageRecipe):
    name = 'manyflavors'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        if Use.readline: 
            r.Create("/etc/readline", contents = "text 1")
        if Use.ssl:
            r.Create("/etc/ssl", contents = "text 1")
        if not Use.ssl and not Use.readline:
            r.Create("/etc/none", contents = "text 1")
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

manyFlavors2 = """\
class ManyFlavors(PackageRecipe):
    name = 'manyflavors'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        if Use.readline: 
            r.Create("/etc/readline", contents = "text 1")
        if Use.ssl:
            r.Create("/etc/ssl", contents = "text 1")
        if not Use.ssl and not Use.readline:
            r.Create("/etc/none", contents = "text 1")
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

autoSource0 = """\
class AutoSource(PackageRecipe):
    name = 'autosource'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('localfile')
"""

autoSource1 = """\
class AutoSource(PackageRecipe):
    name = 'autosource'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('distcc-2.9.tar.bz2')
        r.addSource('localfile')
"""

autoSource2 = """\
class AutoSource(PackageRecipe):
    name = 'autosource'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('multilib-sample.tar.bz2')
        r.addSource('localfile')
"""

autoSource3 = """\
class AutoSource(PackageRecipe):
    name = 'autosource'
    version = '3.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('multilib-sample.tar.bz2')
        r.addSource('localfile')
        r.Create('/foo')
"""

autoSource4 = """\
class AutoSource(PackageRecipe):
    name = 'autosource'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('distcache-1.4.5.tar.bz2')
        r.Create('/foo')
"""

autoSource5 = """\
class AutoSource(PackageRecipe):
    name = 'autosource'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.addSource('distcache-1.4.5.tar.bz2', rpm='distcache-1.4.5-2.src.rpm')
        r.Create('/bar')
"""



configFileGoesEmpty1 = """\
class Config(PackageRecipe):
    name = 'config'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/config', contents='test 123')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

configFileGoesEmpty2 = """\
class Config(PackageRecipe):
    name = 'config'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/config')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

testRemove1 = """\
class Remove(PackageRecipe):
    name = 'remove'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/config')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

testRemove2 = """\
class Remove(PackageRecipe):
    name = 'remove'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/blah')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

configFileBecomesSymlink1 = """\
class Config(PackageRecipe):
    name = 'config'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/config', contents='test 123')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

configFileBecomesSymlink2 = """\
class Config(PackageRecipe):
    name = 'config'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/foo', contents='test 234')
        r.Symlink('foo', '/etc/config')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

symlinkBecomesFile1 = """\
class Test(PackageRecipe):
    name = 'test'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/usr/share/man/man1/bar.1', contents='test 234')
        r.Symlink('bar.1', '/usr/share/man/man1/foo.1')
"""

symlinkBecomesFile2 = """\
class Test(PackageRecipe):
    name = 'test'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/usr/share/man/man1/foo.1', contents='test 123')
"""

branchedFileIdTest1 = """
class BranchedFileId(PackageRecipe):
    name = 'branchedFileId'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/first', 'unchanged')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

branchedFileIdTest2 = """
class BranchedFileId(PackageRecipe):
    name = 'branchedFileId'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('/etc/second', 'unchanged')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

pathIdTest1 = """
class PathIdTest(PackageRecipe):
    name = 'PathIdTest'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create("/lib/1")
        r.Create("/lib/first")
        r.Create("/lib/non-utf8" + '\200')
        r.NonUTF8Filenames(exceptions="/lib/non-utf8" + '\200')
"""

pathIdTest2 = """
class PathIdTest(PackageRecipe):
    name = 'PathIdTest'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create("/lib/1")
        r.Create("/lib/2")
"""

pathIdTest3 = """
class PathIdTest(PackageRecipe):
    name = 'PathIdTest'
    version = '3.0'
    clearBuildReqs()

    def setup(r):
        r.Create("/lib/1")
        r.Create("/lib/2")
        r.Create("/lib/3")
"""

pathIdTest4 = """
class PathIdTest(PackageRecipe):
    name = 'PathIdTest'
    version = '4.0'
    clearBuildReqs()

    def setup(r):
        r.Create("/lib/1")
        r.Create("/lib/2")
        r.Create("/lib/3")
        r.Create("/lib/4")
"""

depsMultiVersionTest1 = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('%(libdir)s/libfoo.so.1')
        r.Provides('file', '%(libdir)s/libfoo.so.1')
"""

depsMultiVersionTest2 = """
class Foo(PackageRecipe):
    name = 'foo'
    version = '2.0'
    clearBuildReqs()

    def setup(r):
        r.Create('%(libdir)s/libfoo.so.2')
        r.Provides('file', '%(libdir)s/libfoo.so.2')
"""

depsMultiVersionUser1 = """
class Bar(PackageRecipe):
    name = 'bar'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('%(bindir)s/bar', mode=0755)
        r.Requires('%(libdir)s/libfoo.so.1' %r.macros, '%(bindir)s/bar')
"""

depsMultiVersionUser2 = """
class Baz(PackageRecipe):
    name = 'baz'
    version = '1.0'
    clearBuildReqs()

    def setup(r):
        r.Create('%(bindir)s/baz', mode=0755)
        r.Requires('%(libdir)s/libfoo.so.2' %r.macros, '%(bindir)s/baz')
"""

testSuiteRecipe = """
class TestSuiteRecipe(PackageRecipe):
    name = 'testcase'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Run('mkdir test; echo -e \#\!/bin/notthere\\nhi > test/foo; chmod 755 test/foo')
        r.TestSuite('test', autoBuildMakeDependencies=False)
        r.Create('/etc/foo')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

dependencyGroup = """
class DependencyGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.Requires('other')
        self.addTrove('test:runtime')
"""



initialContentsRecipe0 = """
class InitialContentsTest(PackageRecipe):
    name = 'initcontents'
    version = '0'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo', contents='initialtransientcontents')
        r.Transient('/foo')
"""
initialContentsRecipe01 = """
class InitialContentsTest(PackageRecipe):
    name = 'initcontents'
    version = '0.1'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo', contents='initialregularcontents')
"""
initialContentsRecipe02 = """
class InitialContentsTest(PackageRecipe):
    name = 'initcontents'
    version = '0.1'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo', contents='initialconfigcontents')
        r.Config('/foo')
        r.ComponentSpec('runtime', '/foo')
"""

initialContentsRecipe1 = """
class InitialContentsTest(PackageRecipe):
    name = 'initcontents'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo', contents='initialrecipecontents')
        r.InitialContents('/foo')
"""

initialContentsRecipe2 = """
class InitialContentsTest(PackageRecipe):
    name = 'initcontents'
    version = '2'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo', contents='secondrecipecontents')
        r.InitialContents('/foo')
"""

otherRecipe = """
class Other(PackageRecipe):
    name = 'other'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/etc/other', contents='secondrecipecontents')
        r.Requires('test:config', '/etc/other')
        r.ComponentSpec('runtime', '%(sysconfdir)s/')
"""

testGroup1 = """
class TestGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.addTrove('test1', '1.0')
"""

testGroup2 = """
class TestGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    def setup(self):
        self.addTrove('test1', '1.1')
"""

testGroup3 = """
class TestGroup(GroupRecipe):
    name = 'group-test'
    version = '1.0'
    clearBuildRequires()
    checkPathConflicts = True
    def setup(self):
        self.startGroup('group-test2', checkPathConflicts=False, groupName='group-test')
        self.addTrove('test1', '1.0')
        self.startGroup('group-test3', groupName='group-test')
        self.addTrove('test2', '1.0')
"""

userInfoRecipe = """
class UserMe(UserInfoRecipe):
    name = 'info-%(user)s'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.User('%(user)s', %(uid)s)
"""

syncGroupRecipe1 = """
class SyncGroup(GroupRecipe):
    name = 'group-sync'
    version = '1'
    imageGroup = False
    clearBuildRequires()
    def setup(r):
        r.addTrove('synctrove', '1', byDefault=False)
"""

syncGroupRecipe2 = """
class SyncGroup(GroupRecipe):
    name = 'group-sync'
    version = '2'
    imageGroup = False
    clearBuildRequires()
    def setup(r):
        r.addTrove('synctrove', '2', byDefault=False)
"""

syncTroveRecipe1 = """
class SyncTrove(PackageRecipe):
    name = 'synctrove'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/share/foo1')
        r.Create('%(debugsrcdir)s/%(name)s-%(version)s/foo1')
"""

syncTroveRecipe2 = """
class SyncTrove(PackageRecipe):
    name = 'synctrove'
    version = '2'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/share/foo2')
        r.Create('%(debugsrcdir)s/%(name)s-%(version)s/foo2')
"""

notByDefaultRecipe = """
class NotByDefault(PackageRecipe):
    name = 'testcase'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        if Use.readline: pass
        r.Create('/usr/share/foo2')
        r.Create('%(debugsrcdir)s/%(name)s-%(version)s/foo2')
"""

sourceSuperClass1 = """
class SourceSuperClass(PackageRecipe):
    name = 'superclass'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/share/foo2')
"""

sourceSuperClass2 = """
class SourceSuperClass(PackageRecipe):
    name = 'superclass'
    version = '1.0'
    clearBuildReqs()
    def setup(r):
        r.Create('/usr/share/foo2')
        r.addSource('newsource')
        r.Install('newsource', '/usr/share/foo3')
"""

sourceSubClass1 = """
loadRecipe('superclass')
class sourceSubClass(SourceSuperClass):
    name = 'subclass'
    version = '1.0'
    clearBuildReqs()
"""

simpleRecipe = """
class SimpleRecipe(PackageRecipe):
    name = 'simple'
    version = '1'
    clearBuildReqs()
    def setup(r):
        r.Create('/foo', contents='simple')
"""

basicSplitGroup = """
class splitGroup(GroupRecipe):
    name = 'group-first'
    version = '1.0'
    checkPathConflicts = False
    clearBuildRequires()

    def setup(self):
        self.add("test", "@rpl:linux")
        self.createGroup('group-second')
        self.createGroup('group-third')
        self.add("test", "@rpl:linux",
                      groupName = ['group-second', 'group-third'])
        # add group-second to group-first
        self.addNewGroup('group-second')
"""

buildReqTest1 = """\
class BuildReqTest(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    buildRequires = ['blah']

    def setup(r):
        r.addSource('distcc-2.9.tar.bz2')
"""

unknownFlagRecipe = """\
class BuildReqTest(PackageRecipe):
    name = 'foo'
    version = '1.0'
    clearBuildReqs()
    if Use.ffff:
        pass

    def setup(r):
        if Use.ffff:
            r.Create('/foo', contents='simple')
        else:
            r.Create('/bar', contents='simple')
"""

simpleFactory = """\
class SimpleFactory(Factory):

    name = "factory-simple"
    version = "1.0"

    def getRecipeClass(self):

        class Subclass(PackageRecipe):
            internalAbstractBaseClass = True
            name = "subclass"
            version = "1.0"

        return Subclass
"""

simpleFactoryWithSources = """\
class SimpleFactory(Factory):

    name = "factory-simple"
    version = "1.0"

    def getRecipeClass(self):

        clearBuildRequires()
        f = self.openSourceFile('VERSION')
        readVersion = f.read()[:-1]
        f.close()

        class RealRecipe(PackageRecipe):
            name = self.packageName
            version = readVersion

            def setup(r):
                if False:
                    # make sure FactoryException is available
                    raise FactoryException

                r.Create("/foo", contents = readVersion + "\\n")

        return RealRecipe
"""
