Name: with-special-files
Version: 0.1
Release: 1
Group: Dummy
License: GPL
#Source: %{name}-%{version}.tar.gz
BuildRoot: /var/tmp/%{name}-%{version}-root
Summary: RPM with special files

%description
Description

%prep
%setup -c -T

%build

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT

# A regular file
install -d $RPM_BUILD_ROOT/%{_sysconfdir} $RPM_BUILD_ROOT/%{_datadir}
cat > $RPM_BUILD_ROOT/%{_datadir}/%{name}.txt << EOF
Some data
EOF

# A configuration file
cat > $RPM_BUILD_ROOT/%{_sysconfdir}/%{name}.cfg << EOF
config option
EOF

# An empty configuration flie
touch $RPM_BUILD_ROOT/%{_sysconfdir}/empty.cfg

# A fifo
mkfifo $RPM_BUILD_ROOT/%{_datadir}/%{name}.fifo

# a regular file with contents not verified by RPM
touch $RPM_BUILD_ROOT/%{_datadir}/noverifydigest

# a %doc file (these are weird)
touch $RPM_BUILD_ROOT/%{_datadir}/documentation

# A symlink to the config file
ln -s %{name}.cfg $RPM_BUILD_ROOT/%{_sysconfdir}/%{name}.symlink.cfg

# a missingok file
touch $RPM_BUILD_ROOT/%{_sysconfdir}/missingok

# a missingok directory
mkdir $RPM_BUILD_ROOT/%{_sysconfdir}/missingokdir

# A symlink to a real file, marked as config
ln -s ..%{_datadir}/%{name}.txt $RPM_BUILD_ROOT/%{_sysconfdir}/%{name}.symlink2.cfg

# A symlink to a real file, with unverified contents
ln -s ..%{_datadir}/%{name}.txt $RPM_BUILD_ROOT/%{_sysconfdir}/%{name}.symlink.unverified.cfg

# A symlink to a real file, with bogus permissions including sugid
ln -s ..%{_datadir}/%{name}.txt $RPM_BUILD_ROOT/%{_sysconfdir}/%{name}.symlink3.cfg

# A non-traversable directory
mkdir $RPM_BUILD_ROOT/%{_datadir}/nontraversable-dir
# With a file in it
touch $RPM_BUILD_ROOT/%{_datadir}/nontraversable-dir/some-file.txt

# A 0111 directory
mkdir -p $RPM_BUILD_ROOT/%{_datadir}/dir-0111/subdir
touch $RPM_BUILD_ROOT/%{_datadir}/dir-0111/some-file.txt
cat > $RPM_BUILD_ROOT/%{_datadir}/dir-0111/subdir/ghost-file.txt << EOF
Ghost contents
EOF

# An empty directory marked as config
mkdir $RPM_BUILD_ROOT%{_sysconfdir}/empty-dir

# A ghost directory
mkdir $RPM_BUILD_ROOT%{_datadir}/ghost-dir

# A ghost config directory
mkdir $RPM_BUILD_ROOT%{_sysconfdir}/ghost-dir

mkdir $RPM_BUILD_ROOT%{_bindir}

# A setuid shell script
cat > $RPM_BUILD_ROOT%{_bindir}/%{name}.setuid.sh << EOF
#!/bin/bash
EOF

# A setuid binary
cp /bin/arch $RPM_BUILD_ROOT%{_bindir}/%{name}.setuid

# A directory with a + character in the name
mkdir -p $RPM_BUILD_ROOT/usr/share/doc/foo/hello-c++

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0644,root,root)
%attr(0755,-,-) /
%attr(0755,-,-) %{_bindir}/%{name}.setuid.sh
%attr(0755,-,-) %{_bindir}/%{name}.setuid
%config %{_sysconfdir}/%{name}.cfg
%config %{_sysconfdir}/empty.cfg
%config %{_sysconfdir}/%{name}.symlink.cfg
%config %{_sysconfdir}/%{name}.symlink2.cfg
%attr(06755,-,-) %{_sysconfdir}/%{name}.symlink3.cfg
%verify(not link) %{_sysconfdir}/%{name}.symlink.unverified.cfg
%{_datadir}/%{name}.txt
%{_datadir}/%{name}.fifo
%verify(not md5) %{_datadir}/noverifydigest
%doc %{_datadir}/documentation
%dev(b,8,0) /dev/sda
%attr(0600,-,-) %dir %{_datadir}/nontraversable-dir
%{_datadir}/nontraversable-dir/some-file.txt
%config(missingok) %{_sysconfdir}/missingok
%config(missingok) %{_sysconfdir}/missingokdir

%attr(0600,-,-) %dir %config %{_sysconfdir}/empty-dir
%attr(0600,-,-) %dir %ghost %{_datadir}/ghost-dir
%attr(0600,-,-) %dir %config %ghost %{_sysconfdir}/ghost-dir

%attr(0111,-,-) %dir %{_datadir}/dir-0111
%attr(0755,-,-) %dir %{_datadir}/dir-0111/subdir
%attr(0644,-,-) /%{_datadir}/dir-0111/some-file.txt
%attr(0644,-,-) %ghost /%{_datadir}/dir-0111/subdir/ghost-file.txt
%dir /usr/share/doc/foo/hello-c++
