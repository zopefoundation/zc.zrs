%define python /opt/cleanpython24/bin/python
%define zrs_version 2.0.2
%define svn_url svn+ssh://svn.zope.com/repos/main/zc.zrs/trunk
requires: cleanpython24
Name: zrsx
Version: %{zrs_version}
Release: 2
Summary: Zope Replication Service
URL: http://www.zope.com/products/zope_replication_services.html

Copyright: ZVSL
Vendor: Zope Corporation
Packager: Zope Corporation <sales@zope.com>
Buildroot: /tmp/buildroot
Prefix: /opt
Group: Applications/Database
AutoReqProv: no

%description
%{summary}

%build
rm -rf $RPM_BUILD_ROOT
mkdir $RPM_BUILD_ROOT
mkdir $RPM_BUILD_ROOT/opt
mkdir $RPM_BUILD_ROOT/etc
mkdir $RPM_BUILD_ROOT/etc/init.d
touch $RPM_BUILD_ROOT/etc/init.d/%{name}
svn export %{svn_url} $RPM_BUILD_ROOT/opt/%{name}
cd $RPM_BUILD_ROOT/opt/%{name}
%{python} bootstrap.py -c rpm.cfg buildout:eggs-directory=eggs
bin/buildout -v -c rpm.cfg \
   "zodb:eggs=ZODB3
              zc.zrs
              zc.queue
              zope.app.keyreference
              zope.minmax" \
   buildout:installed= \
   bootstrap:recipe=zc.rebootstrap \
   buildout:eggs-directory=eggs

%post
cd $RPM_INSTALL_PREFIX/%{name}
%{python} bin/bootstrap -Uc rpmpost.cfg
bin/buildout -Uc rpmpost.cfg \
   "zodb:eggs=ZODB3
              zc.zrs
              zc.queue
              zope.app.keyreference
              zope.minmax" \
   buildout:offline=true buildout:find-links= buildout:installed= \
   mercury:name=%{name} mercury:recipe=buildoutmercury
chmod -R -w . 

%preun
cd $RPM_INSTALL_PREFIX/%{name}
chmod -R +w . 
find . -name \*.pyc | xargs rm -f

%files
%attr(-, root, root) /opt/%{name}
%attr(744, root, root) /etc/init.d/%{name}
