%define python zpython
%define svn_url svn+ssh://svn.zope.com/repos/main/zc.zrs/trunk
requires: zpython
Name: zrs
Version: `cat version.txt`
Release: 1
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
bin/buildout -c rpm.cfg \
   buildout:installed= \
   bootstrap:recipe=zc.rebootstrap \
   buildout:eggs-directory=eggs

%post
cd $RPM_INSTALL_PREFIX/%{name}
%{python} bin/bootstrap -Uc rpmpost.cfg
bin/buildout -Uc rpmpost.cfg \
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
