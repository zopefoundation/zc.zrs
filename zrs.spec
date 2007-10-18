%define python /opt/cleanpython24/bin/python
%define zrs_version 2.0.2
%define release 3
%define svn_project svn+ssh://svn.zope.com/repos/main/zc.zrs
%define svn_url %{svn_project}/tags/%{name}-%{zrs_version}-%{release}

requires: cleanpython24
Name: zrs
Version: %{zrs_version}
Release: %{release}
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
   buildout:installed= \
   bootstrap:recipe=zc.rebootstrap \
   buildout:eggs-directory=eggs

%post
cd $RPM_INSTALL_PREFIX/%{name}
%{python} bin/bootstrap -Uc rpmpost.cfg
bin/buildout -Uc rpmpost.cfg \
   buildout:offline=true buildout:find-links= buildout:installed= 
chmod -R -w . 

%preun
cd $RPM_INSTALL_PREFIX/%{name}
chmod -R +w . 
find . -name \*.pyc | xargs rm -f

%files
%attr(-, root, root) /opt/%{name}
%attr(744, root, root) /etc/init.d/%{name}
