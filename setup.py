##############################################################################
#
# Copyright (c) Zope Corporation.  All Rights Reserved.
#
# This software is subject to the provisions of the Zope Visible Source
# License, Version 1.0 (ZVSL).  A copy of the ZVSL should accompany this
# distribution.
#
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################

name = 'zc.zrs'
version = '2.0.5'

from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

import os, shutil
if os.path.isdir('build'):
    shutil.rmtree('build')

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
last-zeo-transaction = zc.zrs.last:main
"""

setup(
    name = name,
    version = version,
    author = "Jim Fulton",
    author_email = "jim#zope.com",
    description = "Zope Replication Server",
    long_description = open('README.rst').read(),
    license = "ZVSL 1.0",
    keywords = "ZODB",

    packages = ['zc', 'zc.zrs'],
    include_package_data = True,
    zip_safe = True,
    entry_points = entry_points,
    package_dir = {'':'src'},
    namespace_packages = ['zc'],
    install_requires = [
        'setuptools',
        'ZODB3 >=3.8, <3.9dev',
        'Twisted',
        ],
    )
