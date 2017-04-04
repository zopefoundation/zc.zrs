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
version = '3.0.0'

try:
    from ez_setup import use_setuptools
    use_setuptools()
except ImportError:
    pass

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


import os, shutil
if os.path.isdir('build'):
    shutil.rmtree('build')

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
last-zeo-transaction = zc.zrs.last:main
zrs-nagios = zc.zrs.nagios:basic
"""

tests_require = ['zope.testing', 'mock', 'ZEO']

long_description = (
    open('README.rst').read() + '\n\n' + open('CHANGES.rst').read())

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: Implementation :: CPython
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
""".strip().split('\n')

setup(
    name = name,
    long_description = long_description,
    description = long_description.split('\n')[1],
    version = version,
    author = "Jim Fulton",
    author_email = "jim@jimfulton.info",
    license = "ZPL 2.1",
    keywords = "ZODB",
    classifiers = classifiers,
    url='https://github.com/zc/zrs',

    packages = ['zc', 'zc.zrs'],
    include_package_data = True,
    data_files = [('.', ['README.rst'])],
    zip_safe = True,
    entry_points = entry_points,
    package_dir = {'':'src'},
    namespace_packages = ['zc'],
    install_requires = [
        'setuptools',
        'ZODB',
        'Twisted',
        ],
    extras_require = dict(test=tests_require),
    tests_require = tests_require,
    test_suite = 'zc.zrstests.tests.test_suite',
    )
