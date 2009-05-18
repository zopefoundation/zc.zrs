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

from setuptools import setup
import datetime
import os
import random
import time

open('setup.cfg', 'w').write("""
[bdist_egg]
exclude-source-files = true
""")

open('LICENSE.txt', 'w').write(open('ZEL.txt').read())

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
"""


version = open('zrsversion.cfg').read().strip().split()[-1]+'.eval'
version += datetime.date.today().isoformat().replace('-', '')

timeout_template="""

if __import__('time').time() > %s:
    raise SystemError('Your ZRS evaluation has expired.')
"""

import shutil
if os.path.isdir('build'):
    shutil.rmtree('build')

options = dict(
    name = 'zc.zrs',
    version = version,
    author = "Jim Fulton",
    author_email = "jim#zope.com",
    description = "Zope Replication Server",
    license = "Zope Evaluation License 1.0",
    keywords = "ZODB",

    packages = ['zc', 'zc.zrs'],
    include_package_data = True,
    data_files = [('.', ['README.txt', 'LICENSE.txt'])],
    zip_safe = True,
    entry_points = entry_points,
    package_dir = {'':'src'},
    namespace_packages = ['zc'],
    install_requires = ['setuptools', 'ZODB3', 'Twisted'],
    )

try:
    setup(**options)

    deadline = random.randint(32, 60)
    deadline = time.time()+(86400*deadline)
    deadline = timeout_template % deadline
    for name in 'primary', 'reactor', 'secondary', 'sizedmessage':
        open(os.path.join('build', 'lib', 'zc', 'zrs', name+'.py'), 'a').write(
            deadline)

    setup(**options)
finally:
    os.remove('setup.cfg')
    os.remove('LICENSE.txt')

