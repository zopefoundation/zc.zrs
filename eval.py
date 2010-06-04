from setuptools import setup
import datetime
import os
import random
import time

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
"""

version = open('version.txt').read().strip()+'.eval'
version += datetime.date.today().isoformat().replace('-', '')

timeout_template="""

if __import__('time').time() > %s:
    raise SystemError('Your ZRS evaluation has expired.')
"""

import shutil
if os.path.isdir('build'):
    shutil.rmtree('build')

open('LICENSE.txt', 'w').write(open('ZEL.txt').read())

entry_points = """
[console_scripts]
zrsmonitor-script = zc.zrs.monitor:main
"""

options = dict(
    name = 'zc.zrs',
    version = version,
    author = "Jim Fulton",
    author_email = "jim@zope.com",
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

open('setup.cfg', 'w').write("""
[bdist_egg]
exclude-source-files = true
""")

try:
    setup(**options)

    deadline = random.randint(40, 60)
    deadline = time.time()+(86400*deadline)
    deadline = timeout_template % deadline
    for name in 'primary', 'reactor', 'secondary', 'sizedmessage':
        open(os.path.join('build', 'lib', 'zc', 'zrs', name+'.py'), 'a').write(
            deadline)

    setup(**options)

finally:
    os.remove('setup.cfg')
    os.remove('LICENSE.txt')
