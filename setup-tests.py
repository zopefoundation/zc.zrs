name = 'zc.zrstests'
version = open('zrsversion.cfg').read().strip().split()[-1]

from setuptools import setup, find_packages

import shutil
if os.path.isdir('build'):
    shutil.rmtree('build')

setup(
    name = name,
    version = version,
    author = "Jim Fulton",
    author_email = "jim#zope.com",
    description = "Zope Replication Server",
    license = "ZVSL 1.0",
    keywords = "ZODB",

    packages = ['zc', 'zc.zrstests'],
    include_package_data = True,
    zip_safe = True,
    package_dir = {'':'tests-src'},
    namespace_packages = ['zc'],
    install_requires = [
        'setuptools',
        'ZODB3',
        'Twisted',
        'zc.zrs ==%s, ==%s.eval' % (version, version),
        ],
    )
